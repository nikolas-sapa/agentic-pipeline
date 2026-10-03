"""spine.queue — the deep module. All SQL, the SKIP LOCKED claim, and the
idempotency race live here and nowhere else (plan §2).
"""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from psycopg import AsyncConnection
from psycopg.rows import dict_row
from psycopg.types.json import Json

from spine.model import (
    DEFAULT_RETRY_POLICY,
    Event,
    HandlerResult,
    Job,
    JobState,
    RetryableError,
    TerminalError,
)


@dataclass(frozen=True)
class Claimed:
    job: Job
    # None for a job reap() reconstructs from a stale `running` row — reap
    # never needs the Event, only claim() (the worker's handler dispatch does).
    event: Event | None
    attempt_id: UUID
    attempt_number: int


async def enqueue(
    conn: AsyncConnection,
    *,
    source: str,
    type: str,
    payload: dict,
    idempotency_key: str,
    trace_id: str,
    handler_name: str,
    variant: str,
) -> tuple[UUID, bool]:
    """Insert Event+Job in one transaction. Returns (job_id, is_new).

    Deviation from plan §2's `enqueue(conn, event) -> JobId | None`: the
    duplicate case (`is_new=False`) still carries the *original* job_id, not
    None, because ticket 2's acceptance check requires the loser to see the
    same job_id the winner got. `None` alone can't express that.

    Race (plan §3a): two concurrent POSTs of the same delivery both attempt
    the INSERT below. Exactly one gets a row back (the winner); the loser
    gets zero rows and falls through to the SELECT. The unique index on
    idempotency_key is the arbiter, not application logic.
    """
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            INSERT INTO events (source, type, payload, idempotency_key, trace_id)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (idempotency_key) DO NOTHING
            RETURNING id
            """,
            (source, type, Json(payload), idempotency_key, trace_id),
        )
        row = await cur.fetchone()

        if row is not None:
            event_id = row["id"]
            await cur.execute(
                """
                INSERT INTO jobs (event_id, handler_name, state, trace_id, variant)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id
                """,
                (event_id, handler_name, JobState.PENDING, trace_id, variant),
            )
            job_row = await cur.fetchone()
            return job_row["id"], True

        # Loser: event already exists. Find its job.
        await cur.execute(
            "SELECT id FROM events WHERE idempotency_key = %s", (idempotency_key,)
        )
        event_id = (await cur.fetchone())["id"]
        await cur.execute("SELECT id FROM jobs WHERE event_id = %s", (event_id,))
        job_row = await cur.fetchone()
        return job_row["id"], False


async def claim(conn: AsyncConnection, lease_seconds: int) -> Claimed | None:
    """Claim one pending, due job. Row-level lock (SKIP LOCKED) means two
    workers can never claim the same row (plan §3b) — this is execution
    dedup, distinct from the ingress dedup in enqueue()."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            UPDATE jobs SET
                state = 'running',
                lease_expires_at = now() + make_interval(secs => %s),
                attempt_count = attempt_count + 1
            WHERE id = (
                SELECT id FROM jobs
                WHERE state = 'pending' AND run_after <= now()
                ORDER BY run_after
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            RETURNING id, event_id, handler_name, state, attempt_count, variant, trace_id
            """,
            (lease_seconds,),
        )
        row = await cur.fetchone()
        if row is None:
            return None

        await cur.execute(
            """
            SELECT id, source, type, payload, idempotency_key, trace_id, created_at
            FROM events WHERE id = %s
            """,
            (row["event_id"],),
        )
        event_row = await cur.fetchone()

        await cur.execute(
            """
            INSERT INTO attempts (job_id, attempt_number, state)
            VALUES (%s, %s, 'running')
            RETURNING id
            """,
            (row["id"], row["attempt_count"]),
        )
        attempt_row = await cur.fetchone()

        job = Job(
            id=row["id"],
            event_id=row["event_id"],
            handler_name=row["handler_name"],
            state=JobState(row["state"]),
            attempt_count=row["attempt_count"],
            variant=row["variant"],
            trace_id=row["trace_id"],
        )
        event = Event(
            id=event_row["id"],
            source=event_row["source"],
            type=event_row["type"],
            payload=event_row["payload"],
            idempotency_key=event_row["idempotency_key"],
            trace_id=event_row["trace_id"],
            created_at=event_row["created_at"],
        )
        return Claimed(
            job=job,
            event=event,
            attempt_id=attempt_row["id"],
            attempt_number=row["attempt_count"],
        )


async def _owns_attempt(conn: AsyncConnection, claimed: Claimed) -> bool:
    """Lock the job before checking ownership. The caller holds a transaction
    across this check and every attempt/job write, including on autocommit
    connections. Reaped or already-finished claims cannot write again."""
    async with conn.cursor() as cur:
        await cur.execute(
            "SELECT 1 FROM jobs WHERE id = %s AND state = %s "
            "AND attempt_count = %s FOR UPDATE",
            (claimed.job.id, JobState.RUNNING, claimed.attempt_number),
        )
        return await cur.fetchone() is not None


async def complete(
    conn: AsyncConnection, claimed: Claimed, result: HandlerResult
) -> bool:
    """Close the attempt and the job as succeeded. Every state change writes
    an attempt row (plan §1 invariant) — claim() already opened it; this
    closes it in the same lifecycle (`running -> succeeded`, plan §1).
    Returns False with no writes when the claim no longer owns the job."""
    async with conn.transaction():
        if not await _owns_attempt(conn, claimed):
            return False
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE attempts SET
                    state = 'succeeded',
                    ended_at = now(),
                    -- EPOCH, not MILLISECONDS: EXTRACT(MILLISECONDS FROM interval)
                    -- returns only the seconds field, so 1m2.5s reads as 2500 not
                    -- 62500. duration_ms feeds p95 latency; slow jobs would have
                    -- silently reported as fast.
                    duration_ms = (EXTRACT(EPOCH FROM (now() - started_at)) * 1000)::int,
                    output = %s
                WHERE id = %s
                """,
                (Json(result.output), claimed.attempt_id),
            )
            await cur.execute(
                "UPDATE jobs SET state = %s, output = %s WHERE id = %s",
                (JobState.SUCCEEDED, Json(result.output), claimed.job.id),
            )
    return True


def _classify(error: BaseException) -> tuple[str, bool]:
    """(error_class, is_terminal). RetryableError/TerminalError carry their
    own code as the message (e.g. TerminalError("budget_exceeded")); an
    undeclared exception is a bug in the handler — treated as retryable until
    it exhausts attempts, per plan §4's third taxonomy row."""
    if isinstance(error, TerminalError):
        return str(error), True
    if isinstance(error, RetryableError):
        return str(error), False
    return type(error).__name__, False


async def fail(
    conn: AsyncConnection, claimed: Claimed, error: BaseException
) -> JobState | None:
    """Close the open attempt as failed and decide retry-vs-dead_letter
    (plan §4): TerminalError dead-letters immediately with attempts
    untouched; anything else retries with full-jitter backoff until
    DEFAULT_RETRY_POLICY.max_attempts, then dead-letters with the last error
    preserved on the job row. Returns the job's new state so the worker
    knows whether the job just became terminal. Returns None with no writes
    when the claim no longer owns the job."""
    error_class, is_terminal = _classify(error)

    async with conn.transaction():
        if not await _owns_attempt(conn, claimed):
            return None
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE attempts SET
                    state = 'failed', ended_at = now(),
                    -- EPOCH, not MILLISECONDS: see complete()'s comment above.
                    duration_ms = (EXTRACT(EPOCH FROM (now() - started_at)) * 1000)::int,
                    error_class = %s, error = %s
                WHERE id = %s
                """,
                (error_class, str(error), claimed.attempt_id),
            )

            if is_terminal or claimed.attempt_number >= DEFAULT_RETRY_POLICY.max_attempts:
                new_state = JobState.DEAD_LETTER
                await cur.execute(
                    """
                    UPDATE jobs SET state = %s, last_error_class = %s, last_error = %s
                    WHERE id = %s
                    """,
                    (new_state, error_class, str(error), claimed.job.id),
                )
            else:
                new_state = JobState.PENDING
                delay = DEFAULT_RETRY_POLICY.delay_seconds(claimed.attempt_number)
                await cur.execute(
                    """
                    UPDATE jobs SET
                        state = %s, run_after = now() + make_interval(secs => %s),
                        last_error_class = %s, last_error = %s
                    WHERE id = %s
                    """,
                    (new_state, delay, error_class, str(error), claimed.job.id),
                )

    return new_state


async def reap(conn: AsyncConnection) -> int:
    """Find state='running' jobs whose lease expired, close the open
    attempt as failed with error_class='lease_expired', and route through
    fail()'s retry-vs-dead_letter decision (plan §1's crash recovery: the
    claim predicate stays narrow; this is the one place that reclaims stale
    leases). One transaction; FOR UPDATE SKIP LOCKED makes concurrent
    reaping across workers safe. Returns the number of jobs reaped."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT id, event_id, handler_name, state, attempt_count, variant, trace_id
            FROM jobs
            WHERE state = 'running' AND lease_expires_at < now()
            FOR UPDATE SKIP LOCKED
            """
        )
        rows = await cur.fetchall()

        reaped = 0
        for row in rows:
            await cur.execute(
                "SELECT id FROM attempts WHERE job_id = %s AND state = 'running'",
                (row["id"],),
            )
            attempt_row = await cur.fetchone()
            if attempt_row is None:
                continue  # nothing open on this job to reap

            claimed = Claimed(
                job=Job(
                    id=row["id"],
                    event_id=row["event_id"],
                    handler_name=row["handler_name"],
                    state=JobState(row["state"]),
                    attempt_count=row["attempt_count"],
                    variant=row["variant"],
                    trace_id=row["trace_id"],
                ),
                event=None,
                attempt_id=attempt_row["id"],
                attempt_number=row["attempt_count"],
            )
            await fail(conn, claimed, RetryableError("lease_expired"))
            reaped += 1

        return reaped


async def replay(conn: AsyncConnection, job_id: UUID) -> UUID | None:
    """Insert a fresh Job from the dead-lettered job's Event, attempt_count=0,
    replay_of=<old job>. Never resurrects the old job (plan §1, §4) — the
    original stays in whatever state it was in, lineage is kept via
    replay_of.

    Only dead-lettered jobs are replayable. Without that predicate, replaying
    a *succeeded* job mints a second job for the same Event and re-runs the
    handler — routing straight around the idempotency guarantee that is the
    whole point of the ingress. The unique index on events.idempotency_key
    cannot help here: the Event already exists, so nothing conflicts.

    ponytail: returns None for both "no such job" and "not dead-lettered"
    rather than distinguishing them with an exception type. The caller says so
    in one message. Split it if an operator ever needs 404-vs-409.
    """
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT event_id, handler_name, variant, trace_id FROM jobs "
            "WHERE id = %s AND state = %s",
            (job_id, JobState.DEAD_LETTER),
        )
        row = await cur.fetchone()
        if row is None:
            return None

        await cur.execute(
            """
            INSERT INTO jobs (event_id, handler_name, state, trace_id, variant, replay_of)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                row["event_id"],
                row["handler_name"],
                JobState.PENDING,
                row["trace_id"],
                row["variant"],
                job_id,
            ),
        )
        new_row = await cur.fetchone()
        return new_row["id"]
