"""spine.queue — the deep module. All SQL, the SKIP LOCKED claim, and the
idempotency race live here and nowhere else (plan §2).

ponytail: only enqueue/claim/complete exist. fail()/reap()/replay() are
plan §2's full interface but belong to tickets 4-7 (retry, dead letter,
lease recovery, replay) — out of scope for the tracer bullet. The happy-path
demo handler never fails, so there is nothing for them to do yet.
"""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from psycopg import AsyncConnection
from psycopg.rows import dict_row
from psycopg.types.json import Json

from spine.model import Event, HandlerResult, Job, JobState


@dataclass(frozen=True)
class Claimed:
    job: Job
    event: Event
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


async def complete(
    conn: AsyncConnection, claimed: Claimed, result: HandlerResult
) -> None:
    """Close the attempt and the job as succeeded. Every state change writes
    an attempt row (plan §1 invariant) — claim() already opened it; this
    closes it in the same lifecycle (`running -> succeeded`, plan §1)."""
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
