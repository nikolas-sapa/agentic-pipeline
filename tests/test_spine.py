"""Runnable check for the tracer bullet (ticket 1) and idempotency (ticket 2).
Hits real Postgres — mocking SKIP LOCKED / ON CONFLICT tests nothing (plan §5).
"""

import asyncio

import pytest
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from spine.db import Settings, run_migrations
from spine.handlers import echo_handler, register  # noqa: F401  (registers demo handler)
from spine.model import HandlerResult, RetryableError, TerminalError
from spine.queue import claim, complete, enqueue, reap, replay
from spine.worker import run as worker_run

# Test-only handlers for the failure taxonomy (plan §4). Registered at import
# time like the demo handler; kept here rather than in spine.handlers because
# they exist only to exercise retry/dead-letter, not as shipped behaviour.
_flaky_fail_counts: dict[str, int] = {}


async def _flaky_handler(ctx):
    """Fails with a RetryableError twice, then succeeds on the 3rd attempt."""
    n = _flaky_fail_counts.get(ctx.idempotency_key, 0)
    if n < 2:
        _flaky_fail_counts[ctx.idempotency_key] = n + 1
        raise RetryableError("flaky")
    return HandlerResult(output={"ok": True})


async def _terminal_handler(ctx):
    raise TerminalError("bad_input")


async def _always_fail_handler(ctx):
    raise RetryableError("always")


register("test", "flaky", _flaky_handler)
register("test", "terminal", _terminal_handler)
register("test", "always_fail", _always_fail_handler)


async def _poll_until(pool, job_id, states, *, timeout=15.0, interval=0.1):
    """Poll jobs.state until it lands in `states` or timeout. Returns the
    last-seen row either way (assertions on the caller's side surface a
    timeout as a normal failed assertion, not a hang)."""
    elapsed = 0.0
    row = None
    while elapsed < timeout:
        async with pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    "SELECT state, attempt_count, last_error_class FROM jobs WHERE id = %s",
                    (job_id,),
                )
                row = await cur.fetchone()
        if row["state"] in states:
            return row
        await asyncio.sleep(interval)
        elapsed += interval
    return row


@pytest.fixture
async def pool():
    settings = Settings.from_env()
    p = AsyncConnectionPool(conninfo=settings.database_url, open=False)
    await p.open()
    async with p.connection() as conn:
        await run_migrations(conn)
        async with conn.cursor() as cur:
            await cur.execute("TRUNCATE events, jobs, attempts, llm_calls CASCADE")
        await conn.commit()
    yield p
    await p.close()


async def test_tracer_bullet_reaches_succeeded(pool):
    async with pool.connection() as conn:
        job_id, is_new = await enqueue(
            conn,
            source="demo",
            type="echo",
            payload={"hello": "world"},
            idempotency_key="test-key-1",
            trace_id="trace-1",
            handler_name="demo:echo",
            variant="default",
        )
    assert is_new is True

    stop_event = asyncio.Event()
    worker_task = asyncio.create_task(
        worker_run(pool, stop_event, lease_seconds=30, poll_interval_seconds=0.1)
    )
    try:
        job = None
        for _ in range(50):
            async with pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "SELECT state, output FROM jobs WHERE id = %s", (job_id,)
                    )
                    job = await cur.fetchone()
            if job[0] == "succeeded":
                break
            await asyncio.sleep(0.1)
    finally:
        stop_event.set()
        await worker_task

    assert job[0] == "succeeded"
    assert job[1] == {"echoed": {"hello": "world"}}


async def test_duration_ms_survives_a_minute(pool):
    """Regression: EXTRACT(MILLISECONDS FROM interval) returns only the seconds
    field, so a 90s attempt recorded as 30000 instead of 90000 — silently, and
    only for jobs slow enough to care about. duration_ms feeds p95 latency.
    Backdates started_at rather than sleeping 90s.
    """
    async with pool.connection() as conn:
        job_id, _ = await enqueue(
            conn,
            source="demo",
            type="echo",
            payload={"slow": True},
            idempotency_key="slow-key",
            trace_id="trace-slow",
            handler_name="demo:echo",
            variant="default",
        )
        claimed = await claim(conn, lease_seconds=30)
        assert claimed is not None
        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE attempts SET started_at = now() - interval '90 seconds' "
                "WHERE id = %s",
                (claimed.attempt_id,),
            )
        await complete(conn, claimed, HandlerResult(output={"ok": True}))
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT duration_ms FROM attempts WHERE id = %s", (claimed.attempt_id,)
            )
            (duration_ms,) = await cur.fetchone()

    assert 89_000 < duration_ms < 92_000, (
        f"expected ~90000ms, got {duration_ms} — "
        "EXTRACT(MILLISECONDS) truncation is back"
    )


async def test_duplicate_event_returns_same_job_id(pool):
    async with pool.connection() as conn:
        job_id_1, is_new_1 = await enqueue(
            conn,
            source="demo",
            type="echo",
            payload={"a": 1},
            idempotency_key="dup-key",
            trace_id="trace-2",
            handler_name="demo:echo",
            variant="default",
        )
    async with pool.connection() as conn:
        job_id_2, is_new_2 = await enqueue(
            conn,
            source="demo",
            type="echo",
            payload={"a": 1},
            idempotency_key="dup-key",
            trace_id="trace-2",
            handler_name="demo:echo",
            variant="default",
        )

    assert job_id_1 == job_id_2
    assert is_new_1 is True
    assert is_new_2 is False

    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT count(*) FROM events WHERE idempotency_key = %s", ("dup-key",)
            )
            (count,) = await cur.fetchone()
    assert count == 1


async def test_flaky_handler_retries_then_succeeds(pool):
    """Ticket 4: a handler failing twice then succeeding => 3 attempts,
    final state succeeded. Backoff delays are full-jitter random (plan §4),
    so this asserts attempt count/outcome, not exact gap sizes."""
    async with pool.connection() as conn:
        job_id, _ = await enqueue(
            conn,
            source="test",
            type="flaky",
            payload={},
            idempotency_key="flaky-key",
            trace_id="trace-flaky",
            handler_name="test:flaky",
            variant="default",
        )

    stop_event = asyncio.Event()
    worker_task = asyncio.create_task(
        worker_run(pool, stop_event, lease_seconds=30, poll_interval_seconds=0.1)
    )
    try:
        row = await _poll_until(pool, job_id, {"succeeded", "dead_letter"})
    finally:
        stop_event.set()
        await worker_task

    assert row["state"] == "succeeded"

    async with pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT attempt_number, state, error_class FROM attempts "
                "WHERE job_id = %s ORDER BY attempt_number",
                (job_id,),
            )
            attempts = await cur.fetchall()

    assert [a["state"] for a in attempts] == ["failed", "failed", "succeeded"]
    assert attempts[0]["error_class"] == "flaky"
    assert attempts[1]["error_class"] == "flaky"


async def test_terminal_error_dead_letters_immediately(pool):
    """Ticket 5: TerminalError => dead letter after 1 attempt, no retries."""
    async with pool.connection() as conn:
        job_id, _ = await enqueue(
            conn,
            source="test",
            type="terminal",
            payload={},
            idempotency_key="terminal-key",
            trace_id="trace-terminal",
            handler_name="test:terminal",
            variant="default",
        )

    stop_event = asyncio.Event()
    worker_task = asyncio.create_task(
        worker_run(pool, stop_event, lease_seconds=30, poll_interval_seconds=0.1)
    )
    try:
        row = await _poll_until(pool, job_id, {"succeeded", "dead_letter"})
    finally:
        stop_event.set()
        await worker_task

    assert row["state"] == "dead_letter"
    assert row["attempt_count"] == 1
    assert row["last_error_class"] == "bad_input"

    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT count(*) FROM attempts WHERE job_id = %s", (job_id,))
            (count,) = await cur.fetchone()
    assert count == 1


async def test_always_failing_handler_exhausts_into_dead_letter(pool):
    """Ticket 5: always-failing handler => dead_letter after 5 attempts
    (DEFAULT_RETRY_POLICY.max_attempts)."""
    async with pool.connection() as conn:
        job_id, _ = await enqueue(
            conn,
            source="test",
            type="always_fail",
            payload={},
            idempotency_key="always-fail-key",
            trace_id="trace-always",
            handler_name="test:always_fail",
            variant="default",
        )

    stop_event = asyncio.Event()
    worker_task = asyncio.create_task(
        worker_run(pool, stop_event, lease_seconds=30, poll_interval_seconds=0.1)
    )
    try:
        # Worst case is 4 backoff waits at full jitter up to 1/2/4/8s each;
        # 30s leaves comfortable margin over the ~15s theoretical max.
        row = await _poll_until(pool, job_id, {"succeeded", "dead_letter"}, timeout=30.0)
    finally:
        stop_event.set()
        await worker_task

    assert row["state"] == "dead_letter"
    assert row["attempt_count"] == 5

    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT count(*) FROM attempts WHERE job_id = %s AND state = 'failed'",
                (job_id,),
            )
            (count,) = await cur.fetchone()
    assert count == 5


async def test_reaper_recovers_expired_lease_into_dead_letter(pool):
    """Ticket 6: a job whose lease expires (crashed worker) is recovered by
    reap(), not the claim query. Drives it through 5 claim+backdate+reap
    rounds (rather than a real worker) so the crash is deterministic and
    doesn't need a sleep for the lease to actually expire."""
    async with pool.connection() as conn:
        job_id, _ = await enqueue(
            conn,
            source="test",
            type="never_runs",
            payload={},
            idempotency_key="reap-key",
            trace_id="trace-reap",
            handler_name="test:never_runs",
            variant="default",
        )

    for _ in range(5):
        async with pool.connection() as conn:
            claimed = await claim(conn, lease_seconds=30)
            assert claimed is not None
            async with conn.cursor() as cur:
                await cur.execute(
                    "UPDATE jobs SET lease_expires_at = now() - interval '1 second' "
                    "WHERE id = %s",
                    (claimed.job.id,),
                )
            reaped = await reap(conn)
            assert reaped == 1
            # Un-backdate run_after so the next round's claim() doesn't wait
            # out fail()'s randomized backoff — this test is about the
            # reaper, not the retry delay.
            async with conn.cursor() as cur:
                await cur.execute(
                    "UPDATE jobs SET run_after = now() WHERE id = %s", (claimed.job.id,)
                )

    async with pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT state, attempt_count FROM jobs WHERE id = %s", (job_id,)
            )
            job = await cur.fetchone()
            await cur.execute(
                "SELECT count(*) AS n FROM attempts "
                "WHERE job_id = %s AND error_class = 'lease_expired'",
                (job_id,),
            )
            lease_expired_count = (await cur.fetchone())["n"]

    assert job["state"] == "dead_letter"
    assert job["attempt_count"] == 5
    assert lease_expired_count == 5


async def test_replay_creates_new_job_with_lineage(pool):
    """Ticket 7: replay of a dead-lettered job creates a new job with
    replay_of set; the original stays in its terminal state."""
    async with pool.connection() as conn:
        job_id, _ = await enqueue(
            conn,
            source="test",
            type="never_runs",
            payload={"x": 1},
            idempotency_key="replay-key",
            trace_id="trace-replay",
            handler_name="test:never_runs",
            variant="default",
        )
        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE jobs SET state = 'dead_letter', last_error_class = 'bad_input' "
                "WHERE id = %s",
                (job_id,),
            )

        new_job_id = await replay(conn, job_id)

        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT state, attempt_count, replay_of FROM jobs WHERE id = %s",
                (new_job_id,),
            )
            new_job = await cur.fetchone()
            await cur.execute("SELECT state FROM jobs WHERE id = %s", (job_id,))
            original_state = (await cur.fetchone())["state"]

    assert new_job_id != job_id
    assert new_job["state"] == "pending"
    assert new_job["attempt_count"] == 0
    assert new_job["replay_of"] == job_id
    assert original_state == "dead_letter"


async def test_replay_refuses_a_job_that_is_not_dead_lettered(pool):
    """Only dead-lettered jobs are replayable. Replaying a succeeded job would
    mint a second job for the same Event and re-run the handler — routing
    around the ingress idempotency guarantee, since the Event already exists
    and so nothing conflicts on its unique index. Double execution.
    """
    async with pool.connection() as conn:
        job_id, _ = await enqueue(
            conn,
            source="test",
            type="echo",
            payload={"x": 1},
            idempotency_key="replay-guard-key",
            trace_id="trace-replay-guard",
            handler_name="demo:echo",
            variant="default",
        )
        claimed = await claim(conn, lease_seconds=30)
        assert claimed is not None
        await complete(conn, claimed, HandlerResult(output={"done": True}))

        refused = await replay(conn, job_id)

        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT count(*) FROM jobs WHERE event_id = "
                "(SELECT event_id FROM jobs WHERE id = %s)",
                (job_id,),
            )
            (job_count,) = await cur.fetchone()

    assert refused is None, "replay must refuse a succeeded job"
    assert job_count == 1, f"expected no duplicate job for the event, found {job_count}"
