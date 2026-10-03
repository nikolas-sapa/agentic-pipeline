"""Runnable check for the tracer bullet (ticket 1) and idempotency (ticket 2).
Hits real Postgres — mocking SKIP LOCKED / ON CONFLICT tests nothing (plan §5).
"""

import asyncio

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from psycopg.rows import dict_row

from spine import alerts, metrics
from spine.handlers import echo_handler, register  # noqa: F401  (registers demo handler)
from spine.model import HandlerResult, RetryableError, TerminalError
from spine.queue import claim, complete, enqueue, reap, replay
from spine.tracing import build_hooks
from spine.worker import run as worker_run

# One process-wide TracerProvider backed by an in-memory exporter (plan O5's
# accept check needs finished spans, not a live Phoenix). Set once at import,
# same pattern as spine.tracing.bootstrap_tracing but swapping the OTLP
# exporter for something a test can read back.
_span_exporter = InMemorySpanExporter()
_test_provider = TracerProvider()
_test_provider.add_span_processor(SimpleSpanProcessor(_span_exporter))
trace.set_tracer_provider(_test_provider)

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


async def test_retry_then_succeed_is_one_trace_two_attempt_spans(pool):
    """O5's acceptance check: a job that fails once and succeeds on retry
    produces ONE trace with two `attempt` spans, both children of the same
    `task` span — this is what makes retry rate and end-to-end latency
    computable at all (plan §2)."""
    _span_exporter.clear()
    async with pool.connection() as conn:
        job_id, _ = await enqueue(
            conn,
            source="test",
            type="flaky",
            payload={},
            idempotency_key="trace-flaky-key",
            trace_id="trace-flaky-2",
            handler_name="test:flaky",
            variant="default",
        )

    stop_event = asyncio.Event()
    worker_task = asyncio.create_task(
        worker_run(
            pool,
            stop_event,
            lease_seconds=30,
            poll_interval_seconds=0.1,
            hooks=build_hooks(pool),
        )
    )
    try:
        row = await _poll_until(pool, job_id, {"succeeded", "dead_letter"})
    finally:
        stop_event.set()
        await worker_task

    assert row["state"] == "succeeded"

    spans = _span_exporter.get_finished_spans()
    task_spans = [
        s for s in spans if s.name == "task" and s.attributes.get("app.job_id") == str(job_id)
    ]
    attempt_spans = [
        s
        for s in spans
        if s.name == "attempt" and s.attributes.get("app.job_id") == str(job_id)
    ]

    assert len(task_spans) == 1
    assert len(attempt_spans) == 3  # flaky fails twice, succeeds on the 3rd
    trace_id = task_spans[0].context.trace_id
    assert all(s.context.trace_id == trace_id for s in attempt_spans)
    assert all(s.parent.span_id == task_spans[0].context.span_id for s in attempt_spans)
    assert task_spans[0].attributes["app.terminal_status"] == "success"


async def test_dead_letter_share_past_alert_threshold(pool):  # CLAUDE_SECRET_ALLOW
    """O7's failure_rate kind — the only alert kind with a real producer on
    this slice (runaway_loop and cost_anomaly have none; see spine.alerts).
    13 succeeded + 7 dead_letter over N=20 is a 35% dead-letter share, over
    the 30% threshold."""
    variant = "alert-test-variant"
    async with pool.connection() as conn:
        for i in range(13):
            job_id, _ = await enqueue(
                conn,
                source="demo",
                type="echo",
                payload={"i": i},
                idempotency_key=f"dl-ok-{i}",
                trace_id=f"t-ok-{i}",
                handler_name="demo:echo",
                variant=variant,
            )
            async with conn.cursor() as cur:
                await cur.execute(
                    "UPDATE jobs SET state = 'succeeded' WHERE id = %s", (job_id,)
                )

        for i in range(7):
            job_id, _ = await enqueue(
                conn,
                source="demo",
                type="echo",
                payload={"i": i},
                idempotency_key=f"dl-bad-{i}",
                trace_id=f"t-bad-{i}",
                handler_name="demo:echo",
                variant=variant,
            )
            async with conn.cursor() as cur:
                await cur.execute(
                    "UPDATE jobs SET state = 'dead_letter' WHERE id = %s", (job_id,)
                )

        await alerts.check_and_raise_failure_rate(conn, job_id=job_id, variant=variant)

        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT kind, variant, detail_json FROM alerts WHERE variant = %s",
                (variant,),
            )
            rows = await cur.fetchall()

    assert len(rows) == 1
    assert rows[0]["kind"] == "failure_rate"
    assert rows[0]["variant"] == variant
    assert rows[0]["detail_json"]["failure_rate"] > 0.3


async def test_task_metrics_view_and_percentiles(pool):
    """O6 + plan §4: task_metrics has a row per terminal task with
    duration_ms from the attempt rollup, and percentile_cont computes
    p50/p95 over it — Postgres is available so this is the live path, not
    the plan's SQLite fallback (00-SEAMS open question 4)."""
    variant = "percentile-test-variant"
    durations_s = [0.1, 0.2, 0.3, 0.4, 0.5]
    async with pool.connection() as conn:
        for i, secs in enumerate(durations_s):
            await enqueue(
                conn,
                source="demo",
                type="echo",
                payload={},
                idempotency_key=f"pct-{i}",
                trace_id=f"t-pct-{i}",
                handler_name="demo:echo",
                variant=variant,
            )
            claimed = await claim(conn, lease_seconds=30)
            assert claimed is not None
            async with conn.cursor() as cur:
                await cur.execute(
                    "UPDATE attempts SET started_at = now() - make_interval(secs => %s) "
                    "WHERE id = %s",
                    (secs, claimed.attempt_id),
                )
            await complete(conn, claimed, HandlerResult(output={"ok": True}))

        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT count(*) AS n FROM task_metrics WHERE variant = %s", (variant,)
            )
            n = (await cur.fetchone())["n"]

        result = await metrics.latency_percentiles(conn, variant)

    assert n == len(durations_s)
    assert 380 <= result["p95_ms"] <= 520
    assert 250 <= result["p50_ms"] <= 350
