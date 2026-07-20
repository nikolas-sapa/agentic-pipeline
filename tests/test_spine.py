"""Runnable check for the tracer bullet (ticket 1) and idempotency (ticket 2).
Hits real Postgres — mocking SKIP LOCKED / ON CONFLICT tests nothing (plan §5).
"""

import asyncio

import pytest
from psycopg_pool import AsyncConnectionPool

from spine.db import Settings, run_migrations
from spine.handlers import echo_handler  # noqa: F401  (registers demo handler)
from spine.model import HandlerResult
from spine.queue import claim, complete, enqueue
from spine.worker import run as worker_run


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
