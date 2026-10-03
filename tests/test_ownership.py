"""Real Postgres checks for attempt ownership after lease recovery."""

import asyncio

import pytest

from spine.handlers import register
from spine.model import HandlerResult, JobState, RetryableError, TerminalError
from spine.queue import claim, complete, enqueue, fail, reap
from spine.worker import Hooks, run


async def _enqueue(conn, *, source="demo", type="echo"):
    job_id, _ = await enqueue(
        conn,
        source=source,
        type=type,
        payload={"value": "original"},
        idempotency_key="ownership-test",
        trace_id="ownership-trace",
        handler_name=f"{source}:{type}",
        variant="default",
    )
    return job_id


async def _recover(conn, claimed):
    async with conn.cursor() as cur:
        await cur.execute(
            "UPDATE jobs SET lease_expires_at = now() - interval '1 second' "
            "WHERE id = %s",
            (claimed.job.id,),
        )
    assert await reap(conn) == 1
    async with conn.cursor() as cur:
        await cur.execute(
            "UPDATE jobs SET run_after = now() WHERE id = %s", (claimed.job.id,)
        )
    replacement = await claim(conn, lease_seconds=30)
    assert replacement is not None
    assert replacement.attempt_number == claimed.attempt_number + 1
    return replacement


async def _snapshot(conn):
    async with conn.cursor() as cur:
        await cur.execute("SELECT * FROM jobs ORDER BY id")
        jobs = await cur.fetchall()
        await cur.execute("SELECT * FROM attempts ORDER BY attempt_number")
        attempts = await cur.fetchall()
        await cur.execute("SELECT * FROM llm_calls ORDER BY id")
        llm_calls = await cur.fetchall()
    return jobs, attempts, llm_calls


@pytest.mark.parametrize("outcome", ["success", "retryable", "terminal"])
async def test_reclaimed_attempt_rejects_stale_writes(pool, outcome):
    async with pool.connection() as conn:
        await _enqueue(conn)
        original = await claim(conn, lease_seconds=30)
    assert original is not None
    async with pool.connection() as conn:
        replacement = await _recover(conn, original)
        async with conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO llm_calls (attempt_id, attempt_idx, model, cost_micros) "
                "VALUES (%s, 0, 'fixture', 17)",
                (replacement.attempt_id,),
            )
    async with pool.connection() as conn:
        before = await _snapshot(conn)
        if outcome == "success":
            accepted = await complete(
                conn,
                original,
                HandlerResult(
                    output={"owner": "stale"}, llm_calls=[{"model": "stale"}]
                ),
            )
            assert accepted is False
        else:
            error = (
                RetryableError("stale")
                if outcome == "retryable"
                else TerminalError("stale")
            )
            assert await fail(conn, original, error) is None
        assert await _snapshot(conn) == before
        assert await complete(
            conn, replacement, HandlerResult(output={"owner": "current"})
        ) is True
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT state, output FROM jobs WHERE id = %s", (original.job.id,)
            )
            assert await cur.fetchone() == (JobState.SUCCEEDED, {"owner": "current"})
            await cur.execute(
                "SELECT state, error_class, output FROM attempts WHERE id = %s",
                (original.attempt_id,),
            )
            assert await cur.fetchone() == ("failed", "lease_expired", None)


async def test_completed_attempt_cannot_finish_again(pool):
    async with pool.connection() as conn:
        await _enqueue(conn)
        claimed = await claim(conn, lease_seconds=30)
        assert claimed is not None
        await complete(conn, claimed, HandlerResult(output={"owner": "first"}))
    async with pool.connection() as conn:
        before = await _snapshot(conn)
        assert await complete(
            conn, claimed, HandlerResult(output={"owner": "second"})
        ) is False
        assert await fail(conn, claimed, TerminalError("late")) is None
        assert await _snapshot(conn) == before


@pytest.mark.parametrize("outcome", ["success", "terminal"])
async def test_stale_worker_closes_attempt_hook_without_terminal_finish(pool, outcome):
    stop = asyncio.Event()
    attempt_ends = []
    finishes = []

    async def handler(ctx):
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "UPDATE jobs SET lease_expires_at = now() - interval '1 second' "
                    "WHERE id = %s",
                    (ctx.job_id,),
                )
            assert await reap(conn) == 1
            async with conn.cursor() as cur:
                await cur.execute(
                    "UPDATE jobs SET run_after = now() WHERE id = %s", (ctx.job_id,)
                )
            assert await claim(conn, lease_seconds=30) is not None
        stop.set()
        if outcome == "terminal":
            raise TerminalError("late failure")
        return HandlerResult(output={"late": True})

    async def attempt_end(ctx, status):
        attempt_ends.append((ctx.attempt_number, status))

    async def job_finish(ctx, status):
        finishes.append((ctx.attempt_number, status))

    register("ownership", outcome, handler)
    async with pool.connection() as conn:
        await _enqueue(conn, source="ownership", type=outcome)
    await asyncio.wait_for(
        run(
            pool,
            stop,
            hooks=Hooks(on_attempt_end=attempt_end, on_job_finish=job_finish),
        ),
        timeout=3,
    )
    assert attempt_ends == [(1, "stale")]
    assert finishes == []
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT state, attempt_count, output FROM jobs")
            assert await cur.fetchone() == ("running", 2, None)


async def test_unknown_handler_dead_letters_first_attempt(pool):
    stop = asyncio.Event()
    finishes = []

    async def attempt_end(ctx, status):
        stop.set()

    async def job_finish(ctx, status):
        finishes.append(status)

    async with pool.connection() as conn:
        await _enqueue(conn, source="unknown", type="missing")
    await asyncio.wait_for(
        run(
            pool,
            stop,
            hooks=Hooks(on_attempt_end=attempt_end, on_job_finish=job_finish),
        ),
        timeout=3,
    )
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT state, attempt_count, last_error_class FROM jobs")
            assert await cur.fetchone() == ("dead_letter", 1, "unknown_handler")
            await cur.execute("SELECT count(*) FROM attempts")
            assert await cur.fetchone() == (1,)
    assert finishes == ["dead_lettered"]
