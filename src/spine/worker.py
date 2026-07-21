"""spine.worker — the poll loop and dispatch to the Handler. No SQL (plan §2).

Four hooks (plan §6.1, per 00-SEAMS R3): on_job_start, on_attempt_start,
on_attempt_end, on_job_finish. No-op by default, passed into run() at the
composition root (main() below) — not monkeypatched over the module.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Awaitable, Callable

from psycopg_pool import AsyncConnectionPool

from spine import handlers
from spine.model import JobContext, JobState
from spine.queue import claim, complete, fail, reap

Hook = Callable[..., Awaitable[None]]


async def _noop(*args, **kwargs) -> None:
    return None


@dataclass(frozen=True)
class Hooks:
    on_job_start: Hook = _noop
    on_attempt_start: Hook = _noop
    on_attempt_end: Hook = _noop
    on_job_finish: Hook = _noop


async def run(
    pool: AsyncConnectionPool,
    stop_event: asyncio.Event,
    *,
    lease_seconds: int = 30,
    poll_interval_seconds: float = 0.5,
    hooks: Hooks = Hooks(),
) -> None:
    while not stop_event.is_set():
        claimed = None
        async with pool.connection() as conn:
            # Every poll reaps first (plan §1): stale `running` rows from a
            # crashed worker get their attempt closed and routed through the
            # same retry-vs-dead_letter decision as a normal failure.
            await reap(conn)
            claimed = await claim(conn, lease_seconds)

        if claimed is None:
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=poll_interval_seconds)
            except asyncio.TimeoutError:
                pass
            continue

        ctx = JobContext(
            event=claimed.event,
            job_id=claimed.job.id,
            attempt_number=claimed.attempt_number,
            idempotency_key=claimed.event.idempotency_key,
            trace_id=claimed.job.trace_id,
            variant=claimed.job.variant,
        )

        await hooks.on_job_start(ctx)
        await hooks.on_attempt_start(ctx)

        try:
            handler = handlers.resolve(claimed.event)
            result = await handler(ctx)
        except Exception as error:
            # Taxonomy classification (RetryableError vs TerminalError vs
            # an undeclared bug) and the retry-vs-dead_letter decision both
            # live in queue.fail() — the worker just routes the exception
            # there (plan §4).
            await hooks.on_attempt_end(ctx, "failed")
            async with pool.connection() as conn:
                new_state = await fail(conn, claimed, error)
            # on_job_finish fires only when the job reaches a terminal state;
            # a job requeued for retry isn't finished yet.
            if new_state == JobState.DEAD_LETTER:
                await hooks.on_job_finish(ctx, "dead_lettered")
            continue

        await hooks.on_attempt_end(ctx, "succeeded")

        async with pool.connection() as conn:
            await complete(conn, claimed, result)

        await hooks.on_job_finish(ctx, "success")


def main() -> None:
    """Composition root: hooks are wired here (or replaced here by capability
    2), never monkeypatched onto the module."""
    from spine.db import Settings, make_pool

    settings = Settings.from_env()
    pool = make_pool(settings)
    stop_event = asyncio.Event()

    async def _run():
        await pool.open()
        try:
            await run(
                pool,
                stop_event,
                lease_seconds=settings.lease_seconds,
                poll_interval_seconds=settings.poll_interval_seconds,
                hooks=Hooks(),
            )
        finally:
            await pool.close()

    asyncio.run(_run())


if __name__ == "__main__":
    main()
