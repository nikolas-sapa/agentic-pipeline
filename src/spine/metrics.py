"""spine.metrics — percentile queries over task_metrics (plan §4).

Postgres only: 00-SEAMS' open question 4 is answered (Postgres, so the
plan's SQLite sort-in-Python fallback is dead code — never built here).
"""

from __future__ import annotations

from psycopg import AsyncConnection
from psycopg.rows import dict_row


async def latency_percentiles(conn: AsyncConnection, variant: str) -> dict[str, float | None]:
    """p50/p95 of the FINAL attempt's duration, successful tasks only (plan §4:
    failures skew both directions).

    This is attempt latency, not end-to-end task latency: task_metrics.duration_ms
    comes from the last attempt only, so a job that retried twice reports just its
    winning run and excludes both failed attempts and the backoff between them.
    For queue-wait-inclusive latency use finished_at - started_at from the same
    view. The distinction matters the moment retries are common, and the two
    numbers are furthest apart exactly for the slow jobs anyone would care about.

    ponytail: not exposing an end-to-end percentile until something asks for it —
    the benchmark dropped its latency claim (CLI substrate overhead makes it
    unmeasurable honestly), so a second percentile would have no consumer today.
    """
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT
                percentile_cont(0.5) WITHIN GROUP (ORDER BY duration_ms) AS p50,
                percentile_cont(0.95) WITHIN GROUP (ORDER BY duration_ms) AS p95
            FROM task_metrics
            WHERE variant = %s AND terminal_status = 'succeeded'
            """,
            (variant,),
        )
        row = await cur.fetchone()

    return {"p50_ms": row["p50"], "p95_ms": row["p95"]}
