"""spine.alerts — "the code that enforces the limit is the code that raises
the alert" (plan §5). No Prometheus, no scheduler: evaluated inline at
on_job_finish (tracing.build_hooks), one INSERT, stderr JSON, optional
webhook.

Only `failure_rate` is implemented here. The other two kinds from the plan
have no real producer today:

- `runaway_loop` fires when agent iterations exceed a cap, but the spine has
  no iteration cap — every handler runs single-turn (plan §2's "blocked, not
  deleted" note). There is nothing to breach.
- `cost_anomaly` compares a task's cost to the rolling median for its
  task_type, sourced from `llm_calls`. Nothing writes that table yet — llm.py
  is out of scope for this slice (no Anthropic SDK, no router calls). The
  query is real SQL once that table has rows; building it now would be
  instrumenting a signal nothing produces, same as runaway_loop.

`failure_rate` has a real producer today: the existing retry/dead-letter
taxonomy in spine.queue already produces terminal jobs to count, no new
scaffolding needed.
"""

from __future__ import annotations

import asyncio
import json
import sys
import urllib.request
from os import environ
from uuid import UUID

from psycopg import AsyncConnection
from psycopg.rows import dict_row
from psycopg.types.json import Json

FAILURE_RATE_N = int(environ.get("ALERT_FAILURE_RATE_N", "20"))
FAILURE_RATE_THRESHOLD = float(environ.get("ALERT_FAILURE_RATE_THRESHOLD", "0.3"))


async def check_and_raise_failure_rate(
    conn: AsyncConnection, *, job_id: UUID, variant: str | None
) -> None:
    """Last FAILURE_RATE_N terminal jobs for this variant; if the
    dead-letter share exceeds FAILURE_RATE_THRESHOLD, raise one alert."""
    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT state FROM jobs
            WHERE state IN ('succeeded', 'dead_letter') AND variant = %s
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (variant, FAILURE_RATE_N),
        )
        rows = await cur.fetchall()

    if len(rows) < FAILURE_RATE_N:
        return  # not enough terminal history yet to judge a rate

    failed = sum(1 for (state,) in rows if state == "dead_letter")
    rate = failed / FAILURE_RATE_N
    if rate <= FAILURE_RATE_THRESHOLD:
        return

    await _raise_alert(
        conn,
        kind="failure_rate",
        job_id=job_id,
        variant=variant,
        detail={
            "failure_rate": rate,
            "n": FAILURE_RATE_N,
            "threshold": FAILURE_RATE_THRESHOLD,
        },
    )


async def _raise_alert(
    conn: AsyncConnection,
    *,
    kind: str,
    job_id: UUID | None,
    variant: str | None,
    detail: dict,
) -> None:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            INSERT INTO alerts (kind, task_id, detail_json, variant)
            VALUES (%s, %s, %s, %s)
            """,
            (kind, job_id, Json(detail), variant),
        )

    line = {
        "alert": kind,
        "task_id": str(job_id) if job_id else None,
        "variant": variant,
        **detail,
    }
    print(json.dumps(line), file=sys.stderr)

    webhook_url = environ.get("ALERT_WEBHOOK_URL")
    if webhook_url:
        await asyncio.to_thread(_post_webhook, webhook_url, line)


def _post_webhook(url: str, payload: dict) -> None:
    """Unset by default (plan §5) — no account required to run the repo.
    stdlib only: the alert row is inserted and stderr is written before
    this runs; a failed POST is swallowed so the DB transaction can commit."""
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=5):
            pass
    except OSError:
        pass
