"""spine.ingress — FastAPI app. Interface = the HTTP surface. Thin by
design: parse, build Event, enqueue, return (plan §2).

ponytail: only POST /events, GET /jobs/{id}, GET /healthz exist. POST
/jobs/{id}/replay and GET /jobs?state=dead_letter are ticket 7 (DLQ + replay)
— out of scope until the retry/dead-letter path lands.
"""

from __future__ import annotations

import uuid
from typing import Any
from uuid import UUID

from fastapi import FastAPI, HTTPException, Request
from psycopg.rows import dict_row
from pydantic import BaseModel

from spine.db import Settings, make_pool, run_migrations
from spine.model import idempotency_key
from spine.queue import enqueue

app = FastAPI(title="spine")
settings = Settings.from_env()
pool = make_pool(settings)


@app.on_event("startup")
async def startup() -> None:
    await pool.open()
    async with pool.connection() as conn:
        await run_migrations(conn)


@app.on_event("shutdown")
async def shutdown() -> None:
    await pool.close()


class EventIn(BaseModel):
    source: str
    type: str
    payload: dict[str, Any]
    external_id: str | None = None


@app.post("/events", status_code=202)
async def post_event(body: EventIn, request: Request):
    trace_id = request.headers.get("traceparent") or str(uuid.uuid4())
    key = idempotency_key(body.source, body.external_id, body.payload)

    async with pool.connection() as conn:
        job_id, is_new = await enqueue(
            conn,
            source=body.source,
            type=body.type,
            payload=body.payload,
            idempotency_key=key,
            trace_id=trace_id,
            handler_name=f"{body.source}:{body.type}",
            variant=settings.variant,
        )

    status_code = 202 if is_new else 200
    return _json_response({"job_id": str(job_id)}, status_code)


def _json_response(payload: dict, status_code: int):
    from fastapi.responses import JSONResponse

    return JSONResponse(content=payload, status_code=status_code)


@app.get("/jobs/{job_id}")
async def get_job(job_id: UUID):
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT id, state, attempt_count, variant, output, trace_id
                FROM jobs WHERE id = %s
                """,
                (job_id,),
            )
            row = await cur.fetchone()

    if row is None:
        raise HTTPException(status_code=404, detail="job not found")

    return {
        "id": str(row["id"]),
        "state": row["state"],
        "attempt_count": row["attempt_count"],
        "variant": row["variant"],
        "output": row["output"],
        "trace_id": row["trace_id"],
    }


@app.get("/healthz")
async def healthz():
    return {"ok": True}
