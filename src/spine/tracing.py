"""spine.tracing — OTel bootstrap and the real implementation of the four
job-lifecycle hooks (plan §2, §3 seam A; 00-SEAMS R3). worker.Hooks is a
no-op by default; build_hooks() below is what capability 2 wires at the
composition root (worker.main()).

Only `task` and `attempt` spans are built here. `chat` (gen_ai.*) spans and
the LLM chokepoint (seam B, plan §3) have NO PRODUCER on this slice: llm.py
is explicitly out of scope (no Anthropic SDK, no router) and the only
handler that runs today (echo_handler) makes no model calls. Wiring gen_ai
attributes onto a span nothing populates would be the same anti-pattern the
plan calls out for execute_tool/app.iterations/runaway_loop — inert
decoration. When llm.py exists, it wraps its own call and reports usage;
this module has nothing further to do to support that (it wraps llm.py from
the outside, per 00-SEAMS D3, not the reverse).
"""

from __future__ import annotations

import os
from uuid import UUID

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.propagate import extract
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import Span, Status, StatusCode
from psycopg_pool import AsyncConnectionPool

from spine import alerts
from spine.model import JobContext
from spine.worker import Hooks

_TRACER_NAME = "spine"


def bootstrap_tracing(service_name: str = "spine") -> None:
    """Set the global TracerProvider once, at process start (plan O2).

    Respects the standard `OTEL_SDK_DISABLED` env var for a true no-op mode —
    no bespoke on/off flag to maintain. BatchSpanProcessor exports on a
    background thread, so a nonexistent OTLP host degrades to dropped spans,
    never a blocked worker (O2's acceptance check).
    """
    base = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:6006")
    exporter = OTLPSpanExporter(endpoint=f"{base.rstrip('/')}/v1/traces")
    provider = TracerProvider(resource=Resource.create({"service.name": service_name}))
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)


def build_hooks(pool: AsyncConnectionPool) -> Hooks:
    """Real implementation of worker.Hooks (00-SEAMS R3's four hooks).

    One in-memory registry bridges on_job_start -> on_job_finish across the
    intervening attempt(s). A job never outlives the worker process it
    started in, so a plain dict is enough.

    ponytail: if workers ever checkpoint/resume a job across a process
    restart, this registry needs to move to the DB (trace_id + a stored
    span-context row) instead of memory. Not needed while one process owns
    a job start-to-finish, which is the tracer bullet's whole model.
    """
    tracer = trace.get_tracer(_TRACER_NAME)
    open_jobs: dict[UUID, Span] = {}
    open_attempts: dict[UUID, Span] = {}

    async def on_job_start(ctx: JobContext) -> None:
        if ctx.job_id in open_jobs:
            # worker.run() calls on_job_start on every claim, including a
            # retry's re-claim of the same job (it doesn't distinguish
            # first-attempt from retry) — not idempotent by construction.
            # The task span must survive across attempts (R2: job_id is
            # stable across retries), so a retry re-claim reuses the span
            # already open rather than minting a second trace.
            return
        # R1: traceparent is already carried through the queue on
        # ctx.trace_id. extract() silently no-ops for the uuid4 fallback
        # ingress.py uses when no traceparent header arrived — that's a
        # real new trace, not a parse failure.
        parent_ctx = extract({"traceparent": ctx.trace_id})
        span = tracer.start_span(
            "task",
            context=parent_ctx,
            attributes={
                "app.job_id": str(ctx.job_id),
                "app.variant": ctx.variant or "",
                # ponytail: JobContext has no task_type field — only
                # variant and the job-level hooks were added per 00-SEAMS
                # R3/R4. event.type (the handler's dispatch key) is the
                # closest real signal until a task_type field exists.
                "app.task_type": ctx.event.type,
            },
        )
        open_jobs[ctx.job_id] = span

    async def on_attempt_start(ctx: JobContext) -> None:
        parent = open_jobs.get(ctx.job_id)
        parent_ctx = trace.set_span_in_context(parent) if parent is not None else None
        span = tracer.start_span(
            "attempt",
            context=parent_ctx,
            attributes={
                "app.job_id": str(ctx.job_id),
                "app.attempt_number": ctx.attempt_number,
            },
        )
        open_attempts[ctx.job_id] = span

    async def on_attempt_end(ctx: JobContext, status: str) -> None:
        span = open_attempts.pop(ctx.job_id, None)
        if span is None:
            return
        if status != "succeeded":
            span.set_status(Status(StatusCode.ERROR))
        span.end()

    async def on_job_finish(ctx: JobContext, terminal_status: str) -> None:
        span = open_jobs.pop(ctx.job_id, None)
        if span is not None:
            span.set_attribute("app.terminal_status", terminal_status)
            if terminal_status != "success":
                span.set_status(Status(StatusCode.ERROR))
            span.end()

        # Alerting lives here too (plan §5): "the code that enforces the
        # limit is the code that raises the alert", evaluated inline at
        # job finish, no scheduler.
        async with pool.connection() as conn:
            await alerts.check_and_raise_failure_rate(
                conn, job_id=ctx.job_id, variant=ctx.variant
            )

    return Hooks(
        on_job_start=on_job_start,
        on_attempt_start=on_attempt_start,
        on_attempt_end=on_attempt_end,
        on_job_finish=on_job_finish,
    )
