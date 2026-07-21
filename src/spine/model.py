"""Dataclasses, enums, and the idempotency key derivation."""

from __future__ import annotations

import hashlib
import json
import random
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any
from uuid import UUID


class JobState(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    DEAD_LETTER = "dead_letter"


def idempotency_key(source: str, external_id: str | None, payload: dict[str, Any]) -> str:
    """sha256(source + ":" + external_id), falling back to a payload hash
    when the provider gives no event id (plan §1)."""
    basis = external_id if external_id is not None else json.dumps(payload, sort_keys=True)
    return hashlib.sha256(f"{source}:{basis}".encode()).hexdigest()


@dataclass(frozen=True)
class Event:
    id: UUID
    source: str
    type: str
    payload: dict[str, Any]
    idempotency_key: str
    trace_id: str
    created_at: datetime


@dataclass(frozen=True)
class Job:
    id: UUID
    event_id: UUID
    handler_name: str
    state: JobState
    attempt_count: int
    variant: str | None
    trace_id: str
    output: dict[str, Any] | None = None


@dataclass(frozen=True)
class JobContext:
    """What the spine hands a handler. Fields per plan §6.2."""

    event: Event
    job_id: UUID
    attempt_number: int
    idempotency_key: str
    trace_id: str
    variant: str | None
    budget_cents: int | None = None
    deadline: datetime | None = None


@dataclass(frozen=True)
class HandlerResult:
    """What a handler returns. The spine persists these fields without
    interpreting them (plan §6.3). llm_calls is empty for the demo handler."""

    output: dict[str, Any]
    llm_calls: list[dict[str, Any]] = field(default_factory=list)


class RetryableError(Exception):
    """Transient failure (plan §4): HTTP 429/5xx, connection reset, timeout,
    provider overloaded, lease_expired. Retried with backoff until
    RetryPolicy.max_attempts, then dead-lettered."""


class TerminalError(Exception):
    """Non-transient failure (plan §4): unknown handler, schema validation
    failure, HTTP 4xx (non-429), budget exceeded, permanent auth failure.
    Dead-letters immediately; attempts untouched."""


@dataclass(frozen=True)
class RetryPolicy:
    """Full-jitter exponential backoff (plan §4). At defaults: ~0-1s, 0-2s,
    0-4s, 0-8s, then dead letter on the 5th failure."""

    max_attempts: int = 5
    base: float = 1.0
    factor: float = 2.0
    cap: float = 60.0

    def delay_seconds(self, attempt_number: int) -> float:
        return random.uniform(0, min(self.cap, self.base * self.factor ** (attempt_number - 1)))


# ponytail: one policy for every Trigger. `Trigger` (plan §1: (source, type) ->
# handler + retry policy) isn't a real module yet — handlers.py is just a
# registry, no policy table. Per-Trigger override is plan §9's explicit
# non-goal anyway ("no retry policy per-job override at runtime"), so a
# single default earns its keep until a Trigger dict exists to key off of.
DEFAULT_RETRY_POLICY = RetryPolicy()
