"""Dataclasses, enums, and the idempotency key derivation.

ponytail: RetryPolicy / failure taxonomy (RetryableError, TerminalError) are
plan §4 — retry/backoff is out of scope for the tracer bullet (tickets 4-5),
so they aren't defined yet. Add them when `queue.fail()` lands.
"""

from __future__ import annotations

import hashlib
import json
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
