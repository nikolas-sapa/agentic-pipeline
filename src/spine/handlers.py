"""The handler registry: register(source, type, fn) / resolve(event). One
demo handler ships here; capability 3 replaces its body (plan §2)."""

from __future__ import annotations

from typing import Awaitable, Callable

from spine.model import Event, HandlerResult, JobContext, TerminalError

Handler = Callable[[JobContext], Awaitable[HandlerResult]]

_registry: dict[tuple[str, str], Handler] = {}


def register(source: str, type: str, fn: Handler) -> None:
    _registry[(source, type)] = fn


def resolve(event: Event) -> Handler:
    try:
        return _registry[(event.source, event.type)]
    except KeyError as error:
        raise TerminalError("unknown_handler") from error


async def echo_handler(ctx: JobContext) -> HandlerResult:
    """Demo handler: deterministic, offline, echoes the event payload back.
    Ships no llm_calls rows — nothing here calls a model."""
    return HandlerResult(output={"echoed": ctx.event.payload})


register("demo", "echo", echo_handler)
