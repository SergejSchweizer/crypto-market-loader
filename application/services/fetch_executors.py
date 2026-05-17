"""Execution helpers for fetch-service task orchestrators."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from typing import TypeVar

TResult = TypeVar("TResult")


def elapsed_seconds(started_at: datetime) -> int:
    """Return elapsed integer seconds from ``started_at`` until now (UTC)."""

    return int((datetime.now(UTC) - started_at).total_seconds())


def run_with_optional_history_chunk(
    *,
    runner: Callable[..., TResult],
    fn: Callable[..., TResult],
    timeout_s: float | None,
    heartbeat_s: float,
    heartbeat: Callable[[int], None],
    use_process_timeout: bool,
    kwargs: dict[str, object],
) -> TResult:
    """Run one fetcher with optional `on_history_chunk` fallback compatibility."""

    try:
        return runner(
            fn,
            timeout_s=timeout_s,
            heartbeat_s=heartbeat_s,
            heartbeat=heartbeat,
            use_process_timeout=use_process_timeout,
            **kwargs,
        )
    except TypeError as exc:
        if "on_history_chunk" not in str(exc):
            raise
        retry_kwargs = dict(kwargs)
        retry_kwargs.pop("on_history_chunk", None)
        return runner(
            fn,
            timeout_s=timeout_s,
            heartbeat_s=heartbeat_s,
            heartbeat=heartbeat,
            use_process_timeout=use_process_timeout,
            **retry_kwargs,
        )
