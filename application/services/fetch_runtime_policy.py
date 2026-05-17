"""Runtime policy helpers for fetch task timing behavior."""

from __future__ import annotations

from application.services.runtime_service import env_float


def task_timeout_seconds() -> float | None:
    """Return optional per-task timeout in seconds from environment."""

    value = env_float("DEPTH_FETCH_TASK_TIMEOUT_S", 0.0)
    if value <= 0:
        return None
    return value


def heartbeat_seconds() -> float:
    """Return heartbeat interval in seconds for long-running fetch tasks."""

    value = env_float("DEPTH_FETCH_HEARTBEAT_S", 30.0)
    if value <= 0:
        return 30.0
    return value
