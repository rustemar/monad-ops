"""Shared handle the route modules receive instead of a closure.

``build_app`` used to define every endpoint as a nested function so each one
could capture ``state``, ``config`` and the read-through cache. That made the
endpoints unreadable in isolation and let the function grow without bound. The
routers take this object instead; it carries exactly what a handler is allowed
to reach for, and nothing else.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from monad_ops.config import Config
from monad_ops.labels import ContractLabels
from monad_ops.state import State


@dataclass(frozen=True)
class ApiContext:
    """What a route group needs from the app it is mounted on.

    ``cached`` is ``build_app``'s read-through cache with per-entry TTL and
    in-flight dedup: ``cached(bucket, ttl_sec, key, loader, ttl_for_value=None)``.
    It stays owned by the app because the store is per-app instance and the
    tests reach it through ``app.state.cache_store``.
    """

    state: State
    config: Config
    cached: Callable[..., Awaitable[Any]]
    labels: ContractLabels
    journal_capture_dir: Path | None = None
