"""Alert sinks + dedup wrapper."""

from __future__ import annotations

import sys
import time
from typing import Protocol

from monad_ops.rules.events import AlertEvent


class AlertSink(Protocol):
    async def deliver(self, event: AlertEvent) -> None: ...


class StdoutSink:
    async def deliver(self, event: AlertEvent) -> None:
        print(
            f"[{event.severity.value.upper()}] {event.rule}: {event.title} — {event.detail}",
            file=sys.stderr,
            flush=True,
        )


class DedupingSink:
    """Wraps another sink and suppresses identical alert keys within a cooldown.

    RECOVERED events bypass the cooldown: if we were alerting, the user
    wants to know immediately that things are back.
    """

    def __init__(self, inner: AlertSink, cooldown_sec: int) -> None:
        self._inner = inner
        self._cooldown = cooldown_sec
        self._last_fired: dict[str, float] = {}

    async def deliver(self, event: AlertEvent) -> None:
        now = time.monotonic()
        last = self._last_fired.get(event.key, 0.0)
        if event.severity.value != "recovered" and now - last < self._cooldown:
            return
        self._last_fired[event.key] = now
        await self._inner.deliver(event)
