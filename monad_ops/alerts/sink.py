"""Alert sinks + dedup wrapper."""

from __future__ import annotations

import asyncio
import sys
import time
from collections.abc import Awaitable, Callable
from typing import Protocol

from monad_ops.rules.events import AlertEvent, Severity

# Meta-table keys shared by the CLI (writer), the gate and /api/state (readers).
MAINTENANCE_UNTIL_KEY = "maintenance_until"
MAINTENANCE_SINCE_KEY = "maintenance_since"


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


def parse_maintenance_ts(raw: str | None) -> float | None:
    """Epoch seconds from a stored meta value, or None when unset/invalid."""
    try:
        ts = float(raw) if raw else 0.0
    except (TypeError, ValueError):
        return None
    return ts if ts > 0 else None


class MaintenanceStore(Protocol):
    """What the gate needs from persistence; ``Storage`` implements it."""

    def maintenance_window(self) -> tuple[float | None, float | None]: ...
    def alerts_between(
        self, since_sec: float, until_sec: float
    ) -> list[tuple[float, str, str, str]]: ...
    def close_maintenance(self) -> None: ...
    def recovering_rules(self) -> set[str]: ...


class MaintenanceGate:
    """Holds delivery while a maintenance window is open, then sends one summary.

    Planned node work (an upgrade, a restart) fires stall, service and lag
    alerts that are all expected and all self-closing; the operator asked for
    one line, not the stream. Events are still recorded upstream — the
    ``_RecordingSink`` sits outside this gate — so the dashboard and the
    history keep every row.

    The window (``since``/``until``) lives in the database and the summary is
    built from the rows recorded there, so a restart inside the window loses
    nothing; closing clears ``since`` before the summary goes out, so it is
    sent at most once. A RECOVERED for a rule this process delivered live is
    let through even inside the window, so a red message the operator already
    has never stays open because of maintenance.
    """

    def __init__(
        self,
        inner: AlertSink,
        store: MaintenanceStore,
        *,
        record: Callable[[AlertEvent], Awaitable[None]] | None = None,
        now: Callable[[], float] = time.time,
        cache_sec: float = 2.0,
    ) -> None:
        self._inner = inner
        self._store = store
        self._record = record
        self._now = now
        self._cache_sec = cache_sec
        self._cached: tuple[float, tuple[float | None, float | None]] | None = None
        self._live: set[str] = set()  # rules delivered WARN/CRITICAL by this process
        self._flushing = asyncio.Lock()

    def _read_window(self) -> tuple[float | None, float | None]:
        try:
            return self._store.maintenance_window()
        except Exception as e:  # noqa: BLE001 — a meta read must never take the alert path down
            print(f"[maintenance] window read failed: {e}", file=sys.stderr, flush=True)
            return self._cached[1] if self._cached is not None else (None, None)

    async def _window(self) -> tuple[float | None, float | None]:
        """The stored window, re-read off the loop at most every ``cache_sec``."""
        now = self._now()
        if self._cached is None or now - self._cached[0] >= self._cache_sec:
            self._cached = (now, await asyncio.to_thread(self._read_window))
        return self._cached[1]

    def window_end(self) -> float | None:
        """End of the open window (epoch seconds) from the last read, else None."""
        if self._cached is None:
            self._cached = (self._now(), self._read_window())
        _since, until = self._cached[1]
        return until if until is not None and self._now() < until else None

    async def deliver(self, event: AlertEvent) -> None:
        _since, until = await self._window()
        if until is not None and self._now() < until:
            # Envelope rules key WARN/CRITICAL as "rule:severity" and RECOVERED
            # as the bare rule, so the match is on the rule, not the key.
            if event.severity is Severity.RECOVERED and event.rule in self._live:
                self._live.discard(event.rule)
                await self._inner.deliver(event)
            return
        await self.flush()
        if event.severity is Severity.RECOVERED:
            self._live.discard(event.rule)
        elif event.severity in (Severity.WARN, Severity.CRITICAL):
            self._live.add(event.rule)
        await self._inner.deliver(event)

    async def flush(self) -> None:
        """Summarise a window that has closed. Safe to call on a timer."""
        async with self._flushing:
            self._cached = None
            since, until = await self._window()
            now = self._now()
            if since is None or until is None or now < until:
                return
            # Up to now, not up to `until`: an event that arrived after `--off`
            # but before this flush was held on the cached window.
            rows = await asyncio.to_thread(self._store.alerts_between, since, now)
            recovering = await asyncio.to_thread(self._store.recovering_rules)
            await asyncio.to_thread(self._store.close_maintenance)
            self._cached = None
            # "Still open" = the rule's last event in the window is WARN/CRITICAL
            # and the rule is envelope-shaped (it has closed with RECOVERED
            # before). process_restart, reorg and friends never recover, so a
            # routine upgrade must not end on a red line because of them.
            last: dict[str, str] = {}
            for _ts, rule, sev, _key in rows:
                last[rule] = sev
            still_open = sorted(
                (rule, sev) for rule, sev in last.items()
                if sev in ("warn", "critical") and rule in recovering
            )
            summary = self._summary(rows, still_open, since, until)
        if self._record is not None:
            await self._record(summary)
        await self._inner.deliver(summary)

    @staticmethod
    def _summary(
        rows: list[tuple[float, str, str, str]],
        still_open: list[tuple[str, str]],
        since: float,
        until: float,
    ) -> AlertEvent:
        span = (
            f"{time.strftime('%H:%M', time.gmtime(since))}–"
            f"{time.strftime('%H:%M', time.gmtime(until))} UTC"
        )
        key = f"maintenance:closed:{int(until)}"
        if not rows:
            return AlertEvent(
                rule="maintenance",
                severity=Severity.INFO,
                key=key,
                title="Maintenance window closed",
                detail=f"Nothing was held ({span}); delivery is live again.",
            )
        counts: dict[str, int] = {}
        for _ts, rule, _sev, _key in rows:
            counts[rule] = counts.get(rule, 0) + 1
        parts = ", ".join(f"{rule} ×{n}" for rule, n in sorted(counts.items()))
        detail = f"Held {len(rows)} alert(s) during the window ({span}): {parts}."
        severity = Severity.INFO
        if still_open:
            detail += " Still open: " + ", ".join(
                f"{rule} {sev.upper()}" for rule, sev in still_open
            ) + "."
            severity = (
                Severity.CRITICAL
                if any(sev == "critical" for _r, sev in still_open)
                else Severity.WARN
            )
        detail += " The alert history has every one of them."
        return AlertEvent(
            rule="maintenance",
            severity=severity,
            key=key,
            title="Maintenance window closed",
            detail=detail,
        )
