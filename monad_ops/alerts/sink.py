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


def envelope_id(key: str) -> str:
    """The envelope an alert key belongs to.

    Envelope rules key WARN/CRITICAL as ``rule:severity`` and the matching
    RECOVERED as the bare ``rule``; per-entity rules (``service_failure:<unit>``,
    ``enrichment_health:failing``) use one key for every severity. Stripping
    the severity suffix gives the identity both shapes share.
    """
    for suffix in (":warn", ":critical"):
        if key.endswith(suffix):
            return key[: -len(suffix)]
    return key


class MaintenanceStore(Protocol):
    """What the gate needs from persistence; ``Storage`` implements it."""

    def maintenance_window(self) -> tuple[float | None, float | None]: ...
    def take_closed_window(self, now_sec: float) -> tuple[float, float] | None: ...
    def alerts_between(
        self, since_sec: float, until_sec: float, *, before: float | None = None
    ) -> list[tuple[float, str, str, str]]: ...
    def recovered_envelopes(self) -> set[str]: ...
    def last_severity_before(self, envelope: str, ts_sec: float) -> str | None: ...


class MaintenanceGate:
    """Holds delivery while a maintenance window is open, then sends one summary.

    Planned node work (an upgrade, a restart) fires stall, service and lag
    alerts that are all expected and all self-closing; the operator asked for
    one line, not the stream. Events are still recorded upstream — the
    ``_RecordingSink`` sits outside this gate — so the dashboard and the
    history keep every row.

    The window (``since``/``until``) lives in the database and the summary is
    built from the rows recorded there, so a restart inside the window loses
    nothing; taking the closed window clears ``since`` in one transaction, so
    the summary goes out at most once. A RECOVERED for an envelope that was
    red before the window is let through, so a red message never stays open
    because of maintenance; one-shot rules (``version_watch``) pass regardless.
    """

    def __init__(
        self,
        inner: AlertSink,
        store: MaintenanceStore,
        *,
        record: Callable[[AlertEvent], Awaitable[None]] | None = None,
        passthrough_rules: frozenset[str] = frozenset({"version_watch"}),
        now: Callable[[], float] = time.time,
        cache_sec: float = 2.0,
    ) -> None:
        self._inner = inner
        self._store = store
        self._record = record
        self._passthrough = passthrough_rules
        self._now = now
        self._cache_sec = cache_sec
        self._cached: tuple[float, tuple[float | None, float | None]] | None = None
        self._last_good: tuple[float | None, float | None] = (None, None)
        self._live: set[str] = set()  # envelopes delivered WARN/CRITICAL by this process
        self._flushing = asyncio.Lock()

    def _read_window(self) -> tuple[float | None, float | None]:
        try:
            self._last_good = self._store.maintenance_window()
        except Exception as e:  # noqa: BLE001 — a meta read must never take the alert path down
            print(f"[maintenance] window read failed: {e}", file=sys.stderr, flush=True)
        return self._last_good

    async def _window(self, *, force: bool = False) -> tuple[float | None, float | None]:
        """The stored window, re-read off the loop at most every ``cache_sec``."""
        now = self._now()
        if force or self._cached is None or now - self._cached[0] >= self._cache_sec:
            self._cached = (now, await asyncio.to_thread(self._read_window))
        return self._cached[1]

    @staticmethod
    def _is_open(window: tuple[float | None, float | None], now: float, ts: float | None) -> bool:
        since, until = window
        if since is None or until is None or now >= until:
            return False
        # A row stamped before the window opened is not part of it.
        return ts is None or ts >= since

    def window_end(self) -> float | None:
        """End of the open window (epoch seconds) from the last read, else None."""
        if self._cached is None:
            self._cached = (self._now(), self._read_window())
        window = self._cached[1]
        return window[1] if self._is_open(window, self._now(), None) else None

    async def deliver(self, event: AlertEvent, ts: float | None = None) -> None:
        window = await self._window()
        envelope = envelope_id(event.key)
        if self._is_open(window, self._now(), ts):
            if event.rule in self._passthrough:
                await self._inner.deliver(event)
            elif event.severity is Severity.RECOVERED and await self._was_red(envelope, window[0]):
                self._live.discard(envelope)
                await self._inner.deliver(event)
            return
        await self.flush(before=ts)
        if event.severity is Severity.RECOVERED:
            self._live.discard(envelope)
        elif event.severity in (Severity.WARN, Severity.CRITICAL):
            self._live.add(envelope)
        await self._inner.deliver(event)

    async def _was_red(self, envelope: str, since: float | None) -> bool:
        """Was this envelope delivered WARN/CRITICAL before the window opened?
        Memory answers for this process; the history answers across restarts."""
        if envelope in self._live:
            return True
        if since is None:
            return False
        try:
            last = await asyncio.to_thread(self._store.last_severity_before, envelope, since)
        except Exception as e:  # noqa: BLE001
            print(f"[maintenance] history read failed: {e}", file=sys.stderr, flush=True)
            return False
        return last in ("warn", "critical")

    async def flush(self, before: float | None = None) -> None:
        """Summarise a window that has closed. Safe to call on a timer.

        ``before`` is the timestamp of the event whose delivery triggered the
        flush: its row is being delivered live, so it is not part of the summary.
        """
        async with self._flushing:
            since, until = await self._window(force=True)
            now = self._now()
            if since is None or until is None or now < until:
                return
            taken = await asyncio.to_thread(self._store.take_closed_window, now)
            self._cached = None
            if taken is None:
                return
            since, until = taken
            rows = await asyncio.to_thread(self._store.alerts_between, since, now, before=before)
            recovered = await asyncio.to_thread(self._store.recovered_envelopes)
            summary = self._summary(rows, recovered, since, until)
            await self._inner.deliver(summary)
            if self._record is not None:
                try:
                    await self._record(summary)
                except Exception as e:  # noqa: BLE001 — the channel line matters more than the row
                    print(f"[maintenance] summary not recorded: {e}", file=sys.stderr, flush=True)

    @staticmethod
    def _summary(
        rows: list[tuple[float, str, str, str]],
        recovered_envelopes: set[str],
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
                detail=f"Nothing was recorded ({span}); delivery is live again.",
            )
        counts: dict[str, int] = {}
        last: dict[str, str] = {}
        for _ts, rule, sev, row_key in rows:
            counts[rule] = counts.get(rule, 0) + 1
            last[envelope_id(row_key)] = sev
        # "Still open" = the envelope's last row in the window is WARN/CRITICAL
        # and it is envelope-shaped (it has closed with RECOVERED before), so
        # point events (process_restart, a crash that already restarted) never
        # turn a routine upgrade's summary red.
        still_open = sorted(
            (envelope, sev) for envelope, sev in last.items()
            if sev in ("warn", "critical") and envelope in recovered_envelopes
        )
        parts = ", ".join(f"{rule} ×{n}" for rule, n in sorted(counts.items()))
        detail = f"{len(rows)} alert(s) recorded during the window ({span}): {parts}."
        severity = Severity.INFO
        if still_open:
            detail += " Still open: " + ", ".join(
                f"{envelope} {sev.upper()}" for envelope, sev in still_open
            ) + "."
            severity = (
                Severity.CRITICAL
                if any(sev == "critical" for _e, sev in still_open)
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
