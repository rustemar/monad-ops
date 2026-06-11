"""WAL-persist-thread death detector (v0.14.5 ``waltrace`` bug).

monad-bft v0.14.5 moved WAL persistence onto a dedicated "waltrace"
thread. That thread can die silently — observed on this node
2026-06-11 (and by three other operators in ``#fullnode-discussion``):
the previous chunk fills to exactly 1 GiB, a fresh chunk is created in
the same second and never written again. From that point WAL
persistence / crash-recovery is OFF, and the only symptom is monad-bft
spamming this ERROR at ~220-250/sec:

    {"level":"ERROR","fields":{"message":"waltrace thread stopped"},
     "target":"monad_node"}

Live consensus is unaffected (WAL is not on the hot path), which is
exactly why this needs a rule: the node looks healthy on every other
metric while crash-recovery is silently disabled and the journal
grows by gigabytes. ``systemctl restart monad-bft`` clears it
instantly, but the bug recurs — other operators report ~16-25 h
between recurrences.

Severity ladder:

  * WARN at first detection (``warn_count`` lines in ``window_sec``).
    Baseline is strictly zero, so any occurrence is the real thing;
    the count threshold only guards against a stray one-off line in
    a future release changing semantics.
  * CRITICAL if still armed ``critical_after_sec`` later — the
    operator hasn't restarted and WAL persistence has been off the
    whole time.
  * RECOVERED once the window fully drains (post-restart), one per
    envelope per the alerting convention.

No hysteresis factor here: the healthy rate is exactly zero and the
sick rate is ~250/sec, so there is no boundary to flap on — disarm
requires a fully empty window.

``on_event`` + ``on_tick`` pairing matches ``NetworkLayerSignalRule``:
the tick path both drains the window after a restart (no fresh events
arrive to trigger re-evaluation) and drives the WARN→CRITICAL
escalation clock.
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field

from monad_ops.parser import ConsensusEvent, ConsensusEventKind
from monad_ops.rules.events import AlertEvent, Severity


@dataclass(slots=True)
class WaltraceFloodRule:
    """Tracks rate of ``waltrace thread stopped`` lines in monad-bft."""

    window_sec: int = 60
    warn_count: int = 10
    critical_after_sec: int = 900

    _events: deque[float] = field(default_factory=deque)
    _state: Severity | None = None
    _armed_at: float | None = None
    # Wall-clock seconds of the first error line in the current
    # envelope — the closest journal-side estimate of the thread's
    # death time, surfaced in the alert and used by the evidence
    # capture to centre its journal window.
    _first_error_ts: float | None = None

    def on_event(
        self, event: ConsensusEvent, now_sec: float | None = None
    ) -> AlertEvent | None:
        """Called for every ConsensusEvent. Non-waltrace kinds are no-ops."""
        if event.kind is not ConsensusEventKind.WALTRACE_STOPPED:
            return None
        now = now_sec if now_sec is not None else time.time()
        ts_sec = (event.ts_ms / 1000.0) if event.ts_ms else now
        self._events.append(ts_sec)
        if self._first_error_ts is None:
            self._first_error_ts = ts_sec
        return self._evaluate(now)

    def on_tick(self, now_sec: float | None = None) -> AlertEvent | None:
        """Periodic re-evaluation: drains the window after a restart and
        advances the WARN→CRITICAL escalation clock."""
        return self._evaluate(now_sec if now_sec is not None else time.time())

    @property
    def first_error_ts(self) -> float | None:
        """Death-time estimate for the current envelope (None when clear)."""
        return self._first_error_ts

    def _evaluate(self, now_sec: float) -> AlertEvent | None:
        cutoff = now_sec - self.window_sec
        while self._events and self._events[0] < cutoff:
            self._events.popleft()
        count = len(self._events)

        prev = self._state

        if prev is None:
            if count >= self.warn_count:
                self._state = Severity.WARN
                self._armed_at = now_sec
                return self._make_event(Severity.WARN, count)
            return None

        # Armed (WARN or CRITICAL). Disarm only on a fully empty window.
        if count == 0:
            self._state = None
            self._armed_at = None
            self._first_error_ts = None
            return AlertEvent(
                rule="waltrace_flood",
                severity=Severity.RECOVERED,
                key="waltrace_flood",
                title="waltrace error flood stopped",
                detail=(
                    "No 'waltrace thread stopped' lines in the last "
                    f"{self.window_sec} s (was {prev.value}). WAL chunks "
                    "should be writing again — verify a fresh wal_* file "
                    "is growing."
                ),
            )

        if (
            prev is Severity.WARN
            and self._armed_at is not None
            and now_sec - self._armed_at >= self.critical_after_sec
        ):
            self._state = Severity.CRITICAL
            return self._make_event(Severity.CRITICAL, count)

        return None

    def _make_event(self, severity: Severity, count: int) -> AlertEvent:
        if severity is Severity.CRITICAL:
            opener = (
                "Still flooding "
                f"{self.critical_after_sec // 60} min after detection — "
                "WAL persistence has been off the whole time."
            )
        else:
            opener = "WAL persist thread died (known v0.14.5 bug)."
        death = ""
        if self._first_error_ts is not None:
            death = f" First error at unix {int(self._first_error_ts)}."
        return AlertEvent(
            rule="waltrace_flood",
            severity=severity,
            key=f"waltrace_flood:{severity.value}",
            title=f"waltrace thread dead, WAL persistence off ({severity.value.upper()})",
            detail=(
                f"{opener} {count} 'waltrace thread stopped' line(s) in "
                f"the last {self.window_sec} s; floods run ~250/sec and "
                "the journal grows until restart. Node stays at tip — "
                "consensus is unaffected — but crash-recovery WAL is not "
                f"being written.{death} Fix: systemctl restart monad-bft "
                "(new chunk writes immediately; recurs ~16-25 h per "
                "operator reports). Evidence snapshot is captured "
                "automatically — grab it before the hourly monad-cruft "
                "cleanup deletes the 0-byte chunk (5 h retention)."
            ),
        )
