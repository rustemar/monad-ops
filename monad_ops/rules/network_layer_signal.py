"""Network-layer signal rate rule.

Monad-specific predictive signal that complements ``ReorgRule`` /
``StallRule`` / ``ReferenceLagRule``. Surveys three classes of
``monad-bft`` log lines that historically co-occur with chain-
disagreement events but rarely fire at steady state:

  * ``failed to decrypt message`` — RaptorCast UDP auth couldn't
    decrypt an inbound packet (target ``monad_raptorcast::auth::socket``).
  * ``session timeout expired`` — wireauth peer session lost
    (``monad_wireauth::session::transport``).
  * ``Timestamp validation failed`` — proposal arrived with a
    timestamp outside the local tolerance (``monad_consensus_state``).

Verified pattern (2026-05-03 investigation): every May 2-3 reorg
captured ~4–6 decrypt-fails + 2–4 session-timeouts + 1–2 timestamp-
validation events in a tight window. April 26-30 background reorgs
had ZERO of any class. The signature flipped between Apr-30 and May-2
without an explanation we can fully attribute. The cluster pattern
itself is the signal worth alerting on; per-class breakdown is shown
in the alert detail for triage.

Why this is monad-ops territory rather than infra-monitoring: these
log lines come from Monad-specific subsystems (RaptorCast,
wireauth-session, consensus_state) and require parsing the
monad-bft journal. A generic Prometheus + node_exporter stack
won't surface them; we already tail this journal for consensus
events, so the marginal cost is one rule + three substring filters.

Calibration on this node (2026-05-03):
  * Baseline (last calm hour): 1 decrypt + 0 session + 3 timestamp =
    ~0.07 events/min combined.
  * Burst (today's morning reorg storm, 5h): ~2000 events combined,
    ~6.7 events/min — about 100× baseline.

Default thresholds give clear separation:

  * ``warn_count_5min`` = 5  combined events in any 5-min window
                              (= 60/h rate, well above any observed
                              steady-state hour)
  * ``critical_count_5min`` = 15 combined events in 5 min
                              (= 180/h, deep into the 2026-05-03
                              burst territory)

State machine matches ``RetrySpikeRule`` / ``BlockProcessingSlowdownRule``:

  CLEAR    -> WARN        : emit WARN
  CLEAR    -> CRITICAL    : emit CRITICAL  (skip intermediate WARN)
  WARN     -> CRITICAL    : emit CRITICAL
  CRITICAL -> WARN        : silent (still in alarm overall)
  WARN     -> CLEAR       : emit RECOVERED
  CRITICAL -> CLEAR       : emit RECOVERED

Hysteresis: arming uses raw thresholds; disarming requires the count
to drop ``HYSTERESIS_FACTOR`` below the arm threshold. Without this,
a count grazing the boundary produces flap pairs.

The rule supports both ``on_event`` (called on each network-layer
ConsensusEvent) and ``on_tick`` (called periodically) so the count
can decay back below thresholds even when no fresh events arrive —
otherwise an isolated burst would keep the rule armed for the full
window length.
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field

from monad_ops.parser import ConsensusEvent, ConsensusEventKind
from monad_ops.rules.events import AlertEvent, Severity


# Disarm threshold = arm × (1 - HYSTERESIS_FACTOR). Wide enough to
# kill boundary flap; narrow enough that real recovery is visible
# within roughly one window.
HYSTERESIS_FACTOR = 0.20


# The three event classes this rule tracks. Anything else passed to
# ``on_event`` is silently ignored — keeps wiring simple (the consensus
# loop can hand us every event without filtering).
_TRACKED_KINDS: frozenset[ConsensusEventKind] = frozenset({
    ConsensusEventKind.NETWORK_DECRYPT_FAIL,
    ConsensusEventKind.NETWORK_SESSION_TIMEOUT,
    ConsensusEventKind.NETWORK_TIMESTAMP_INVALID,
})


@dataclass(slots=True)
class NetworkLayerSignalRule:
    """Tracks rate of monad-bft network-layer error events."""

    window_sec: int
    warn_count: int
    critical_count: int

    # (ts_sec, kind) tuples — ts_sec is unix-epoch seconds, parsed
    # from the monad-bft journal line. We sort by event time, NOT
    # arrival time, so a delayed-tail batch doesn't artificially
    # inflate the present-window rate.
    _events: deque[tuple[float, ConsensusEventKind]] = field(default_factory=deque)
    _state: Severity | None = None

    def on_event(
        self, event: ConsensusEvent, now_sec: float | None = None
    ) -> AlertEvent | None:
        """Called for every ConsensusEvent. Non-network kinds are no-ops."""
        if event.kind not in _TRACKED_KINDS:
            return None
        # Use the event's own timestamp if known, otherwise wall clock —
        # the parser falls back to ts_ms=0 on schema drift, which would
        # bucket every line into 1970 and break the window. Detect that
        # and substitute now.
        ts_sec = (event.ts_ms / 1000.0) if event.ts_ms else (now_sec or time.time())
        self._events.append((ts_sec, event.kind))
        return self._evaluate(now_sec if now_sec is not None else time.time())

    def on_tick(self, now_sec: float | None = None) -> AlertEvent | None:
        """Called periodically so the rule de-arms when the window
        empties even with no fresh events."""
        return self._evaluate(now_sec if now_sec is not None else time.time())

    def _evaluate(self, now_sec: float) -> AlertEvent | None:
        cutoff = now_sec - self.window_sec
        while self._events and self._events[0][0] < cutoff:
            self._events.popleft()

        count = len(self._events)

        # Arm thresholds going up; disarm thresholds going down.
        if self._state == Severity.CRITICAL:
            warn_t = self.warn_count * (1 - HYSTERESIS_FACTOR)
            crit_t = self.critical_count * (1 - HYSTERESIS_FACTOR)
        elif self._state == Severity.WARN:
            warn_t = self.warn_count * (1 - HYSTERESIS_FACTOR)
            crit_t = self.critical_count
        else:  # CLEAR
            warn_t = self.warn_count
            crit_t = self.critical_count

        if count >= crit_t:
            target: Severity | None = Severity.CRITICAL
        elif count >= warn_t:
            target = Severity.WARN
        else:
            target = None

        prev = self._state
        self._state = target

        if prev == target:
            return None

        # Escalation paths.
        if target == Severity.CRITICAL and prev != Severity.CRITICAL:
            return self._make_event(Severity.CRITICAL, count)
        if target == Severity.WARN and prev not in (Severity.WARN, Severity.CRITICAL):
            return self._make_event(Severity.WARN, count)

        # De-escalation to CLEAR.
        if target is None and prev in (Severity.WARN, Severity.CRITICAL):
            return AlertEvent(
                rule="network_layer_signal",
                severity=Severity.RECOVERED,
                key="network_layer_signal",
                title="Network-layer signal rate normalized",
                detail=(
                    f"{count} network-layer event(s) in the last "
                    f"{self.window_sec // 60} min "
                    f"(was {prev.value})."
                ),
            )

        # CRITICAL -> WARN stays silent (still in alarm).
        return None

    def _make_event(self, severity: Severity, count: int) -> AlertEvent:
        threshold = (
            self.critical_count if severity == Severity.CRITICAL else self.warn_count
        )
        per_class = self._per_class_breakdown()
        return AlertEvent(
            rule="network_layer_signal",
            severity=severity,
            key=f"network_layer_signal:{severity.value}",
            title=f"Network-layer signal rate {severity.value.upper()}",
            detail=(
                f"{count} monad-bft network-layer event(s) in the last "
                f"{self.window_sec // 60} min (threshold {threshold}). "
                f"{per_class} "
                "Predictive: peer-stack stress (RaptorCast auth + "
                "wireauth session + consensus-state timestamp). "
                "Co-occurs with chain-disagreement clusters; investigate "
                "peer connectivity if sustained."
            ),
        )

    def _per_class_breakdown(self) -> str:
        decrypt = sum(1 for _, k in self._events if k == ConsensusEventKind.NETWORK_DECRYPT_FAIL)
        session = sum(1 for _, k in self._events if k == ConsensusEventKind.NETWORK_SESSION_TIMEOUT)
        ts_inv = sum(1 for _, k in self._events if k == ConsensusEventKind.NETWORK_TIMESTAMP_INVALID)
        return (
            f"By class: decrypt-fail={decrypt}, "
            f"session-timeout={session}, "
            f"timestamp-invalid={ts_inv}."
        )
