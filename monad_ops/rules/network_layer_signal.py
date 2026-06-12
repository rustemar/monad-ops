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

Deployed thresholds (third generation, 2026-06-12 audit): arm
``warn_count`` = 25 / ``critical_count`` = 50 per 5-min window —
p99.7 / p99.9 of the 30-day measured distribution. The original 5/15
and the 2026-05-04 bump to 10/15 both sat inside the background tail
(warn=10 ≈ p98) and produced 74% of all Telegram volume with a 4-min
median flap. Real incidents in that window ran 91–2582 events/5min,
so the audit raised arms an order of magnitude below incident scale
but clear of background.

Diversity gates suppress single-neighbour storms that produce volume
without node-wide signal (observed 2026-05-06 chronic chatter from one
flapping peer post-upgrade): WARN needs ≥``warn_min_unique_peers``
distinct peers, CRITICAL needs ≥``critical_min_unique_peers``. Below
the WARN floor the rule stays silent entirely; between the two it
holds at WARN with a gate-hint. Events from classes without peer info
(session-timeout, timestamp-invalid) don't block escalation.

State machine matches ``RetrySpikeRule`` / ``BlockProcessingSlowdownRule``:

  CLEAR    -> WARN        : emit WARN
  CLEAR    -> CRITICAL    : emit CRITICAL  (skip intermediate WARN)
  WARN     -> CRITICAL    : emit CRITICAL
  CRITICAL -> WARN        : silent (still in alarm overall)
  WARN     -> CLEAR       : emit RECOVERED (after recovery confirm)
  CRITICAL -> CLEAR       : emit RECOVERED (after recovery confirm)

Hysteresis: arming uses raw thresholds; disarming uses the explicit
``warn_disarm_count`` / ``critical_disarm_count`` levels when set
(deployed: 12/25 ≈ 0.5 × arm), else falls back to
``arm × (1 - HYSTERESIS_FACTOR)``. Without a gap, a count grazing the
boundary produces flap pairs.

Recovery confirmation (``recovery_confirm_sec``, deployed 600 s):
RECOVERED is not emitted on the first dip below disarm — the count
must stay below it for the whole confirmation window. RECOVERED
bypasses the dedup sink by design, so without this every dip-and-
rebound delivered an orphan green (87 RECOVERED vs 37 real episodes
in the 30-day audit). Same defense ``StallRule`` got in April.

Gate-fail hold: if the peer-diversity gate drops the target to None
while the measured count is still at/above the warn disarm level, the
rule HOLDS its armed state silently instead of emitting RECOVERED —
a single-peer storm fading in and out of the gate is not a recovery
(observed 2026-06-02: RECOVERED greens at counts 75–108).

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
    # Diversity gates. WARN entry needs at least ``warn_min_unique_peers``
    # distinct peers; CRITICAL escalation needs ``critical_min_unique_peers``.
    # Single-peer storms (one chronically desynced neighbour spamming
    # RaptorCast) are peer-pair issues, not node-wide stress, and were
    # producing constant Telegram chatter (2026-05-06) without any
    # predictive value. Below the WARN floor we stay silent entirely;
    # between WARN and CRITICAL we still emit WARN with a gate-hint.
    # Only counts peers we actually parsed; events from classes without
    # peer info (session-timeout, timestamp-invalid) contribute to the
    # count but not the diversity check, so a volume-only burst on those
    # classes still escalates. Set either to 1 to disable that gate.
    warn_min_unique_peers: int = 2
    critical_min_unique_peers: int = 3
    # Explicit disarm levels (hysteresis). None = legacy fallback to
    # arm × (1 - HYSTERESIS_FACTOR).
    warn_disarm_count: int | None = None
    critical_disarm_count: int | None = None
    # Seconds the count must stay below the warn disarm level before
    # RECOVERED is emitted. 0 = immediate (legacy behavior).
    recovery_confirm_sec: float = 0.0

    # (ts_sec, kind, peer) tuples — ts_sec is unix-epoch seconds,
    # parsed from the monad-bft journal line. We sort by event time,
    # NOT arrival time, so a delayed-tail batch doesn't artificially
    # inflate the present-window rate. peer is "ip:port" or None.
    _events: deque[tuple[float, ConsensusEventKind, str | None]] = field(default_factory=deque)
    _state: Severity | None = None
    # When the count first dipped below the warn disarm level while
    # armed; None when not in a pending-recovery stretch.
    _clear_since: float | None = None
    # True after we've already emitted a "held below CRITICAL" WARN
    # for the current arming cycle. Resets when the rule disarms to
    # CLEAR. Prevents a repeat WARN on every event past critical_count
    # while the gate keeps holding.
    _held_below_crit_emitted: bool = False

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
        self._events.append((ts_sec, event.kind, event.peer))
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

        warn_d = (
            self.warn_disarm_count
            if self.warn_disarm_count is not None
            else self.warn_count * (1 - HYSTERESIS_FACTOR)
        )
        crit_d = (
            self.critical_disarm_count
            if self.critical_disarm_count is not None
            else self.critical_count * (1 - HYSTERESIS_FACTOR)
        )

        # Arm thresholds going up; disarm thresholds going down.
        if self._state == Severity.CRITICAL:
            warn_t = warn_d
            crit_t = crit_d
        elif self._state == Severity.WARN:
            warn_t = warn_d
            crit_t = self.critical_count
        else:  # CLEAR
            warn_t = self.warn_count
            crit_t = self.critical_count

        # Diversity gate: count distinct peers seen on classes that
        # carry one (decrypt-fail today). Events without a peer don't
        # block escalation but don't contribute to diversity either,
        # so a session-timeout-only burst still escalates on volume.
        unique_peers = len({p for _, _, p in self._events if p is not None})
        peers_known = any(p is not None for _, _, p in self._events)

        if count >= crit_t and (
            not peers_known or unique_peers >= self.critical_min_unique_peers
        ):
            target: Severity | None = Severity.CRITICAL
        elif count >= warn_t and (
            not peers_known or unique_peers >= self.warn_min_unique_peers
        ):
            target = Severity.WARN
        else:
            target = None

        prev = self._state

        # De-escalation is gated twice before RECOVERED can fire.
        if target is None and prev in (Severity.WARN, Severity.CRITICAL):
            if count >= warn_d:
                # Only the diversity gate failed; volume is still at or
                # above disarm. A single-peer storm fading in and out of
                # the gate is not a recovery — hold armed, silently.
                self._clear_since = None
                self._state = prev
                return None
            if self._clear_since is None:
                self._clear_since = now_sec
            if now_sec - self._clear_since < self.recovery_confirm_sec:
                self._state = prev
                return None
            quiet_sec = now_sec - self._clear_since
            self._state = None
            self._clear_since = None
            self._held_below_crit_emitted = False
            return AlertEvent(
                rule="network_layer_signal",
                severity=Severity.RECOVERED,
                key="network_layer_signal",
                title="Network-layer signal rate normalized",
                detail=(
                    f"{count} network-layer event(s) in the last "
                    f"{self.window_sec // 60} min "
                    f"(was {prev.value}; quiet {int(quiet_sec)}s "
                    f"below disarm {warn_d:g})."
                ),
            )

        self._clear_since = None
        self._state = target

        # Reset the held-back marker any time we drop to CLEAR.
        if target is None:
            self._held_below_crit_emitted = False

        # Sub-state re-fire: state stays at WARN but count just crossed
        # critical_count while the diversity gate held. Worth one
        # extra WARN so the operator sees the elevated volume with the
        # gate-hint context — silence here would mislead.
        gate_holding = (
            target == Severity.WARN
            and count >= self.critical_count
            and not self._held_below_crit_emitted
        )
        if gate_holding:
            self._held_below_crit_emitted = True
            return self._make_event(Severity.WARN, count)

        if prev == target:
            return None

        # Escalation paths.
        if target == Severity.CRITICAL and prev != Severity.CRITICAL:
            return self._make_event(Severity.CRITICAL, count)
        if target == Severity.WARN and prev not in (Severity.WARN, Severity.CRITICAL):
            return self._make_event(Severity.WARN, count)

        # CRITICAL -> WARN stays silent (still in alarm).
        return None

    def _make_event(self, severity: Severity, count: int) -> AlertEvent:
        threshold = (
            self.critical_count if severity == Severity.CRITICAL else self.warn_count
        )
        per_class = self._per_class_breakdown()
        unique_peers = len({p for _, _, p in self._events if p is not None})
        # Tail hint when WARN is being held back by the peer-diversity
        # gate. Helps the operator distinguish a real network-wide
        # signal from a single-neighbour storm without leaving the
        # alert.
        gate_hint = ""
        if (
            severity == Severity.WARN
            and count >= self.critical_count
            and unique_peers < self.critical_min_unique_peers
        ):
            gate_hint = (
                f" Held below CRITICAL: only {unique_peers} unique "
                f"peer(s) (need {self.critical_min_unique_peers})."
            )
        return AlertEvent(
            rule="network_layer_signal",
            severity=severity,
            key=f"network_layer_signal:{severity.value}",
            title=f"Network-layer signal rate {severity.value.upper()}",
            detail=(
                f"{count} monad-bft network-layer event(s) in the last "
                f"{self.window_sec // 60} min (threshold {threshold}). "
                f"{per_class} Unique peers: {unique_peers}.{gate_hint} "
                "Predictive: peer-stack stress (RaptorCast auth + "
                "wireauth session + consensus-state timestamp). "
                "Co-occurs with chain-disagreement clusters; investigate "
                "peer connectivity if sustained."
            ),
        )

    def _per_class_breakdown(self) -> str:
        decrypt = sum(1 for _, k, _ in self._events if k == ConsensusEventKind.NETWORK_DECRYPT_FAIL)
        session = sum(1 for _, k, _ in self._events if k == ConsensusEventKind.NETWORK_SESSION_TIMEOUT)
        ts_inv = sum(1 for _, k, _ in self._events if k == ConsensusEventKind.NETWORK_TIMESTAMP_INVALID)
        return (
            f"By class: decrypt-fail={decrypt}, "
            f"session-timeout={session}, "
            f"timestamp-invalid={ts_inv}."
        )
