"""Retry-percentage spike detector.

Maintains a sliding window of recent blocks and fires when the
average ``retry_pct`` over the window crosses configured thresholds.

State machine (intentionally simple):
  CLEAR -> WARN        : emit WARN
  CLEAR -> CRITICAL    : emit CRITICAL (skip intermediate WARN)
  WARN  -> CRITICAL    : emit CRITICAL
  CRITICAL -> WARN     : silent  (we're still alerting overall)
  WARN  -> CLEAR       : emit RECOVERED
  CRITICAL -> CLEAR    : emit RECOVERED

**Hysteresis.** Arm thresholds (going up) use ``warn_pct`` / ``critical_pct``
directly. Disarm thresholds (going down) are lower by ``HYSTERESIS_PCT``.
Without this, a ``retry_pct`` average that hovers right around an arm
threshold produces a flood of WARN→RECOVERED pairs — the RECOVERED events
bypass the dedup cooldown (by design, so operators see recovery immediately),
so the visible channel fills with green "normalized" messages. The gap
prevents micro-oscillation from firing any events at all.

Why averaged over a window rather than per-block: a single 100% block
is often just a dense-contention micro-event (MEV bundle, NFT mint,
oracle update). A 60-block window catches genuinely sustained
contention that matters for operators and dApp teams.

**Quiet-period gate.** When the network is essentially idle, blocks
carry only a handful of transactions. One retried tx out of three is
33% retry_pct; two of four is 50%. Across a 60-block window those ratios
keep the average bouncing over the arm threshold, producing hundreds of
WARN/RECOVERED pairs per day with no actionable signal. ``min_window_tx_avg``
suppresses arming while the window's *average* tx_count per block is
below that floor. The gate applies to both WARN and CRITICAL — CRITICAL
at 77% retry on 9-tx blocks is still small-sample noise, not a real
contention event. The gate deliberately does NOT block disarming: if the
rule is already armed and the network quiets down, RECOVERED still fires,
so operators see the all-clear transition promptly.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field

from monad_ops.parser import ExecBlock
from monad_ops.rules.events import AlertEvent, Severity


# Gap between arm and disarm thresholds. 5 percentage points is wide enough
# to kill noise from avg hovering at the boundary while still letting a
# genuine recovery be visible within one window.
HYSTERESIS_PCT = 5.0


@dataclass(slots=True)
class RetrySpikeRule:
    window: int
    warn_pct: float
    critical_pct: float
    min_window_tx_avg: float = 0.0  # 0 disables the quiet-period gate

    _samples: deque[float] = field(default_factory=deque)
    _tx_counts: deque[int] = field(default_factory=deque)
    _state: Severity | None = None  # None means CLEAR

    def on_block(self, block: ExecBlock) -> AlertEvent | None:
        self._samples.append(block.retry_pct)
        self._tx_counts.append(block.tx_count)
        while len(self._samples) > self.window:
            self._samples.popleft()
        while len(self._tx_counts) > self.window:
            self._tx_counts.popleft()

        # Require a full window before firing; prevents a single
        # high block from tripping the alarm at startup.
        if len(self._samples) < self.window:
            return None

        avg = sum(self._samples) / len(self._samples)
        tx_avg = sum(self._tx_counts) / len(self._tx_counts)

        # Quiet-period gate: when blocks are tiny, retry_pct is
        # small-sample noise. Suppress *arming* (CLEAR → WARN/CRITICAL
        # and WARN → CRITICAL). Disarming is NOT gated — if we're
        # already armed and the network quiets down, we still emit
        # RECOVERED so operators see the all-clear.
        arming_suppressed = tx_avg < self.min_window_tx_avg

        # Effective thresholds depend on the current state. This is the
        # hysteresis band: once armed, stay armed until avg drops meaningfully
        # below the arm threshold — not just grazes it.
        if self._state == Severity.CRITICAL:
            warn_t = self.warn_pct - HYSTERESIS_PCT
            crit_t = self.critical_pct - HYSTERESIS_PCT
        elif self._state == Severity.WARN:
            warn_t = self.warn_pct - HYSTERESIS_PCT
            crit_t = self.critical_pct
        else:  # CLEAR
            warn_t = self.warn_pct
            crit_t = self.critical_pct

        if avg >= crit_t:
            target: Severity | None = Severity.CRITICAL
        elif avg >= warn_t:
            target = Severity.WARN
        else:
            target = None  # CLEAR

        prev = self._state

        # Block escalations while the quiet-period gate is engaged.
        # Escalations = any transition that raises severity (CLEAR→WARN,
        # CLEAR→CRITICAL, WARN→CRITICAL). De-escalations and CLEAR
        # transitions fall through unaffected.
        if arming_suppressed:
            escalating = (
                (prev is None and target is not None)
                or (prev == Severity.WARN and target == Severity.CRITICAL)
            )
            if escalating:
                # State stays where it was — no event emitted.
                return None

        self._state = target

        if prev == target:
            return None

        # Escalation.
        if target == Severity.CRITICAL and prev != Severity.CRITICAL:
            return self._make_event(Severity.CRITICAL, avg, block)
        if target == Severity.WARN and prev not in (Severity.WARN, Severity.CRITICAL):
            return self._make_event(Severity.WARN, avg, block)

        # De-escalation to CLEAR.
        if target is None and prev in (Severity.WARN, Severity.CRITICAL):
            return AlertEvent(
                rule="retry_spike",
                severity=Severity.RECOVERED,
                key="retry_spike",
                title="Retry rate normalized",
                detail=(
                    f"Average retry_pct dropped to {avg:.1f}% "
                    f"over the last {self.window} blocks "
                    f"(was {prev.value})."
                ),
            )

        # CRITICAL -> WARN is silent (still in alarm).
        return None

    def _make_event(self, severity: Severity, avg: float, block: ExecBlock) -> AlertEvent:
        threshold = self.critical_pct if severity == Severity.CRITICAL else self.warn_pct
        return AlertEvent(
            rule="retry_spike",
            severity=severity,
            key=f"retry_spike:{severity.value}",
            title=f"Re-execution rate {severity.value.upper()}",
            detail=(
                f"Average retry_pct = {avg:.1f}% over last {self.window} blocks "
                f"(threshold {threshold}%). "
                f"Last block #{block.block_number} had rtp={block.retry_pct}%, "
                f"{block.retried}/{block.tx_count} tx retried."
            ),
        )
