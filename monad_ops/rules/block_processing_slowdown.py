"""Block-processing slowdown detector.

Predictive companion to ``StallRule``. Where ``StallRule`` fires when
the node has *already* failed to produce a block in N seconds, this
rule fires while the node is still keeping up but the per-block
processing time is trending into a danger zone — a precursor that
gives operators time to react before cadence actually drops.

Signal: rolling median of ``total_us`` (microseconds spent processing
each block: state-reset + tx-exec + commit). Verified against the
2026-04-20 stress-test data:

  Quiet baseline (Apr-20 06:00):     median 1.2 ms,   p99 10.5 ms
  Mid-stress (Apr-20 12:00):         median 24.6 ms,  p99 161.2 ms
  Peak stress (Apr-20 18:00):        median 81.3 ms,  p99 112.5 ms
  Post-stress recovery (Apr-26):     median 1.9 ms,   p99 11.2 ms

Median ratio under load: ~20×. p99 saturates earlier (~10×). Median
is the steadier signal — picks up sustained pressure without spiking
on individual outlier blocks.

Inter-block production target: ~400 ms (testnet ~2.5 blk/s). When per-
block processing approaches a meaningful fraction of that budget, the
node is at risk of falling behind even if it hasn't yet missed a block.

State machine matches ``RetrySpikeRule``:

  CLEAR    -> WARN        : emit WARN
  CLEAR    -> CRITICAL    : emit CRITICAL  (skip intermediate WARN)
  WARN     -> CRITICAL    : emit CRITICAL
  CRITICAL -> WARN        : silent (still in alarm overall)
  WARN     -> CLEAR       : emit RECOVERED
  CRITICAL -> CLEAR       : emit RECOVERED

Hysteresis: arming uses raw thresholds; disarming requires the median
to drop ``HYSTERESIS_FACTOR`` below the arm threshold. Without this,
a median hovering at the boundary produces flap pairs (the RECOVERED
event bypasses dedup cooldown by design, so the channel fills with
green messages).

Why median, not mean: a single 200 ms block (e.g. dense MEV bundle)
shouldn't move the alarm. Median over a 120-block window is robust to
outliers and still reacts within ~50 s.

Why not p99: p99 over a 120-sample window IS the 119th-largest sample,
making it as noisy as a single outlier. Median was empirically steadier
on the stress-test data.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field

from monad_ops.parser import ExecBlock
from monad_ops.rules.events import AlertEvent, Severity


# Disarm threshold is `arm_threshold * (1 - HYSTERESIS_FACTOR)`. 0.20
# means the median must drop to 80% of the arm threshold before
# RECOVERED fires. Wide enough to kill boundary flap, narrow enough that
# real recovery shows up within roughly one window.
HYSTERESIS_FACTOR = 0.20


@dataclass(slots=True)
class BlockProcessingSlowdownRule:
    """Tracks rolling median of per-block ``total_us``.

    Default thresholds calibrated against testnet observations:

      ``warn_us`` = 10_000  (10 ms — ~5× quiet baseline, catches
                              load shifting upward early)
      ``critical_us`` = 50_000  (50 ms — ~25× baseline, well into
                                  stress-test territory but well below
                                  the 400 ms inter-block budget; gives
                                  operators headroom before stall)
      ``window`` = 120  (~48 s at 2.5 blk/s — long enough to wash out
                         single outlier blocks, short enough to react
                         within a minute)
    """

    window: int
    warn_us: int
    critical_us: int

    _samples: deque[int] = field(default_factory=deque)
    _state: Severity | None = None  # None means CLEAR
    # Block number of the last sample, kept so the alert detail can
    # cite the most recent block when it fires. Saves the rule from
    # having to remember the whole ExecBlock.
    _last_block: int | None = None

    def on_block(self, block: ExecBlock) -> AlertEvent | None:
        self._samples.append(block.total_us)
        while len(self._samples) > self.window:
            self._samples.popleft()
        self._last_block = block.block_number

        # Require a full window before firing; prevents one anomalous
        # block at startup from arming the alarm. Same motivation as
        # RetrySpikeRule.
        if len(self._samples) < self.window:
            return None

        median = _median(self._samples)

        # Effective thresholds depend on the current state — hysteresis
        # band. Once armed, stay armed until median drops meaningfully
        # below the arm threshold, not just grazes it.
        if self._state == Severity.CRITICAL:
            warn_t = self.warn_us * (1 - HYSTERESIS_FACTOR)
            crit_t = self.critical_us * (1 - HYSTERESIS_FACTOR)
        elif self._state == Severity.WARN:
            warn_t = self.warn_us * (1 - HYSTERESIS_FACTOR)
            crit_t = self.critical_us
        else:  # CLEAR
            warn_t = self.warn_us
            crit_t = self.critical_us

        if median >= crit_t:
            target: Severity | None = Severity.CRITICAL
        elif median >= warn_t:
            target = Severity.WARN
        else:
            target = None  # CLEAR

        prev = self._state
        self._state = target

        if prev == target:
            return None

        # Escalation paths.
        if target == Severity.CRITICAL and prev != Severity.CRITICAL:
            return self._make_event(Severity.CRITICAL, median)
        if target == Severity.WARN and prev not in (Severity.WARN, Severity.CRITICAL):
            return self._make_event(Severity.WARN, median)

        # De-escalation to CLEAR.
        if target is None and prev in (Severity.WARN, Severity.CRITICAL):
            return AlertEvent(
                rule="block_processing_slowdown",
                severity=Severity.RECOVERED,
                key="block_processing_slowdown",
                title="Block processing time normalized",
                detail=(
                    f"Median total_us dropped to {median/1000:.1f} ms "
                    f"over the last {self.window} blocks "
                    f"(was {prev.value})."
                ),
            )

        # CRITICAL -> WARN stays silent (still in alarm).
        return None

    def _make_event(self, severity: Severity, median: float) -> AlertEvent:
        threshold = self.critical_us if severity == Severity.CRITICAL else self.warn_us
        return AlertEvent(
            rule="block_processing_slowdown",
            severity=severity,
            key=f"block_processing_slowdown:{severity.value}",
            title=f"Block processing slowdown {severity.value.upper()}",
            detail=(
                f"Median total_us = {median/1000:.1f} ms over last "
                f"{self.window} blocks (threshold {threshold/1000:.1f} ms). "
                f"Last block: #{self._last_block}. "
                "Predictive: node is still keeping up, but per-block "
                "processing time has shifted into stress territory — "
                "watch cadence."
            ),
        )


def _median(samples: deque[int]) -> float:
    """Median of a deque, computed on a sorted snapshot. O(N log N)
    per call; with window=120 and ~2.5 blk/s ingest rate, this is
    negligible cost (~1.7K ops/block * 2.5 blk/s = ~4K ops/s)."""
    s = sorted(samples)
    n = len(s)
    if n % 2 == 1:
        return float(s[n // 2])
    return (s[n // 2 - 1] + s[n // 2]) / 2.0
