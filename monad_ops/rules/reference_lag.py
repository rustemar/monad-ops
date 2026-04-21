"""Reference-RPC lag rule.

Fires when the local node is sustained behind a public reference RPC —
the "is it me or is it the whole network" question during a stress test.

Semantics:
  * Positive delta = reference ahead of local = we are lagging.
  * Negative delta = local ahead of reference (our RPC samples fresher
    than the public endpoint) = normal, never alerts.

Confirmation window and hysteresis match ``RetrySpikeRule`` in spirit:
a single bad sample is not enough to arm (noise filter), and a single
good sample is not enough to disarm (avoids chattering at the boundary).
We summarize the window with ``min(delta)`` — the best delta across
recent samples has to be bad before we call it sustained lag.

Probe failures (``reference_error`` set) are soft-ignored: they don't
change state and don't update the window. A lag alert should not fire
because Cloudflare blipped for 5 seconds.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field

from monad_ops.rules.events import AlertEvent, Severity


# Hysteresis band (blocks). Once armed, the lag must drop this many
# blocks below the arm threshold before disarming. Block time on
# testnet is ~400 ms, so 5 blocks ≈ 2 s of headroom.
HYSTERESIS_BLOCKS = 5


@dataclass(slots=True)
class ReferenceLagRule:
    warn_blocks: int = 15
    critical_blocks: int = 60
    window: int = 2  # consecutive samples required to confirm/deconfirm

    _recent: deque[int] = field(default_factory=deque)
    _state: Severity | None = None  # None means CLEAR

    def on_sample(
        self,
        *,
        reference_block: int | None,
        local_block: int | None,
        reference_error: str | None,
    ) -> AlertEvent | None:
        # Soft failures — can't evaluate, leave state untouched.
        if reference_error or reference_block is None or local_block is None:
            return None

        delta = reference_block - local_block
        self._recent.append(delta)
        while len(self._recent) > self.window:
            self._recent.popleft()

        # Wait until we have enough samples. Same motivation as RetrySpike:
        # don't arm from a single reading at startup.
        if len(self._recent) < self.window:
            return None

        # Representative value: the smallest delta in the window. If even
        # the smallest exceeds threshold, the lag is sustained; if any
        # recent sample was below, we are (still) recovering.
        delta_repr = min(self._recent)

        # Thresholds widen once armed — classic hysteresis to avoid
        # oscillation when delta hovers around the arm point.
        if self._state == Severity.CRITICAL:
            warn_t = self.warn_blocks - HYSTERESIS_BLOCKS
            crit_t = self.critical_blocks - HYSTERESIS_BLOCKS
        elif self._state == Severity.WARN:
            warn_t = self.warn_blocks - HYSTERESIS_BLOCKS
            crit_t = self.critical_blocks
        else:  # CLEAR
            warn_t = self.warn_blocks
            crit_t = self.critical_blocks

        if delta_repr >= crit_t:
            target: Severity | None = Severity.CRITICAL
        elif delta_repr >= warn_t:
            target = Severity.WARN
        else:
            target = None  # CLEAR

        prev = self._state
        self._state = target

        if prev == target:
            return None

        # Escalation paths.
        if target == Severity.CRITICAL and prev != Severity.CRITICAL:
            return self._make_event(target, delta_repr, reference_block, local_block, crit_t)
        if target == Severity.WARN and prev not in (Severity.WARN, Severity.CRITICAL):
            return self._make_event(target, delta_repr, reference_block, local_block, warn_t)

        # De-escalation to CLEAR.
        if target is None and prev in (Severity.WARN, Severity.CRITICAL):
            return AlertEvent(
                rule="reference_lag",
                severity=Severity.RECOVERED,
                key="reference_lag",
                title="Reference lag normalized",
                detail=(
                    f"Local block caught up to within "
                    f"{self.warn_blocks - HYSTERESIS_BLOCKS} blocks of reference "
                    f"#{reference_block} (was {prev.value})."
                ),
            )

        # CRITICAL -> WARN stays silent (still in alarm).
        return None

    def _make_event(
        self,
        severity: Severity,
        delta: int,
        reference_block: int,
        local_block: int,
        threshold: int,
    ) -> AlertEvent:
        return AlertEvent(
            rule="reference_lag",
            severity=severity,
            key=f"reference_lag:{severity.value}",
            title=f"Reference-RPC lag {severity.value.upper()}",
            detail=(
                f"Local block #{local_block} is {delta} blocks behind "
                f"reference #{reference_block} (threshold {threshold} blocks, "
                f"confirmed over {self.window} consecutive samples). "
                f"If reference is also frozen — network-wide halt, not our node."
            ),
        )
