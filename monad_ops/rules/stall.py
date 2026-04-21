"""Block-height stall detector.

The rule answers: is the node actually stuck, or is our collector
just late? Measures gap as wall-clock now minus the last block's
``timestamp_ms`` (produced time), NOT the moment we received it. This
is the key difference from an earlier design that used
``time.monotonic()`` against the observation time — that version lit
up during the 2026-04-20 stress test whenever the event loop froze
for >10s (journalctl tailer fell behind), producing ~120 false
warn/critical alerts while the node actually kept producing blocks
every ~400ms.

Clock-based design stays deliberate: healthy testnet produces one
block every ~0.4s; a gap of 10s is 25x healthy interval — confident
signal. Same node+server host means clock skew is negligible. If
ever run on separate hosts, a small negative gap simply silences
the rule (correct fail-quiet).
"""

from __future__ import annotations

import time
from dataclasses import dataclass

from monad_ops.parser import ExecBlock
from monad_ops.rules.events import AlertEvent, Severity


# If our last on_block call happened within this many seconds, the
# tailer is actively processing — whatever the block's own timestamp
# says, we are NOT in a stall. This guards against the "tailer catch-up"
# pattern observed during the 2026-04-20 stress test: under load the
# monad-ops event loop freezes for ~30s, the journal pipe buffers
# blocks, then on_block() fires 75 times in quick succession against
# blocks whose timestamp_ms is 30s old. Pre-2026-04-20 the rule would
# alert 82 times in 15 minutes based on those stale timestamps, even
# though the node never actually paused.
_TAILER_LIVENESS_WINDOW_SEC = 2.0

# Recovery confirmation window. RECOVERED is NOT fired on the first
# block after a stall: instead, we wait this long while observing no
# re-stall, and only then announce recovery. Without this, the 2026-04-20
# post-stress channel saw 68 RECOVERED / 35 CRITICAL / 35 WARN in 2h —
# a ~2-minute flap cycle where each ~10–30s block gap produced CRITICAL,
# the next block produced RECOVERED, and the cycle restarted within
# seconds. The DedupingSink cooldown doesn't help because RECOVERED
# intentionally bypasses dedup (operators want to see recovery
# immediately), so every flap made it to Telegram. 60s is a healthy
# multiple of the post-stress gap period and long enough to coalesce
# typical oscillation into a single alert envelope.
_RECOVERY_CONFIRM_SEC = 60.0


@dataclass(slots=True)
class StallRule:
    warn_after_sec: int
    critical_after_sec: int

    _last_block: int | None = None
    _last_block_ts_sec: float | None = None   # unix epoch seconds from block.timestamp_ms
    _last_seen_wall: float | None = None      # unix epoch when we actually received it
    _current_state: Severity | None = None
    # Wall-clock time at which the last block arrived AFTER a prior
    # WARN/CRITICAL. Used to defer RECOVERED until production has been
    # steady for _RECOVERY_CONFIRM_SEC. None means "no pending recovery".
    _recovery_pending_since: float | None = None
    # Peak severity reached during the current alert envelope. We carry
    # this from on_tick's arm to on_tick's disarm so the eventual
    # RECOVERED detail can reference the worst state seen, not just the
    # most recent one.
    _peak_severity: Severity | None = None

    def on_block(self, block: ExecBlock, now_sec: float | None = None) -> AlertEvent | None:
        """Call on each new ExecBlock. RECOVERED is NOT emitted here —
        it's deferred to on_tick once production has looked steady for
        _RECOVERY_CONFIRM_SEC. Returning None from on_block keeps the
        channel quiet until we're confident the stall actually ended.
        """
        prev_state = self._current_state
        self._last_block = block.block_number
        self._last_block_ts_sec = block.timestamp_ms / 1000.0
        self._last_seen_wall = now_sec if now_sec is not None else time.time()

        # If we were armed (WARN/CRITICAL), arm the recovery countdown
        # instead of firing RECOVERED immediately. on_tick will fire
        # RECOVERED once the confirm window elapses with no new arm.
        if prev_state in (Severity.WARN, Severity.CRITICAL):
            if self._recovery_pending_since is None:
                self._recovery_pending_since = self._last_seen_wall
                if self._peak_severity is None:
                    self._peak_severity = prev_state
        return None

    def on_tick(self, now_sec: float | None = None) -> AlertEvent | None:
        """Call periodically (e.g. every second) to catch a stall even
        when no new blocks arrive. ``now_sec`` is unix-epoch seconds
        (wall-clock) — defaults to ``time.time()``.
        """
        if self._last_block_ts_sec is None or self._last_seen_wall is None:
            return None
        now_sec = now_sec if now_sec is not None else time.time()

        # Tailer liveness guard: if we've processed a block very
        # recently, our collector is catching up and the stale
        # block.timestamp_ms is NOT evidence the node stalled.
        if (now_sec - self._last_seen_wall) < _TAILER_LIVENESS_WINDOW_SEC:
            # During tailer-catch-up, DO still advance a pending
            # recovery — fresh blocks are landing, so the node is clearly
            # producing. Without this, a fast stream of blocks after a
            # stall would reset the confirm-window silence each tick.
            return self._maybe_confirm_recovery(now_sec)

        gap = now_sec - self._last_block_ts_sec

        desired: Severity | None = None
        if gap >= self.critical_after_sec:
            desired = Severity.CRITICAL
        elif gap >= self.warn_after_sec:
            desired = Severity.WARN

        # New-arm (or re-arm) path: if we're arming up from CLEAR, or
        # escalating WARN → CRITICAL, fire the event. Cancel any pending
        # recovery — the stall is ongoing, not recovering.
        if desired is not None and desired != self._current_state:
            # Suppress WARN if we're already at CRITICAL (escalation-only).
            if self._current_state == Severity.CRITICAL and desired == Severity.WARN:
                return None
            self._current_state = desired
            self._recovery_pending_since = None
            # Track peak severity across the envelope so the final
            # RECOVERED reflects the worst we saw.
            order = {Severity.WARN: 1, Severity.CRITICAL: 2}
            if (
                self._peak_severity is None
                or order.get(desired, 0) > order.get(self._peak_severity, 0)
            ):
                self._peak_severity = desired
            return AlertEvent(
                rule="stall",
                severity=desired,
                key=f"stall:{desired.value}",
                title=f"Block production {desired.value.upper()}",
                detail=(
                    f"No new block for {gap:.1f}s. "
                    f"Last seen block: {self._last_block}."
                ),
            )

        # No new arm. Check whether a pending recovery has matured into
        # a RECOVERED event.
        return self._maybe_confirm_recovery(now_sec)

    def _maybe_confirm_recovery(self, now_sec: float) -> AlertEvent | None:
        if self._recovery_pending_since is None:
            return None
        if (now_sec - self._recovery_pending_since) < _RECOVERY_CONFIRM_SEC:
            return None
        # Confirmed recovery. Reset state envelope.
        peak = self._peak_severity or self._current_state or Severity.WARN
        pending_for = now_sec - self._recovery_pending_since
        last_block = self._last_block
        self._current_state = None
        self._recovery_pending_since = None
        self._peak_severity = None
        return AlertEvent(
            rule="stall",
            severity=Severity.RECOVERED,
            key="stall",
            title="Block production recovered",
            detail=(
                f"Block production steady for {pending_for:.0f}s after a "
                f"{peak.value} stall. Last seen block: {last_block}."
            ),
        )
