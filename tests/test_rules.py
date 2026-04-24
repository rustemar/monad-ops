from monad_ops.parser import ExecBlock
from monad_ops.rules import (
    CodeColor,
    ReferenceLagRule,
    ReorgRule,
    RetrySpikeRule,
    Severity,
    StallRule,
    code_color_for,
)


def _block(n: int, tx: int = 2, rt: int = 0, rtp: float = 0.0, block_id: str | None = None) -> ExecBlock:
    return ExecBlock(
        block_number=n,
        block_id=block_id if block_id is not None else f"0x{n:064x}",
        timestamp_ms=1_776_000_000_000 + n,
        tx_count=tx,
        retried=rt,
        retry_pct=rtp,
        state_reset_us=50,
        tx_exec_us=100,
        commit_us=200,
        total_us=500,
        tps_effective=1000,
        tps_avg=1000,
        gas_used=100_000,
        gas_per_sec_effective=200,
        gas_per_sec_avg=200,
        active_chunks=1_000_000,
        slow_chunks=10_000_000,
    )


class TestStallRule:
    # Helper: clone a _block with an explicit timestamp_ms. Stall rule now
    # measures gap from block.timestamp_ms (produced time), not wall-clock
    # observation time — so tests must drive ts explicitly.
    @staticmethod
    def _block_at(n: int, ts_sec: float) -> ExecBlock:
        import dataclasses
        return dataclasses.replace(_block(n), timestamp_ms=int(ts_sec * 1000))

    def test_warn_then_critical_then_recovered(self):
        rule = StallRule(warn_after_sec=10, critical_after_sec=30)

        # First block produced at T=0 — baseline; no event.
        assert rule.on_block(self._block_at(1, 0.0), now_sec=0.0) is None
        # 5s later (wall clock), no tick-alert.
        assert rule.on_tick(now_sec=5.0) is None
        # 11s later, WARN.
        warn = rule.on_tick(now_sec=11.0)
        assert warn is not None and warn.severity == Severity.WARN
        # No duplicate warn.
        assert rule.on_tick(now_sec=12.0) is None
        # 31s later, CRITICAL.
        crit = rule.on_tick(now_sec=31.0)
        assert crit is not None and crit.severity == Severity.CRITICAL
        # Block (produced at T=32) arrives — does NOT fire RECOVERED yet;
        # recovery is deferred until production has looked steady for
        # _RECOVERY_CONFIRM_SEC. This is the anti-flap guard.
        assert rule.on_block(self._block_at(2, 32.0), now_sec=32.0) is None
        # Before the confirm window elapses, RECOVERED still held back.
        assert rule.on_tick(now_sec=60.0) is None
        # Past the confirm window (60s after the first steady block at
        # T=32 → T=92), RECOVERED fires.
        rec = rule.on_tick(now_sec=92.0)
        assert rec is not None and rec.severity == Severity.RECOVERED

    def test_flapping_does_not_flood_recovered(self):
        """Regression for 2026-04-20 post-stress flap: 68 RECOVERED in 2h.

        Block gap oscillates around the warn threshold — each crossing
        pre-fix produced WARN → RECOVERED → WARN in a tight loop, every
        RECOVERED bypassing the sink cooldown and reaching the Telegram
        channel. Post-fix, once armed the rule stays armed until
        production has been steady for _RECOVERY_CONFIRM_SEC — interim
        oscillation inside that window is coalesced into the original
        envelope, producing exactly 1 WARN at the start and 1 RECOVERED
        at the end, not N × pairs.
        """
        rule = StallRule(warn_after_sec=10, critical_after_sec=30)
        events: list = []

        # Baseline block at T=0.
        rule.on_block(self._block_at(1, 0.0), now_sec=0.0)

        # Stall 1: fire WARN at T=11 — one event emitted.
        e = rule.on_tick(now_sec=11.0)
        assert e is not None and e.severity == Severity.WARN
        events.append(e)

        # Block lands at T=12. Pending recovery starts, rule stays armed.
        assert rule.on_block(self._block_at(2, 12.0), now_sec=12.0) is None

        # Re-stall at T=23 — inside confirm window. Rule is already WARN;
        # on_tick sees no state change → silent.
        assert rule.on_tick(now_sec=23.0) is None

        # Another block at T=24 — pending recovery window reset silently.
        assert rule.on_block(self._block_at(3, 24.0), now_sec=24.0) is None

        # Re-stall again at T=35. Silent.
        assert rule.on_tick(now_sec=35.0) is None

        # Block at T=36 and then steady production every ~5s (typical
        # post-stress cadence). _recovery_pending_since anchors on the
        # FIRST post-alert block (T=12), so RECOVERED fires 60s after
        # that anchor. Feed blocks and check that intermediate ticks
        # stay silent until the anchor+60 threshold is crossed.
        for n, t in [(4, 36.0), (5, 41.0), (6, 46.0), (7, 51.0),
                     (8, 56.0), (9, 61.0), (10, 66.0)]:
            assert rule.on_block(self._block_at(n, t), now_sec=t) is None
            # on_tick right after a block: gap < warn, and anchor+60
            # (= T=72) not yet reached → silent.
            assert rule.on_tick(now_sec=t + 1.0) is None
        # Anchor was T=12. First tick past T=72 fires RECOVERED.
        e = rule.on_tick(now_sec=73.0)
        assert e is not None and e.severity == Severity.RECOVERED
        events.append(e)

        # Exactly 1 WARN at start + 1 RECOVERED at end. Pre-fix this
        # sequence would have produced 3 WARN + 3 RECOVERED events.
        sev = [x.severity for x in events]
        assert sev == [Severity.WARN, Severity.RECOVERED]

    def test_escalation_during_pending_recovery_cancels_countdown(self):
        """A re-stall severe enough to escalate (WARN → CRITICAL) during
        the pending-recovery window must cancel the countdown and fire
        the escalation. Otherwise an operator would miss a CRITICAL
        worsening in the middle of what initially looked like recovery.
        """
        rule = StallRule(warn_after_sec=10, critical_after_sec=30)

        # Baseline, then WARN at T=11.
        rule.on_block(self._block_at(1, 0.0), now_sec=0.0)
        e = rule.on_tick(now_sec=11.0)
        assert e is not None and e.severity == Severity.WARN

        # Block at T=12 starts pending recovery.
        assert rule.on_block(self._block_at(2, 12.0), now_sec=12.0) is None

        # Now no blocks for 35s from the last-block-ts — CRITICAL must fire
        # and cancel the pending recovery. on_tick at T=47 sees gap=35.
        e = rule.on_tick(now_sec=47.0)
        assert e is not None and e.severity == Severity.CRITICAL
        assert rule._recovery_pending_since is None

    def test_no_event_if_no_baseline_yet(self):
        rule = StallRule(warn_after_sec=10, critical_after_sec=30)
        assert rule.on_tick(now_sec=1000.0) is None

    def test_tailer_hiccup_does_not_fire(self):
        """Regression for 2026-04-20: event loop froze for ~25s, wall-clock
        advanced, but the node had kept producing blocks the whole time —
        block.timestamp_ms stayed close to wall-clock. Rule must stay quiet.
        """
        rule = StallRule(warn_after_sec=10, critical_after_sec=30)
        # Node produces a block at T=100.
        assert rule.on_block(self._block_at(1, 100.0), now_sec=100.0) is None
        # Our process was frozen for 25s. When it thaws, tick fires — but
        # the next block (produced at T=100.4) arrives right after. Tick
        # sees last_block_ts=100, wall=100.4 → gap 0.4s → silent.
        assert rule.on_tick(now_sec=100.4) is None
        # New block lands almost immediately.
        assert rule.on_block(self._block_at(2, 100.4), now_sec=100.4) is None

    def test_catchup_suppresses_stale_timestamp_alert(self):
        """Regression for the second half of 2026-04-20: under stress the
        tailer fell behind by ~30s, then caught up by processing a burst of
        blocks whose timestamps were 30s old. Each on_tick between those
        burst-arriving blocks would fire WARN/CRITICAL based on the stale
        block.timestamp_ms, even though we were actively receiving blocks.
        The liveness guard (`now_wall - last_seen_wall`) suppresses these.
        """
        rule = StallRule(warn_after_sec=10, critical_after_sec=30)
        # Block 1 arrives. Its timestamp is fresh (wall=100, ts=100).
        assert rule.on_block(self._block_at(1, 100.0), now_sec=100.0) is None
        # 30 seconds later, block 2 finally reaches us — but its
        # timestamp_ms says it was produced 29s ago (at wall=101).
        # gap = now_wall(130) - block.ts_sec(101) = 29s → would have
        # fired CRITICAL pre-fix.
        # But we JUST received it: last_seen_wall = 130. Next tick at 130.5
        # sees "last seen 0.5s ago" → catch-up → silent.
        assert rule.on_block(self._block_at(2, 101.0), now_sec=130.0) is None
        assert rule.on_tick(now_sec=130.5) is None
        assert rule.on_tick(now_sec=131.0) is None

    def test_real_stall_still_fires_when_tailer_is_idle(self):
        """Inverse of the catch-up test: if the tailer really stops getting
        blocks (last_seen_wall becomes old AND block.ts becomes old), we
        must still alert. This proves the liveness guard doesn't silence
        real stalls."""
        rule = StallRule(warn_after_sec=10, critical_after_sec=30)
        assert rule.on_block(self._block_at(1, 100.0), now_sec=100.0) is None
        # 35s pass, no blocks. Both last_seen_wall AND last_block_ts are
        # 35s old. CRITICAL fires.
        ev = rule.on_tick(now_sec=135.0)
        assert ev is not None and ev.severity == Severity.CRITICAL

    def test_recovered_message_flags_ops_side_when_catchup_burst(self):
        """Iter-20: when recovery closes a large block-number gap in a
        short window, the dominant interpretation is 'monad-ops tailer
        was paused while chain produced normally'. The RECOVERED detail
        gains a diagnostic line so a public-dashboard viewer reads the
        envelope as ops-side, not chain-side. Calibrated against the
        canonical 2026-04-23 20:29 UTC FP — 180 blocks in 60s = 3.0
        blk/s = 1.2× healthy rate, exactly at the threshold.
        """
        rule = StallRule(warn_after_sec=10, critical_after_sec=30)
        # Baseline at block 100, T=0.
        rule.on_block(self._block_at(100, 0.0), now_sec=0.0)
        # WARN fires at T=11 — gap 11s since baseline.
        warn = rule.on_tick(now_sec=11.0)
        assert warn is not None and warn.severity == Severity.WARN
        # Tailer thaws at T=12. During the 60s confirm window, blocks
        # land continuously (chain was producing throughout); their
        # chain timestamps track wall-clock so re-arm never triggers.
        # Anchor for confirm window = T=12 (first post-WARN block).
        # Final block before confirmation: 280 with chain_ts=72 ≈ wall.
        # Total caught up: 280-100 = 180 blocks across 60s = 3.0 blk/s.
        # rate = 3.0 ≥ 1.2 × 2.5 = 3.0 → diagnostic appears.
        for n, t in [(120, 12.0), (160, 24.0), (200, 36.0),
                     (240, 48.0), (260, 60.0), (280, 72.0)]:
            rule.on_block(self._block_at(n, t), now_sec=t)
        rec = rule.on_tick(now_sec=72.0)
        assert rec is not None and rec.severity == Severity.RECOVERED
        assert "Tailer caught up 180 blocks" in rec.detail
        assert "monad-ops processing pause" in rec.detail

    def test_recovered_message_silent_when_normal_recovery(self):
        """Inverse: a recovery where the chain genuinely paused (so
        only a handful of blocks land in the confirm window) should NOT
        carry the ops-side diagnostic. Otherwise the message reads as
        'tailer paused' on every recovery, defeating the differential."""
        rule = StallRule(warn_after_sec=10, critical_after_sec=30)
        rule.on_block(self._block_at(100, 0.0), now_sec=0.0)
        warn = rule.on_tick(now_sec=11.0)
        assert warn is not None and warn.severity == Severity.WARN
        # Only a few blocks land during recovery — chain was actually
        # slow. 5 blocks / 60s = 0.08 blk/s, well below the threshold.
        rule.on_block(self._block_at(101, 12.0), now_sec=12.0)
        rule.on_block(self._block_at(102, 25.0), now_sec=25.0)
        rule.on_block(self._block_at(103, 40.0), now_sec=40.0)
        rule.on_block(self._block_at(104, 55.0), now_sec=55.0)
        rule.on_block(self._block_at(105, 70.0), now_sec=70.0)
        # 60s confirm window — anchor at T=12 → fires at T=72.
        rec = rule.on_tick(now_sec=72.0)
        assert rec is not None and rec.severity == Severity.RECOVERED
        assert "Tailer caught up" not in rec.detail
        assert "processing pause" not in rec.detail

    def test_arm_block_captured_at_clear_to_warn_not_at_escalation(self):
        """Escalation WARN→CRITICAL must NOT reset the arm-block;
        otherwise the catchup count for the eventual RECOVERED would
        underreport (just the post-escalation slice). Captured at
        CLEAR→WARN, frozen through escalation, cleared on RECOVERED."""
        rule = StallRule(warn_after_sec=10, critical_after_sec=30)
        rule.on_block(self._block_at(100, 0.0), now_sec=0.0)
        # WARN at T=11 — captures arm_block=100
        warn = rule.on_tick(now_sec=11.0)
        assert warn is not None
        assert rule._arm_block == 100
        # CRITICAL at T=31 — must not overwrite arm_block
        crit = rule.on_tick(now_sec=31.0)
        assert crit is not None and crit.severity == Severity.CRITICAL
        assert rule._arm_block == 100   # still the original arm-time block
        # Recovery — tailer catches up. Counts (280-100), not (280-N_at_critical).
        rule.on_block(self._block_at(280, 32.0), now_sec=32.0)
        rec = rule.on_tick(now_sec=92.0)
        assert rec is not None and rec.severity == Severity.RECOVERED
        assert "180 blocks" in rec.detail
        # Reset on confirmed recovery.
        assert rule._arm_block is None


class TestRetrySpikeRule:
    def test_requires_full_window_before_firing(self):
        rule = RetrySpikeRule(window=10, warn_pct=50.0, critical_pct=75.0)
        events: list = []
        for i in range(9):
            events.append(rule.on_block(_block(i, rtp=100.0)))
        assert all(e is None for e in events)
        # 10th block fills the window with avg=100 -> CRITICAL in one jump.
        e = rule.on_block(_block(9, rtp=100.0))
        assert e is not None and e.severity == Severity.CRITICAL

    def test_full_lifecycle(self):
        """CLEAR -> WARN -> CRITICAL -> (CRITICAL->WARN silent) -> RECOVERED."""
        rule = RetrySpikeRule(window=4, warn_pct=50.0, critical_pct=75.0)
        events: list = []

        # 4 blocks at 60% — avg=60, cross WARN threshold on the 4th.
        for i, rtp in enumerate([60, 60, 60, 60]):
            e = rule.on_block(_block(i, rtp=float(rtp)))
            if e is not None:
                events.append(e)
        assert [e.severity for e in events] == [Severity.WARN]

        # 4 blocks at 80% — window slides, avg crosses 75 mid-way.
        events.clear()
        for i, rtp in enumerate([80, 80, 80, 80], start=10):
            e = rule.on_block(_block(i, rtp=float(rtp)))
            if e is not None:
                events.append(e)
        assert Severity.CRITICAL in [e.severity for e in events]

        # 4 blocks at 10% — CRITICAL drops through WARN (silent) to CLEAR -> RECOVERED.
        events.clear()
        for i, rtp in enumerate([10, 10, 10, 10], start=20):
            e = rule.on_block(_block(i, rtp=float(rtp)))
            if e is not None:
                events.append(e)
        assert [e.severity for e in events] == [Severity.RECOVERED]

    def test_critical_to_warn_is_silent(self):
        rule = RetrySpikeRule(window=4, warn_pct=50.0, critical_pct=75.0)
        # Push to CRITICAL first.
        for i, rtp in enumerate([80, 80, 80, 80]):
            rule.on_block(_block(i, rtp=float(rtp)))
        assert rule._state == Severity.CRITICAL

        # Slide to avg ~60 (WARN band) — should NOT emit.
        e = rule.on_block(_block(100, rtp=0.0))  # window: [80,80,80,0] avg=60
        assert e is None
        assert rule._state == Severity.WARN

    def test_hysteresis_silences_oscillation_at_warn_boundary(self):
        """Avg hovering right around warn_pct must NOT produce a chain of
        WARN→RECOVERED pairs. Without hysteresis the RECOVERED events
        bypass the sink cooldown and flood the alert channel with green
        'normalized' messages (observed in prod 2026-04-19 at retry_pct
        oscillating around the 50% arm threshold).
        """
        rule = RetrySpikeRule(window=4, warn_pct=50.0, critical_pct=75.0)

        # Prime the window at avg=52 -> enters WARN once.
        events = []
        for i, rtp in enumerate([52, 52, 52, 52]):
            e = rule.on_block(_block(i, rtp=float(rtp)))
            if e is not None:
                events.append(e)
        assert [e.severity for e in events] == [Severity.WARN]

        # Now oscillate avg just below/above 50% — hysteresis means the
        # rule should keep _state = WARN silently until avg drops below
        # warn_pct - HYSTERESIS (= 45%).
        events.clear()
        for i, rtp in enumerate([48, 52, 48, 52, 48, 52, 48, 52], start=10):
            e = rule.on_block(_block(i, rtp=float(rtp)))
            if e is not None:
                events.append(e)
        assert events == [], f"hysteresis breached, spurious events: {events}"
        assert rule._state == Severity.WARN

        # Driving avg firmly below the disarm threshold (< 45%) SHOULD
        # finally produce a single RECOVERED.
        events.clear()
        for i, rtp in enumerate([30, 30, 30, 30], start=100):
            e = rule.on_block(_block(i, rtp=float(rtp)))
            if e is not None:
                events.append(e)
        assert [e.severity for e in events] == [Severity.RECOVERED]

    def test_quiet_period_gate_blocks_warn_arming(self):
        """When avg tx/block < min_window_tx_avg, rule must NOT arm WARN
        even if retry_pct avg crosses warn_pct. retry_pct on 3-tx blocks
        is small-sample noise, not an actionable signal.
        """
        rule = RetrySpikeRule(
            window=4, warn_pct=50.0, critical_pct=75.0,
            min_window_tx_avg=50.0,
        )
        # High retry_pct but tiny blocks — gate engages.
        events = []
        for i in range(4):
            e = rule.on_block(_block(i, tx=3, rt=2, rtp=66.0))
            if e is not None:
                events.append(e)
        assert events == []
        assert rule._state is None

    def test_quiet_period_gate_blocks_critical_arming(self):
        """CRITICAL is gated too — 99% retry on a 5-tx block is still
        small-sample noise, not a stress-contention event.
        """
        rule = RetrySpikeRule(
            window=4, warn_pct=50.0, critical_pct=75.0,
            min_window_tx_avg=50.0,
        )
        events = []
        for i in range(4):
            e = rule.on_block(_block(i, tx=5, rt=5, rtp=99.0))
            if e is not None:
                events.append(e)
        assert events == []
        assert rule._state is None

    def test_quiet_period_gate_releases_when_volume_rises(self):
        """Once avg tx/block crosses min_window_tx_avg, the gate releases
        and normal arming resumes.
        """
        rule = RetrySpikeRule(
            window=4, warn_pct=50.0, critical_pct=75.0,
            min_window_tx_avg=50.0,
        )
        # Prime with high-retry tiny blocks — gated, no event.
        for i in range(4):
            rule.on_block(_block(i, tx=3, rt=2, rtp=66.0))
        assert rule._state is None

        # Slide in high-retry large blocks — avg tx rises, gate releases.
        events = []
        for i in range(4):
            e = rule.on_block(_block(100 + i, tx=500, rt=330, rtp=66.0))
            if e is not None:
                events.append(e)
        assert [e.severity for e in events] == [Severity.WARN]

    def test_quiet_period_gate_does_not_block_recovered(self):
        """If the rule is already armed and network quiets down, RECOVERED
        must still fire so operators see the all-clear. The gate only
        suppresses arming, never disarming.
        """
        rule = RetrySpikeRule(
            window=4, warn_pct=50.0, critical_pct=75.0,
            min_window_tx_avg=50.0,
        )
        # Arm at WARN with high-volume blocks.
        for i in range(4):
            rule.on_block(_block(i, tx=500, rt=330, rtp=66.0))
        assert rule._state == Severity.WARN

        # Now network quiets + retry drops — gate would suppress arming,
        # but de-escalation must still go through.
        events = []
        for i in range(4):
            e = rule.on_block(_block(100 + i, tx=3, rt=0, rtp=0.0))
            if e is not None:
                events.append(e)
        assert [e.severity for e in events] == [Severity.RECOVERED]
        assert rule._state is None

    def test_quiet_period_gate_disabled_by_default(self):
        """Without min_window_tx_avg set, rule behaves as before: any
        block volume can arm WARN/CRITICAL. Backward-compat guard.
        """
        rule = RetrySpikeRule(window=4, warn_pct=50.0, critical_pct=75.0)
        events = []
        for i in range(4):
            e = rule.on_block(_block(i, tx=3, rt=2, rtp=66.0))
            if e is not None:
                events.append(e)
        assert [e.severity for e in events] == [Severity.WARN]


class TestReorgRule:
    def test_first_sight_of_each_block_is_silent(self):
        rule = ReorgRule()
        for i in range(50):
            assert rule.on_block(_block(i)) is None
        assert rule.reorg_count == 0

    def test_duplicate_same_id_is_silent(self):
        """Journal replay / bootstrap can deliver the same block twice;
        same block_number with the same block_id must not fire."""
        rule = ReorgRule()
        b = _block(100, block_id="0xaaaa")
        assert rule.on_block(b) is None
        assert rule.on_block(b) is None
        assert rule.reorg_count == 0

    def test_different_id_at_same_number_fires_critical(self):
        rule = ReorgRule()
        first = _block(100, block_id="0x" + "a" * 64)
        second = _block(100, block_id="0x" + "b" * 64)
        assert rule.on_block(first) is None
        ev = rule.on_block(second)
        assert ev is not None
        assert ev.severity == Severity.CRITICAL
        assert ev.rule == "reorg"
        assert "100" in ev.detail
        assert rule.reorg_count == 1
        assert rule.last_reorg_number == 100
        assert rule.last_reorg_old_id == first.block_id
        assert rule.last_reorg_new_id == second.block_id

    def test_counters_accumulate_across_multiple_reorgs(self):
        rule = ReorgRule()
        rule.on_block(_block(100, block_id="0xa"))
        rule.on_block(_block(100, block_id="0xb"))  # reorg 1
        rule.on_block(_block(200, block_id="0xc"))
        rule.on_block(_block(200, block_id="0xd"))  # reorg 2
        assert rule.reorg_count == 2
        assert rule.last_reorg_number == 200

    def test_track_window_evicts_oldest(self):
        """After more than ``track_window`` distinct numbers, the
        earliest entry is forgotten — a reorg at that height would be
        undetectable, which is the documented trade-off."""
        rule = ReorgRule(track_window=3)
        for n in [1, 2, 3]:
            rule.on_block(_block(n, block_id=f"0x{n:x}"))
        # #3 is still in the window; a diverging id at #3 fires normally.
        ev = rule.on_block(_block(3, block_id="0xff3"))
        assert ev is not None and ev.severity == Severity.CRITICAL
        assert rule.reorg_count == 1

        # Advance past #1's eviction. After blocks 4 and 5, _seen = {3,4,5}
        # and #1 is forgotten.
        for n in [4, 5]:
            rule.on_block(_block(n, block_id=f"0x{n:x}"))
        assert rule.on_block(_block(1, block_id="0xff1")) is None
        assert rule.reorg_count == 1  # unchanged — we can no longer see it

    def test_second_reorg_at_same_height_produces_distinct_key(self):
        """If the same block_number gets *another* new id after an initial
        reorg, the new AlertEvent.key must be distinct so the deduping
        sink doesn't collapse it into the previous one."""
        rule = ReorgRule()
        rule.on_block(_block(50, block_id="0xaaaa"))
        ev1 = rule.on_block(_block(50, block_id="0xbbbb"))
        ev2 = rule.on_block(_block(50, block_id="0xcccc"))
        assert ev1 is not None and ev2 is not None
        assert ev1.key != ev2.key

    def test_bootstrap_seeds_counter_and_last_reorg_fields(self):
        """Counters restored from storage must populate the rule so the
        next live reorg emits "Total reorgs observed: N+1", not 1."""
        rule = ReorgRule()
        rule.bootstrap(
            count=2,
            last_block_number=26543283,
            last_old_id="0x8bca75d2…80987e",
            last_new_id="0x4e18d392…ab61d4",
            last_ts_ms=1_776_638_584_137,
        )
        assert rule.reorg_count == 2
        assert rule.last_reorg_number == 26543283
        assert rule.last_reorg_old_id == "0x8bca75d2…80987e"
        assert rule.last_reorg_new_id == "0x4e18d392…ab61d4"
        assert rule.last_reorg_ts_ms == 1_776_638_584_137

        # A real reorg now increments *from* the bootstrapped count.
        ev = rule.on_block(_block(9999, block_id="0xfirst"))
        assert ev is None  # first sighting; no reorg
        ev = rule.on_block(_block(9999, block_id="0xsecond"))
        assert ev is not None
        assert rule.reorg_count == 3
        assert "Total reorgs observed: 3" in ev.detail

    def test_bootstrap_accepts_minimal_count_only(self):
        """When we have a count but no detail parse succeeded, the rule
        still honours the count and leaves last_reorg_* untouched."""
        rule = ReorgRule()
        rule.bootstrap(count=5)
        assert rule.reorg_count == 5
        assert rule.last_reorg_number is None
        assert rule.last_reorg_old_id is None


class TestReferenceLagRule:
    def _s(self, rule, ref, local, err=None):
        return rule.on_sample(reference_block=ref, local_block=local, reference_error=err)

    def test_zero_lag_is_silent(self):
        rule = ReferenceLagRule(warn_blocks=15, critical_blocks=60, window=2)
        assert self._s(rule, 1000, 1000) is None
        assert self._s(rule, 1001, 1001) is None
        assert rule._state is None

    def test_probe_error_does_not_change_state(self):
        """A failing probe must not flip us to CLEAR or anywhere else —
        we simply have no evidence from that sample."""
        rule = ReferenceLagRule(warn_blocks=15, critical_blocks=60, window=2)
        # Prime to WARN first
        self._s(rule, 1020, 1000)
        self._s(rule, 1020, 1000)
        assert rule._state == Severity.WARN
        # Probe fails — state must stick
        ev = self._s(rule, None, 1000, err="transport: timeout")
        assert ev is None
        assert rule._state == Severity.WARN

    def test_local_ahead_never_alerts(self):
        """Negative delta (our RPC fresher than the public endpoint) is
        common and must NOT produce lag alerts."""
        rule = ReferenceLagRule(warn_blocks=15, critical_blocks=60, window=2)
        for _ in range(5):
            ev = self._s(rule, 1000, 1010)  # we are 10 blocks ahead
            assert ev is None
        assert rule._state is None

    def test_requires_full_window_before_firing(self):
        rule = ReferenceLagRule(warn_blocks=15, critical_blocks=60, window=3)
        # Two bad samples aren't enough when window=3
        assert self._s(rule, 1020, 1000) is None
        assert self._s(rule, 1022, 1000) is None
        # Third completes the window and fires WARN
        ev = self._s(rule, 1025, 1000)
        assert ev is not None and ev.severity == Severity.WARN

    def test_escalation_warn_to_critical(self):
        rule = ReferenceLagRule(warn_blocks=15, critical_blocks=60, window=2)
        # 2 samples around 20-block lag → WARN
        self._s(rule, 1020, 1000)
        ev1 = self._s(rule, 1022, 1000)
        assert ev1 is not None and ev1.severity == Severity.WARN
        # 2 samples at 70-block lag → CRITICAL
        self._s(rule, 1070, 1000)
        ev2 = self._s(rule, 1075, 1000)
        assert ev2 is not None and ev2.severity == Severity.CRITICAL

    def test_recovery_emits_recovered_once(self):
        rule = ReferenceLagRule(warn_blocks=15, critical_blocks=60, window=2)
        # Prime to WARN
        self._s(rule, 1020, 1000)
        assert self._s(rule, 1022, 1000).severity == Severity.WARN
        # First sample below disarm threshold (warn_blocks - HYSTERESIS = 10)
        # tips the min of the window below threshold -> RECOVERED fires.
        ev = self._s(rule, 1002, 1000)
        assert ev is not None and ev.severity == Severity.RECOVERED
        # Already recovered; further good samples are silent
        assert self._s(rule, 1001, 1000) is None
        assert self._s(rule, 1000, 1000) is None

    def test_hysteresis_silences_oscillation_at_warn_boundary(self):
        """Delta hovering at 14–16 blocks (just below/above warn=15)
        must not oscillate once armed. Same property RetrySpikeRule
        guards — hysteresis band is 5 blocks for this rule."""
        rule = ReferenceLagRule(warn_blocks=15, critical_blocks=60, window=2)
        # Arm WARN at sustained ~17 lag
        self._s(rule, 1017, 1000)
        assert self._s(rule, 1017, 1000).severity == Severity.WARN

        # Now oscillate 12 / 16 / 12 / 16 — still above disarm (10), must stay WARN silently
        for delta in [12, 16, 12, 16, 12, 16]:
            ev = self._s(rule, 1000 + delta, 1000)
            assert ev is None, f"hysteresis breached at delta={delta}, got {ev}"
        assert rule._state == Severity.WARN

        # Drop firmly below disarm (10) → RECOVERED on first sample
        # whose min-of-window falls below the disarm threshold.
        ev = self._s(rule, 1005, 1000)
        assert ev is not None and ev.severity == Severity.RECOVERED


class TestCodeColor:
    """Severity-to-Foundation-colour-code mapping.

    The mapping is Foundation-facing (Jackson's 2026-03-26 colour-code
    framework) and must remain stable: dashboards, Telegram alerts and
    third-party integrators will key on the emitted strings. Test every
    severity explicitly so a future reshuffle shows up as a failure
    rather than a silent semantic drift.
    """

    def test_critical_is_red(self):
        assert code_color_for(Severity.CRITICAL) is CodeColor.RED

    def test_warn_is_orange(self):
        assert code_color_for(Severity.WARN) is CodeColor.ORANGE

    def test_info_is_green(self):
        assert code_color_for(Severity.INFO) is CodeColor.GREEN

    def test_recovered_is_green(self):
        """RECOVERED is informational: the incident that drove the arm is
        over, no operator action required. Maps to GREEN same as INFO."""
        assert code_color_for(Severity.RECOVERED) is CodeColor.GREEN

    def test_values_are_plain_lowercase(self):
        """The JSON payload uses the string value, not the enum name.
        Pin the wire format so it can't drift to e.g. "RED" uppercase
        (which would break a deployed dashboard after a restart)."""
        assert CodeColor.RED.value == "red"
        assert CodeColor.ORANGE.value == "orange"
        assert CodeColor.GREEN.value == "green"
