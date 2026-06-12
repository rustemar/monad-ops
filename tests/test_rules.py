from monad_ops.collector.process_restart import InvocationSnapshot
from monad_ops.parser import ConsensusEvent, ConsensusEventKind, ExecBlock
from monad_ops.rules import (
    BlockProcessingSlowdownRule,
    CodeColor,
    NetworkLayerSignalRule,
    ProcessRestartRule,
    ReferenceLagRule,
    ReorgRule,
    RetrySpikeRule,
    Severity,
    StallRule,
    WaltraceFloodRule,
    code_color_for,
)


def _block(
    n: int,
    tx: int = 2,
    rt: int = 0,
    rtp: float = 0.0,
    block_id: str | None = None,
    total_us: int = 500,
) -> ExecBlock:
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
        total_us=total_us,
        tps_effective=1000,
        tps_avg=1000,
        gas_used=100_000,
        gas_per_sec_effective=200,
        gas_per_sec_avg=200,
        active_chunks=1_000_000,
        storage_cache_size=10_000_000,
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

    def test_single_reorg_defaults_to_info(self):
        """An isolated divergence is INFO, not WARN — pre-finalization
        block-id changes are HotStuff-2 expected behaviour (re-execution
        at the same height before finalization). The 2026-05-03 reframe
        moved the default down a tier so the dashboard doesn't paint
        ORANGE on background protocol behaviour. WARN is reserved for
        clusters; CRITICAL is reserved for actual node-failure rules."""
        rule = ReorgRule()
        first = _block(100, block_id="0x" + "a" * 64)
        second = _block(100, block_id="0x" + "b" * 64)
        assert rule.on_block(first) is None
        ev = rule.on_block(second)
        assert ev is not None
        assert ev.severity == Severity.INFO
        assert ev.rule == "reorg"
        assert "100" in ev.detail
        assert "Cluster" not in ev.detail
        assert "finality violation" in ev.detail
        assert rule.reorg_count == 1
        assert rule.last_reorg_number == 100
        assert rule.last_reorg_old_id == first.block_id
        assert rule.last_reorg_new_id == second.block_id

    def test_cluster_threshold_escalates_to_warn(self):
        """Once cluster_threshold divergences land inside
        cluster_window_sec, the next event is promoted to WARN with a
        cluster note. Post-2026-05-03 reframe: the previous CRITICAL
        tier was retired because the canonical chain finalizes
        regardless of pre-finalization divergence — clusters are
        operationally interesting (network-layer correlation) but not
        a node/chain emergency."""
        # Tight window so the 4 events fall comfortably inside it.
        rule = ReorgRule(cluster_window_sec=600, cluster_threshold=3)
        # block timestamps are 1ms apart in _block fixture; spread them
        # to seconds so the cluster window arithmetic is clear.
        import dataclasses
        def at(n, sec, bid):
            b = _block(n, block_id=bid)
            return dataclasses.replace(b, timestamp_ms=int(sec * 1000))
        # 3 divergences at 0, 60, 120 seconds — first two INFO, third WARN.
        rule.on_block(at(100, 0, "0xa"))
        ev1 = rule.on_block(at(100, 10, "0xb"))
        rule.on_block(at(200, 20, "0xc"))
        ev2 = rule.on_block(at(200, 30, "0xd"))
        rule.on_block(at(300, 40, "0xe"))
        ev3 = rule.on_block(at(300, 50, "0xf"))
        assert ev1.severity == Severity.INFO
        assert ev2.severity == Severity.INFO
        assert ev3.severity == Severity.WARN
        assert "Cluster" in ev3.detail
        assert "3 divergences" in ev3.detail

    def test_cluster_window_eviction_drops_back_to_info(self):
        """Once old divergences fall outside cluster_window_sec, the
        rule de-escalates back to INFO — a cluster from yesterday
        should not keep painting today's isolated event as WARN."""
        import dataclasses
        rule = ReorgRule(cluster_window_sec=600, cluster_threshold=3)
        def at(n, sec, bid):
            b = _block(n, block_id=bid)
            return dataclasses.replace(b, timestamp_ms=int(sec * 1000))
        # 3 divergences inside the window → WARN on the 3rd.
        rule.on_block(at(100, 0, "0xa"))
        rule.on_block(at(100, 1, "0xb"))
        rule.on_block(at(200, 2, "0xc"))
        rule.on_block(at(200, 3, "0xd"))
        rule.on_block(at(300, 4, "0xe"))
        ev_warn_cluster = rule.on_block(at(300, 5, "0xf"))
        assert ev_warn_cluster.severity == Severity.WARN
        # Now jump well past the cluster window — next event is alone.
        rule.on_block(at(900, 10000, "0xg"))
        ev_info = rule.on_block(at(900, 10001, "0xh"))
        assert ev_info.severity == Severity.INFO
        assert "Cluster" not in ev_info.detail

    def test_counters_accumulate_across_multiple_reorgs(self):
        rule = ReorgRule()
        rule.on_block(_block(100, block_id="0xa"))
        rule.on_block(_block(100, block_id="0xb"))  # reorg 1
        rule.on_block(_block(200, block_id="0xc"))
        rule.on_block(_block(200, block_id="0xd"))  # reorg 2
        assert rule.reorg_count == 2

    def test_cluster_repeats_stay_info_until_window_clears(self):
        """Only the first event that opens a cluster fires WARN; the rest
        stay INFO with an "ongoing" note. Otherwise a sustained burst of
        10 divergences fires 10 WARN alerts that say the same thing."""
        import dataclasses
        rule = ReorgRule(cluster_window_sec=600, cluster_threshold=3)
        def at(n, sec, bid):
            b = _block(n, block_id=bid)
            return dataclasses.replace(b, timestamp_ms=int(sec * 1000))
        rule.on_block(at(100, 0, "0xa"))
        ev1 = rule.on_block(at(100, 1, "0xb"))
        rule.on_block(at(200, 2, "0xc"))
        ev2 = rule.on_block(at(200, 3, "0xd"))
        rule.on_block(at(300, 4, "0xe"))
        ev3 = rule.on_block(at(300, 5, "0xf"))
        rule.on_block(at(400, 6, "0xg"))
        ev4 = rule.on_block(at(400, 7, "0xh"))
        rule.on_block(at(500, 8, "0xi"))
        ev5 = rule.on_block(at(500, 9, "0xj"))
        assert ev1.severity == Severity.INFO
        assert ev2.severity == Severity.INFO
        assert ev3.severity == Severity.WARN
        assert ev4.severity == Severity.INFO
        assert "ongoing" in ev4.detail
        assert ev5.severity == Severity.INFO
        assert "ongoing" in ev5.detail

    def test_cluster_re_arms_after_window_clears(self):
        """A second cluster following a quiet period must re-arm WARN —
        otherwise we'd silently miss the second burst."""
        import dataclasses
        rule = ReorgRule(cluster_window_sec=600, cluster_threshold=3)
        def at(n, sec, bid):
            b = _block(n, block_id=bid)
            return dataclasses.replace(b, timestamp_ms=int(sec * 1000))
        rule.on_block(at(100, 0, "0xa"))
        rule.on_block(at(100, 1, "0xb"))
        rule.on_block(at(200, 2, "0xc"))
        rule.on_block(at(200, 3, "0xd"))
        rule.on_block(at(300, 4, "0xe"))
        first_warn = rule.on_block(at(300, 5, "0xf"))
        assert first_warn.severity == Severity.WARN
        rule.on_block(at(400, 10000, "0xg"))
        ev_info = rule.on_block(at(400, 10001, "0xh"))
        assert ev_info.severity == Severity.INFO
        assert "Cluster" not in ev_info.detail
        rule.on_block(at(500, 10002, "0xi"))
        rule.on_block(at(500, 10003, "0xj"))
        rule.on_block(at(600, 10004, "0xk"))
        second_warn = rule.on_block(at(600, 10005, "0xl"))
        assert second_warn.severity == Severity.WARN

    def test_track_window_evicts_oldest(self):
        """After more than ``track_window`` distinct numbers, the
        earliest entry is forgotten — a reorg at that height would be
        undetectable, which is the documented trade-off."""
        rule = ReorgRule(track_window=3)
        for n in [1, 2, 3]:
            rule.on_block(_block(n, block_id=f"0x{n:x}"))
        # #3 is still in the window; a diverging id at #3 fires normally
        # — at INFO since this is a single, isolated divergence.
        ev = rule.on_block(_block(3, block_id="0xff3"))
        assert ev is not None and ev.severity == Severity.INFO
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
        next live divergence emits "Total divergences observed: N+1",
        not 1."""
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

        # A real divergence now increments *from* the bootstrapped count.
        ev = rule.on_block(_block(9999, block_id="0xfirst"))
        assert ev is None  # first sighting; no divergence
        ev = rule.on_block(_block(9999, block_id="0xsecond"))
        assert ev is not None
        assert rule.reorg_count == 3
        assert "Total divergences observed: 3" in ev.detail

    def test_bootstrap_accepts_minimal_count_only(self):
        """When we have a count but no detail parse succeeded, the rule
        still honours the count and leaves last_reorg_* untouched."""
        rule = ReorgRule()
        rule.bootstrap(count=5)
        assert rule.reorg_count == 5
        assert rule.last_reorg_number is None
        assert rule.last_reorg_old_id is None

    def test_bootstrap_recent_ts_preserves_cluster_state(self):
        """Restart in the middle of a cluster must not drop severity
        back to INFO — bootstrap rehydrates the cluster-window deque."""
        import dataclasses
        rule = ReorgRule(cluster_window_sec=600, cluster_threshold=3)
        rule.bootstrap(
            count=2,
            recent_ts_seconds=[1000.0, 1100.0],  # 2 divergences already in window
        )
        # The third divergence inside the window should now escalate.
        b1 = _block(500, block_id="0xpre")
        b2 = _block(500, block_id="0xpost")
        b1 = dataclasses.replace(b1, timestamp_ms=1_200_000)
        b2 = dataclasses.replace(b2, timestamp_ms=1_201_000)
        assert rule.on_block(b1) is None
        ev = rule.on_block(b2)
        assert ev is not None
        assert ev.severity == Severity.WARN
        assert "Cluster" in ev.detail


class TestBlockProcessingSlowdownRule:
    """Predictive precursor for ``StallRule``. Fires while the node is
    still keeping up but per-block ``total_us`` has shifted into a
    danger zone — gives operators time to react before cadence drops."""

    def _rule(self, window: int = 5, warn_us: int = 10_000, critical_us: int = 50_000) -> BlockProcessingSlowdownRule:
        # Tiny window so the tests don't have to feed 120 blocks each.
        return BlockProcessingSlowdownRule(
            window=window, warn_us=warn_us, critical_us=critical_us
        )

    def test_silent_until_window_full(self):
        """Don't fire on partial windows — one anomalous block at
        startup must not trigger the alarm."""
        rule = self._rule(window=5)
        # 4 blocks, all over WARN threshold — but window not full.
        for n in range(4):
            assert rule.on_block(_block(n, total_us=20_000)) is None

    def test_quiet_baseline_stays_clear(self):
        """Median 500 µs (the test-fixture default) is well below
        warn_us = 10 ms; rule must stay CLEAR."""
        rule = self._rule(window=5)
        events = []
        for n in range(20):
            ev = rule.on_block(_block(n))  # total_us=500
            if ev is not None:
                events.append(ev)
        assert events == []

    def test_warn_arms_when_median_crosses_warn_threshold(self):
        """Sustained median at ~12 ms (above warn_us = 10 ms but below
        critical_us = 50 ms) should arm WARN."""
        rule = self._rule(window=5)
        # Fill the window with 12 ms blocks.
        events = []
        for n in range(5):
            ev = rule.on_block(_block(n, total_us=12_000))
            if ev is not None:
                events.append(ev)
        assert len(events) == 1
        assert events[0].severity == Severity.WARN
        assert events[0].rule == "block_processing_slowdown"
        assert "12.0 ms" in events[0].detail
        assert "Predictive" in events[0].detail

    def test_critical_arms_directly_from_clear(self):
        """Median jumping straight past WARN to CRITICAL fires CRITICAL
        only — not a WARN+CRITICAL pair."""
        rule = self._rule(window=5)
        events = []
        for n in range(5):
            ev = rule.on_block(_block(n, total_us=80_000))  # 80 ms, > critical 50 ms
            if ev is not None:
                events.append(ev)
        assert len(events) == 1
        assert events[0].severity == Severity.CRITICAL

    def test_warn_to_critical_escalation(self):
        """WARN arm, then median rises past critical_us → CRITICAL.
        The intermediate WARN is NOT re-emitted."""
        rule = self._rule(window=5)
        # Fill with 15 ms (WARN territory).
        for n in range(5):
            rule.on_block(_block(n, total_us=15_000))
        # State is now WARN. Drift up to 60 ms (CRITICAL).
        events = []
        for n in range(5, 15):
            ev = rule.on_block(_block(n, total_us=60_000))
            if ev is not None:
                events.append(ev)
        assert any(e.severity == Severity.CRITICAL for e in events)
        # Should not see any extra WARN events during the escalation.
        assert sum(1 for e in events if e.severity == Severity.WARN) == 0

    def test_recovered_fires_only_after_hysteresis_clears(self):
        """A median grazing back below the arm threshold must NOT
        fire RECOVERED yet; it must drop below `arm * (1 - hysteresis)`
        — otherwise boundary oscillation produces a flap."""
        rule = self._rule(window=5)
        # Arm at 12 ms.
        for n in range(5):
            rule.on_block(_block(n, total_us=12_000))
        assert rule._state == Severity.WARN

        # Push median to 9 ms — just below warn_us=10 ms but still
        # above the disarm threshold (8 ms = warn_us * 0.8). Should
        # stay armed.
        for n in range(5, 10):
            ev = rule.on_block(_block(n, total_us=9_000))
            assert ev is None
        assert rule._state == Severity.WARN

        # Now drop to 5 ms — well below 8 ms disarm threshold.
        events = []
        for n in range(10, 15):
            ev = rule.on_block(_block(n, total_us=5_000))
            if ev is not None:
                events.append(ev)
        assert len(events) == 1
        assert events[0].severity == Severity.RECOVERED
        assert rule._state is None

    def test_critical_to_warn_is_silent(self):
        """De-escalation CRITICAL → WARN stays silent — we're still
        in alarm overall. RECOVERED fires only on full disarm to CLEAR."""
        rule = self._rule(window=5)
        # Arm at CRITICAL (80 ms).
        for n in range(5):
            rule.on_block(_block(n, total_us=80_000))
        assert rule._state == Severity.CRITICAL

        # Drop to 15 ms (WARN band).
        events = []
        for n in range(5, 12):
            ev = rule.on_block(_block(n, total_us=15_000))
            if ev is not None:
                events.append(ev)
        # No event emitted on the CRITICAL→WARN transition.
        assert events == []
        assert rule._state == Severity.WARN

    def test_single_outlier_block_doesnt_arm(self):
        """A 200 ms block among 4 normal ones should not push the
        median high enough to arm — that's the whole point of using
        median over mean."""
        rule = self._rule(window=5)
        # 4 quiet (500 µs) + 1 outlier (200 ms).
        for n in range(4):
            assert rule.on_block(_block(n, total_us=500)) is None
        ev = rule.on_block(_block(4, total_us=200_000))
        # Window is now [500, 500, 500, 500, 200_000]; sorted median = 500.
        assert ev is None
        assert rule._state is None


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


class TestNetworkLayerSignalRule:
    """Aggregate-rate rule across three monad-bft network-layer event
    classes. Sparse at baseline (<1 event/min observed); fires on
    cluster-shape bursts that historically co-occur with chain
    disagreement. Rule is per-event for the count and per-tick for
    natural de-arming as events fall out of the rolling window."""

    def _ev(self, kind: ConsensusEventKind, ts_sec: float, peer: str | None = None) -> ConsensusEvent:
        return ConsensusEvent(
            kind=kind, round=0, epoch=None, ts_ms=int(ts_sec * 1000), peer=peer,
        )

    def _decrypt(self, ts: float, peer: str | None = None) -> ConsensusEvent:
        # Default to a fresh-looking peer per timestamp so tests that
        # expect CRITICAL escalation get the diversity they need without
        # threading peers through every call site.
        if peer is None:
            peer = f"10.0.0.{int(ts) % 250 + 1}:8001"
        return self._ev(ConsensusEventKind.NETWORK_DECRYPT_FAIL, ts, peer=peer)

    def _session(self, ts: float) -> ConsensusEvent:
        return self._ev(ConsensusEventKind.NETWORK_SESSION_TIMEOUT, ts)

    def _ts_invalid(self, ts: float) -> ConsensusEvent:
        return self._ev(ConsensusEventKind.NETWORK_TIMESTAMP_INVALID, ts)

    def test_quiet_baseline_stays_clear(self):
        """Stray events (1-2 per minute) below warn_count must not arm."""
        rule = NetworkLayerSignalRule(window_sec=300, warn_count=5, critical_count=15)
        events = []
        # 4 events over 5 min — below warn_count=5.
        for i in range(4):
            ev = rule.on_event(self._decrypt(i * 60), now_sec=i * 60)
            if ev is not None:
                events.append(ev)
        assert events == []

    def test_warn_arms_on_burst(self):
        """5 events of any class within window_sec arms WARN."""
        rule = NetworkLayerSignalRule(window_sec=300, warn_count=5, critical_count=15)
        events = []
        for i in range(5):
            ev = rule.on_event(self._decrypt(100.0 + i), now_sec=100.0 + i)
            if ev is not None:
                events.append(ev)
        assert len(events) == 1
        assert events[0].severity == Severity.WARN
        assert events[0].rule == "network_layer_signal"
        assert "5 monad-bft network-layer event" in events[0].detail
        assert "decrypt-fail=5" in events[0].detail
        assert "session-timeout=0" in events[0].detail
        assert "Predictive" in events[0].detail

    def test_critical_arms_directly_on_large_burst(self):
        """A burst that lands above critical_count arms CRITICAL
        directly, not WARN+CRITICAL."""
        rule = NetworkLayerSignalRule(window_sec=300, warn_count=5, critical_count=15)
        events = []
        for i in range(15):
            # Mix all three classes — the rule sums across.
            kind = [
                ConsensusEventKind.NETWORK_DECRYPT_FAIL,
                ConsensusEventKind.NETWORK_SESSION_TIMEOUT,
                ConsensusEventKind.NETWORK_TIMESTAMP_INVALID,
            ][i % 3]
            ev = rule.on_event(self._ev(kind, 100.0 + i), now_sec=100.0 + i)
            if ev is not None:
                events.append(ev)
        # First arm is at the 5th event (WARN), then at the 15th (CRITICAL).
        severities = [e.severity for e in events]
        assert Severity.CRITICAL in severities
        # Per-class breakdown should reflect the mix (5/5/5).
        crit = next(e for e in events if e.severity == Severity.CRITICAL)
        assert "decrypt-fail=5" in crit.detail
        assert "session-timeout=5" in crit.detail
        assert "timestamp-invalid=5" in crit.detail

    def test_recovered_fires_when_window_empties(self):
        """As old events age out of the rolling window, the count drops
        below the disarm threshold and RECOVERED fires on tick."""
        rule = NetworkLayerSignalRule(window_sec=300, warn_count=5, critical_count=15)
        # Burst at t=100..104 — 5 events arms WARN.
        for i in range(5):
            rule.on_event(self._decrypt(100.0 + i), now_sec=100.0 + i)
        assert rule._state == Severity.WARN

        # Tick at t=410 (5 min + 6 sec after first event) — all events
        # have aged out.
        ev = rule.on_tick(now_sec=410.0)
        assert ev is not None
        assert ev.severity == Severity.RECOVERED
        assert rule._state is None

    def test_critical_to_warn_is_silent(self):
        """De-escalation CRITICAL → WARN stays silent (still alarming
        overall). RECOVERED fires only on full disarm to CLEAR."""
        rule = NetworkLayerSignalRule(window_sec=300, warn_count=5, critical_count=15)
        # 16 events — enough to land at CRITICAL.
        for i in range(16):
            rule.on_event(self._decrypt(100.0 + i), now_sec=100.0 + i)
        assert rule._state == Severity.CRITICAL

        # Tick a bit later, but only enough that ~10 events remain
        # (still above warn_count=5 but below critical's disarm
        # threshold of 12).
        # First 7 events (t=100..106) age out by t=406.
        ev = rule.on_tick(now_sec=406.0)
        # State should fall to WARN; no event emitted.
        assert ev is None
        assert rule._state == Severity.WARN

    def test_non_network_event_is_ignored(self):
        """The rule ignores non-network ConsensusEvent kinds — the
        consensus loop hands us every event without filtering."""
        rule = NetworkLayerSignalRule(window_sec=300, warn_count=5, critical_count=15)
        for i in range(10):
            ev = rule.on_event(
                ConsensusEvent(
                    kind=ConsensusEventKind.LOCAL_TIMEOUT,
                    round=100 + i, epoch=None, ts_ms=int((100.0 + i) * 1000),
                ),
                now_sec=100.0 + i,
            )
            assert ev is None
        assert rule._state is None

    def test_single_peer_storm_emits_nothing(self):
        """A single-peer storm — even one crossing critical_count — must
        produce zero alerts. Chronic single-neighbour decrypt-fail flap
        was generating constant Telegram chatter (2026-05-06) without
        any predictive value; the WARN gate now suppresses it entirely
        rather than holding at WARN with a gate-hint."""
        rule = NetworkLayerSignalRule(
            window_sec=300, warn_count=5, critical_count=15,
            warn_min_unique_peers=2, critical_min_unique_peers=3,
        )
        events = []
        for i in range(16):
            ev = rule.on_event(
                self._decrypt(100.0 + i, peer="23.83.186.216:8001"),
                now_sec=100.0 + i,
            )
            if ev is not None:
                events.append(ev)
        assert events == []
        assert rule._state is None

    def test_two_peer_burst_warns_but_holds_below_critical(self):
        """Two distinct peers pass the WARN gate (≥2) but not the
        CRITICAL gate (≥3) — fires WARN with the gate-hint, never
        escalates to CRITICAL even when count crosses critical_count."""
        rule = NetworkLayerSignalRule(
            window_sec=300, warn_count=5, critical_count=15,
            warn_min_unique_peers=2, critical_min_unique_peers=3,
        )
        peers = ["10.0.0.1:8001", "10.0.0.2:8001"]
        events = []
        for i in range(16):
            ev = rule.on_event(
                self._decrypt(100.0 + i, peer=peers[i % 2]),
                now_sec=100.0 + i,
            )
            if ev is not None:
                events.append(ev)
        severities = [e.severity for e in events]
        assert Severity.CRITICAL not in severities
        assert Severity.WARN in severities
        assert rule._state == Severity.WARN
        warn_events = [e for e in events if e.severity == Severity.WARN]
        assert "Held below CRITICAL" in warn_events[-1].detail
        assert "only 2 unique peer" in warn_events[-1].detail

    def test_warn_gate_disabled_restores_legacy_behaviour(self):
        """Setting ``warn_min_unique_peers=1`` reverts to the pre-2026-05-06
        behaviour where any single-peer burst above warn_count still
        WARNs (escape hatch for operators who'd rather see the noise)."""
        rule = NetworkLayerSignalRule(
            window_sec=300, warn_count=5, critical_count=15,
            warn_min_unique_peers=1, critical_min_unique_peers=3,
        )
        events = []
        for i in range(16):
            ev = rule.on_event(
                self._decrypt(100.0 + i, peer="23.83.186.216:8001"),
                now_sec=100.0 + i,
            )
            if ev is not None:
                events.append(ev)
        severities = [e.severity for e in events]
        assert Severity.CRITICAL not in severities
        assert Severity.WARN in severities
        assert rule._state == Severity.WARN

    def test_diverse_peer_burst_still_escalates_to_critical(self):
        """When ≥critical_min_unique_peers distinct peers participate,
        crossing critical_count still escalates as before."""
        rule = NetworkLayerSignalRule(
            window_sec=300, warn_count=5, critical_count=15,
            critical_min_unique_peers=3,
        )
        peers = ["10.0.0.1:8001", "10.0.0.2:8001", "10.0.0.3:8001"]
        events = []
        for i in range(16):
            ev = rule.on_event(
                self._decrypt(100.0 + i, peer=peers[i % len(peers)]),
                now_sec=100.0 + i,
            )
            if ev is not None:
                events.append(ev)
        severities = [e.severity for e in events]
        assert Severity.CRITICAL in severities
        crit = next(e for e in events if e.severity == Severity.CRITICAL)
        assert "Unique peers: 3" in crit.detail

    def test_hysteresis_silences_oscillation_at_warn_boundary(self):
        """Count grazing back below warn_count must NOT fire RECOVERED
        until it drops below warn_count × 0.8 — the disarm threshold."""
        rule = NetworkLayerSignalRule(window_sec=300, warn_count=5, critical_count=15)
        # 5 events arms WARN. Spread over 4 sec so the windowing can
        # selectively drop them later.
        for i in range(5):
            rule.on_event(self._decrypt(100.0 + i), now_sec=100.0 + i)
        assert rule._state == Severity.WARN

        # Tick at t=400.5: cutoff=100.5; only the event at t=100 falls
        # out of window. 4 events remain. 4 == disarm threshold (5*0.8),
        # so the rule stays armed (not strictly less than disarm).
        ev = rule.on_tick(now_sec=400.5)
        assert ev is None
        assert rule._state == Severity.WARN

        # Tick at t=401.5: cutoff=101.5; both t=100 and t=101 fall out.
        # 3 events remain — below disarm. RECOVERED fires.
        ev = rule.on_tick(now_sec=401.5)
        assert ev is not None
        assert ev.severity == Severity.RECOVERED

    def test_explicit_disarm_levels_override_factor(self):
        """Deployed config sets explicit disarm levels (~0.5 × arm)
        instead of the legacy 0.8 factor. Armed at warn=25, the rule
        must stay armed all the way down to 12 and only disarm below."""
        rule = NetworkLayerSignalRule(
            window_sec=300, warn_count=25, critical_count=50,
            warn_disarm_count=12, critical_disarm_count=25,
        )
        for i in range(25):
            rule.on_event(self._decrypt(100.0 + i), now_sec=100.0 + i)
        assert rule._state == Severity.WARN

        # Age out events until 13 remain — above disarm=12, below the
        # legacy 0.8 disarm (20). Must hold WARN silently.
        ev = rule.on_tick(now_sec=411.5)  # cutoff 111.5 → 13 left
        assert ev is None
        assert rule._state == Severity.WARN

        # 11 remain — below disarm. Immediate RECOVERED (confirm=0).
        ev = rule.on_tick(now_sec=413.5)  # cutoff 113.5 → 11 left
        assert ev is not None
        assert ev.severity == Severity.RECOVERED
        assert rule._state is None

    def test_recovery_confirmation_coalesces_dip_and_rebound(self):
        """With recovery_confirm_sec set, a dip below disarm followed by
        a rebound must NOT produce RECOVERED + re-WARN — the envelope
        stays open. RECOVERED fires only after a full quiet window."""
        rule = NetworkLayerSignalRule(
            window_sec=300, warn_count=5, critical_count=15,
            recovery_confirm_sec=600.0,
        )
        for i in range(5):
            rule.on_event(self._decrypt(100.0 + i), now_sec=100.0 + i)
        assert rule._state == Severity.WARN

        # All events age out — dip starts, but no RECOVERED yet.
        ev = rule.on_tick(now_sec=500.0)
        assert ev is None
        assert rule._state == Severity.WARN

        # Rebound within the confirm window: 5 fresh events. The rule
        # re-arms internally with NO new Telegram message (state never
        # left WARN).
        events = []
        for i in range(5):
            e = rule.on_event(self._decrypt(600.0 + i), now_sec=600.0 + i)
            if e is not None:
                events.append(e)
        assert events == []
        assert rule._state == Severity.WARN

        # Second dip; quiet from t=1000. Still silent mid-confirm.
        ev = rule.on_tick(now_sec=1000.0)
        assert ev is None
        ev = rule.on_tick(now_sec=1500.0)
        assert ev is None
        # Confirm window (600s) elapsed → exactly one RECOVERED.
        ev = rule.on_tick(now_sec=1601.0)
        assert ev is not None
        assert ev.severity == Severity.RECOVERED
        assert "quiet" in ev.detail
        assert rule._state is None

    def test_gate_fail_at_volume_holds_armed_no_recovered(self):
        """A diversity-gate failure while the count is still at/above
        disarm must HOLD the armed state, not emit RECOVERED. Observed
        2026-06-02: single-peer storm faded in/out of the gate and
        produced greens at counts 75–108."""
        rule = NetworkLayerSignalRule(
            window_sec=300, warn_count=5, critical_count=15,
            warn_min_unique_peers=2, critical_min_unique_peers=3,
        )
        # Arm CRITICAL with 3 diverse peers.
        peers = ["10.0.0.1:8001", "10.0.0.2:8001", "10.0.0.3:8001"]
        for i in range(16):
            rule.on_event(
                self._decrypt(100.0 + i, peer=peers[i % 3]), now_sec=100.0 + i
            )
        assert rule._state == Severity.CRITICAL

        # A continuous single-peer flood overlaps the diverse burst and
        # outlives it: once the diverse events age out (t>415), the
        # window holds dozens of events from ONE peer — the gate drops
        # the target to None while volume stays far above disarm.
        events = []
        for t in range(120, 460, 5):
            e = rule.on_event(
                self._decrypt(float(t), peer="23.83.186.216:8001"),
                now_sec=float(t),
            )
            if e is not None:
                events.append(e)
        # Exactly one message is allowed through: the by-design
        # "held below CRITICAL" WARN as diversity collapses. Crucially:
        # zero RECOVERED while volume is above disarm.
        assert [e.severity for e in events] == [Severity.WARN]
        assert "Held below CRITICAL" in events[0].detail
        assert rule._state in (Severity.WARN, Severity.CRITICAL)  # held

        # Only when volume itself drains does recovery begin (confirm=0
        # here → immediate).
        ev = rule.on_tick(now_sec=900.0)
        assert ev is not None
        assert ev.severity == Severity.RECOVERED


class TestProcessRestartRule:
    """systemd-InvocationID change detector. First sight silent;
    subsequent change fires WARN. Probe error soft-ignored."""

    def _snap(
        self,
        service: str,
        invocation_id: str | None = "abc",
        sub_state: str = "running",
        active_state: str = "active",
        error: str | None = None,
    ) -> InvocationSnapshot:
        return InvocationSnapshot(
            service=service,
            invocation_id=invocation_id,
            sub_state=sub_state,
            active_state=active_state,
            error=error,
        )

    def test_first_sight_is_silent(self):
        """Bootstrap on monad-ops's own startup must not page —
        first observation is always silent."""
        rule = ProcessRestartRule()
        ev = rule.on_snapshot(self._snap("monad-bft", invocation_id="aaa1"))
        assert ev is None
        assert rule._last["monad-bft"] == "aaa1"

    def test_unchanged_invocation_is_silent(self):
        """Stable InvocationID across polls = no event."""
        rule = ProcessRestartRule()
        rule.on_snapshot(self._snap("monad-bft", invocation_id="aaa1"))
        for _ in range(5):
            assert rule.on_snapshot(self._snap("monad-bft", invocation_id="aaa1")) is None

    def test_changed_invocation_fires_warn(self):
        """A different InvocationID on a subsequent poll = restart event."""
        rule = ProcessRestartRule()
        rule.on_snapshot(self._snap("monad-bft", invocation_id="aaa1"))
        ev = rule.on_snapshot(self._snap("monad-bft", invocation_id="bbb2"))
        assert ev is not None
        assert ev.severity == Severity.WARN
        assert ev.rule == "process_restart"
        assert ev.key == "process_restart:monad-bft"
        assert "monad-bft" in ev.detail
        assert "active/running" in ev.detail

    def test_probe_error_does_not_change_state(self):
        """systemctl timeout / non-zero rc must not corrupt the rule's
        last-seen map. Reusable lesson from the 2026-04-20 event-loop
        freeze that produced phantom probe-services criticals."""
        rule = ProcessRestartRule()
        rule.on_snapshot(self._snap("monad-bft", invocation_id="aaa1"))
        ev = rule.on_snapshot(self._snap(
            "monad-bft", invocation_id=None, error="timeout",
        ))
        assert ev is None
        # Original ID still recorded — the next real sample with the
        # SAME id should stay silent (not fire as if it had changed).
        ev = rule.on_snapshot(self._snap("monad-bft", invocation_id="aaa1"))
        assert ev is None

    def test_missing_invocation_field_treated_as_error(self):
        """Some systemctl edge cases (unit not loaded, very old systemd)
        return zero rc but no InvocationID line. Treat that the same
        as a probe error — no state change, no event."""
        rule = ProcessRestartRule()
        rule.on_snapshot(self._snap("monad-bft", invocation_id="aaa1"))
        ev = rule.on_snapshot(self._snap(
            "monad-bft", invocation_id=None, error=None,
        ))
        assert ev is None

    def test_multiple_services_tracked_independently(self):
        """A restart on monad-rpc must not affect monad-bft state and
        vice versa. Per-service alert keys also keep the deduping sink
        from collapsing concurrent restarts."""
        rule = ProcessRestartRule()
        rule.on_snapshot(self._snap("monad-bft", invocation_id="bft-1"))
        rule.on_snapshot(self._snap("monad-rpc", invocation_id="rpc-1"))
        # Restart only RPC.
        ev_bft = rule.on_snapshot(self._snap("monad-bft", invocation_id="bft-1"))
        ev_rpc = rule.on_snapshot(self._snap("monad-rpc", invocation_id="rpc-2"))
        assert ev_bft is None
        assert ev_rpc is not None
        assert ev_rpc.severity == Severity.WARN
        assert ev_rpc.key == "process_restart:monad-rpc"

    def test_consecutive_restarts_each_fire(self):
        """Each distinct restart in sequence must fire — flap detection
        is a downstream concern (deduping sink + cooldown), not the
        rule's job."""
        rule = ProcessRestartRule()
        rule.on_snapshot(self._snap("monad-bft", invocation_id="a"))
        ev1 = rule.on_snapshot(self._snap("monad-bft", invocation_id="b"))
        ev2 = rule.on_snapshot(self._snap("monad-bft", invocation_id="c"))
        assert ev1 is not None and ev1.severity == Severity.WARN
        assert ev2 is not None and ev2.severity == Severity.WARN


class TestCodeColor:
    """Severity-to-Foundation-colour-code mapping.

    The mapping is Foundation-facing (the 2026-03-26 colour-code
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


class TestWaltraceFloodRule:
    """v0.14.5 WAL-thread death detector. Baseline is strictly zero;
    a flood runs ~250 lines/sec, so arming is effectively instant.
    CRITICAL is time-driven (operator hasn't restarted), RECOVERED
    fires once the window drains post-restart."""

    def _ev(self, ts_sec: float) -> ConsensusEvent:
        return ConsensusEvent(
            kind=ConsensusEventKind.WALTRACE_STOPPED,
            round=0, epoch=None, ts_ms=int(ts_sec * 1000),
        )

    def test_below_threshold_stays_clear(self):
        rule = WaltraceFloodRule(window_sec=60, warn_count=10, critical_after_sec=900)
        for i in range(9):
            assert rule.on_event(self._ev(100.0 + i), now_sec=100.0 + i) is None

    def test_flood_arms_warn_once(self):
        """First 10 lines arm WARN; the next thousands stay silent."""
        rule = WaltraceFloodRule(window_sec=60, warn_count=10, critical_after_sec=900)
        events = []
        for i in range(50):
            ev = rule.on_event(self._ev(100.0 + i * 0.004), now_sec=100.0 + i * 0.004)
            if ev is not None:
                events.append(ev)
        assert len(events) == 1
        assert events[0].severity == Severity.WARN
        assert events[0].rule == "waltrace_flood"
        assert "v0.14.5" in events[0].detail
        assert "systemctl restart monad-bft" in events[0].detail
        assert rule.first_error_ts == 100.0

    def test_non_waltrace_event_is_ignored(self):
        rule = WaltraceFloodRule(window_sec=60, warn_count=10, critical_after_sec=900)
        ev = ConsensusEvent(
            kind=ConsensusEventKind.LOCAL_TIMEOUT, round=5, epoch=None, ts_ms=100_000,
        )
        assert rule.on_event(ev, now_sec=100.0) is None
        assert rule.first_error_ts is None

    def test_escalates_to_critical_when_not_restarted(self):
        """Flood still running critical_after_sec past arming -> exactly
        one CRITICAL, whichever evaluation path crosses the clock first."""
        rule = WaltraceFloodRule(window_sec=60, warn_count=10, critical_after_sec=900)
        for i in range(10):
            rule.on_event(self._ev(100.0 + i * 0.004), now_sec=100.0 + i * 0.004)
        # Keep the window non-empty as the flood continues.
        t = 100.0
        events = []
        while t < 1100.0:
            t += 5.0
            for ev in (rule.on_event(self._ev(t), now_sec=t), rule.on_tick(now_sec=t)):
                if ev is not None:
                    events.append(ev)
        assert [e.severity for e in events] == [Severity.CRITICAL]
        assert "off the whole time" in events[0].detail

    def test_escalates_via_tick_alone(self):
        """Escalation must not depend on fresh events: if lines somehow
        stop arriving but the window still holds some, the tick clock
        alone escalates. (Real floods never pause, but the tick path is
        what guarantees the clock.)"""
        rule = WaltraceFloodRule(window_sec=2000, warn_count=10, critical_after_sec=900)
        for i in range(10):
            rule.on_event(self._ev(100.0 + i), now_sec=100.0 + i)
        assert rule.on_tick(now_sec=1000.0) is None
        ev = rule.on_tick(now_sec=1010.0)
        assert ev is not None and ev.severity == Severity.CRITICAL

    def test_recovered_after_restart_drains_window(self):
        """Post-restart no fresh lines arrive; ticks drain the window
        and emit exactly one RECOVERED, resetting the envelope."""
        rule = WaltraceFloodRule(window_sec=60, warn_count=10, critical_after_sec=900)
        for i in range(10):
            rule.on_event(self._ev(100.0 + i), now_sec=100.0 + i)
        # 60s after the last line the window empties.
        assert rule.on_tick(now_sec=150.0) is None
        ev = rule.on_tick(now_sec=170.0)
        assert ev is not None and ev.severity == Severity.RECOVERED
        assert rule.first_error_ts is None
        # Further ticks are silent.
        assert rule.on_tick(now_sec=180.0) is None

    def test_second_envelope_rearms_and_recaptures_first_ts(self):
        """The bug recurs ~16-25h after a restart; a fresh flood after
        RECOVERED must arm again with a fresh first-error timestamp."""
        rule = WaltraceFloodRule(window_sec=60, warn_count=10, critical_after_sec=900)
        for i in range(10):
            rule.on_event(self._ev(100.0 + i), now_sec=100.0 + i)
        rule.on_tick(now_sec=170.0)  # RECOVERED
        events = []
        for i in range(10):
            ev = rule.on_event(self._ev(90000.0 + i), now_sec=90000.0 + i)
            if ev is not None:
                events.append(ev)
        assert [e.severity for e in events] == [Severity.WARN]
        assert rule.first_error_ts == 90000.0

    def test_critical_clock_starts_at_arm_not_first_line(self):
        """A slow trickle that arms late must not instantly escalate:
        the escalation clock runs from arming, not the first line."""
        rule = WaltraceFloodRule(window_sec=60, warn_count=10, critical_after_sec=900)
        # 10 lines spread over 50s -> arms at t=150.
        for i in range(10):
            rule.on_event(self._ev(100.0 + i * 5.55), now_sec=100.0 + i * 5.55)
        # 899s after arming: still WARN.
        rule.on_event(self._ev(1040.0), now_sec=1040.0)
        assert rule.on_tick(now_sec=1049.0) is None
