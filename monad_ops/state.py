"""Shared in-memory state.

One instance is created in ``cli`` and handed to both the collector
loop and the FastAPI app. Optionally mirrors writes to an on-disk
SQLite ``Storage`` so the dashboard/API can recover recent history
after a restart and so post-event analysis (e.g. the 2026-04-20
execution stress test) can query arbitrary block ranges.
"""

from __future__ import annotations

import asyncio
import time
from collections import deque
from dataclasses import dataclass, field
from threading import Lock

from monad_ops.collector.probes import ProbeResult
from monad_ops.collector.reference_rpc import ReferenceSample
from monad_ops.parser import ConsensusEvent, ConsensusEventKind, ExecBlock
from monad_ops.rules.events import AlertEvent, code_color_for
from monad_ops.rules.reorg import ReorgRule
from monad_ops.storage import BftBaseFee, BftMinute, Storage


# Keep ~1 hour of blocks (testnet cadence ~2.5 blk/s → 9000 blocks/hr).
# Round up for bursty periods.
_BLOCK_BUFFER = 12_000
# Keep recent alert events for the dashboard's "recent activity" panel.
_ALERT_BUFFER = 200
# Per-minute consensus rollup. 6 hours @ 1 row/min = 360 rows — cheap
# in memory, comfortably covers the longest "explain that incident"
# window the dashboard surfaces today (1h chart + 5m KPI both fit).
_BFT_MINUTE_BUFFER = 360
_MINUTE_MS = 60_000


@dataclass
class Snapshot:
    """Point-in-time summary for the /api/state endpoint."""
    started_at: float
    uptime_sec: int
    blocks_seen: int
    last_block: int | None
    last_block_seen_ms: int | None
    blocks_per_sec_1m: float
    rtp_avg_1m: float
    rtp_avg_5m: float
    rtp_max_1m: float
    tx_per_sec_1m: float
    gas_per_sec_1m: float
    # Parallelism / throughput ceiling. tps_effective is the per-block
    # intrablock TPS — "how fast Monad actually ran during the busiest
    # microsecond of that block". Higher than tx_per_sec because testnet
    # blocks are thinly populated; exposes Monad's execution headroom.
    tps_effective_peak_1m: int
    tps_effective_avg_1m: int
    tps_eff_peak_block: int | None
    gas_per_sec_effective_peak_1m: int
    gas_eff_peak_block: int | None
    # Chain-integrity signal (ReorgRule). Reorg_count is always present
    # so the dashboard can show "0 reorgs over N blocks" as a clear
    # green signal, not just the absence of data.
    reorg_count: int
    # Reorgs observed in the last 24h (rolling). Distinct from the
    # lifetime ``reorg_count`` so the dashboard can show a "recent (24h)"
    # badge alongside the all-time number — single reorgs are background
    # noise on testnet, the 24h count is the operationally interesting
    # number for "should I be looking at this right now".
    recent_reorgs_24h: int
    last_reorg_number: int | None
    last_reorg_old_id: str | None
    last_reorg_new_id: str | None
    last_reorg_ts_ms: int | None
    # External reference tip — answers "is my node lagging or is the
    # whole network stalled?". None when the reference probe hasn't
    # run yet or is misconfigured.
    reference_block: int | None
    reference_checked_ms: int | None
    reference_error: str | None
    # Local tip captured at the exact moment the reference was polled.
    # Using this instead of the live `last_block` eliminates the bias
    # where a fresh local was compared against a 0–15s stale reference
    # and systematically over-reported "local ahead of reference".
    reference_local_at_sample: int | None
    current_alerts: list[dict]
    # Epoch progress. Derived from the monad-bft journal (epoch info
    # isn't advertised via any eth_* RPC method). All four are None
    # until the first successful probe; size/eta are None until we've
    # observed at least one epoch roll so we can learn typical length
    # empirically rather than hard-coding a magic number.
    epoch_number: int | None
    epoch_blocks_in: int | None       # blocks produced in this epoch so far
    epoch_typical_length: int | None  # median of closed-epoch sizes seen
    epoch_eta_sec: float | None       # projected seconds until next epoch
    # Consensus / validator-timeout health (Foundation tracks <3% as
    # the chain-wide target, see wiki/narratives/stress-test-april-20.md).
    # All four are 0 until the bft tailer has filled at least one
    # complete minute bucket.
    validator_timeout_pct_5m: float    # rounds_tc / rounds_total over last 5m
    local_timeout_per_min_5m: float    # this node's pacemaker fires per minute
    bft_rounds_observed_5m: int        # round_advance count in the same window
    bft_local_timeouts_5m: int         # convenience: same window absolute count


class State:
    def __init__(self, storage: Storage | None = None) -> None:
        self._blocks: deque[ExecBlock] = deque(maxlen=_BLOCK_BUFFER)
        self._alerts: deque[AlertEvent] = deque(maxlen=_ALERT_BUFFER)
        # Parallel deque of wall-clock timestamps (unix seconds) for each
        # alert. Kept separate from AlertEvent (which models "what a rule
        # emitted" and has no notion of observation time) so the same
        # event shape is reusable in tests/rules without a ts field.
        # Always same length as `_alerts` thanks to the single lock.
        self._alert_ts: deque[float] = deque(maxlen=_ALERT_BUFFER)
        self._probes: dict[str, ProbeResult] = {}
        self._probes_ran_at: float | None = None
        self._started_at = time.time()
        self._blocks_seen_total = 0
        self._storage = storage
        # Observable rule handles (counters/details read by snapshot()).
        self._reorg: ReorgRule | None = None
        # Latest reference-RPC sample; updated by the reference-poll loop
        # in cli.py, read by the dashboard via snapshot().
        self._reference: ReferenceSample | None = None
        # Observed epoch → (first_seq_seen, last_seq_seen). Populated by
        # the epoch probe. We learn typical epoch length empirically so
        # no magic constant has to track upstream chain config changes.
        self._epochs: dict[int, tuple[int, int]] = {}
        # bft consensus accumulator. Closed minutes live in the deque;
        # the in-progress minute lives in `_bft_current` and gets flushed
        # to both the deque and storage on every minute roll.
        # Snapshot reads sum across `_bft_minutes` for window stats and
        # adds the in-progress bucket so the live dashboard isn't a
        # minute behind.
        self._bft_minutes: deque[BftMinute] = deque(maxlen=_BFT_MINUTE_BUFFER)
        self._bft_current_ts: int | None = None
        self._bft_current_total = 0
        self._bft_current_tc = 0
        self._bft_current_local = 0
        # Pending writes accumulator. Drained by the cli.py
        # bft_flush_loop on a 1s cadence so storage cost scales with
        # flush interval, not event rate. Closed minutes land here on
        # rollover; base-fee samples land here on every PROPOSAL event.
        # In-progress minute is NOT pre-buffered — drain assembles a
        # fresh BftMinute snapshot at flush time.
        self._bft_pending_minutes: list[BftMinute] = []
        self._bft_pending_base_fee: list[BftBaseFee] = []
        self._lock = Lock()  # cheap — collector and FastAPI share loop but we're defensive

    def attach_reorg_rule(self, rule: ReorgRule) -> None:
        """Register the reorg rule so snapshot() can read its counters.

        Counters live on the rule (not re-computed here) so the rule is
        the single source of truth — no double-bookkeeping.
        """
        self._reorg = rule

    def set_reference(self, sample: ReferenceSample) -> None:
        """Store the latest external-RPC sample for the /api/state view."""
        with self._lock:
            self._reference = sample

    def observe_epoch(self, epoch: int, seq_num: int) -> None:
        """Record an (epoch, seq_num) observation from the bft probe.

        Expands the known range for this epoch; the typical-length
        calculation below uses any epoch that is already bracketed by
        a newer epoch — i.e. fully-closed epochs — as a sample for
        the median.
        """
        if epoch <= 0 or seq_num <= 0:
            return
        with self._lock:
            prev = self._epochs.get(epoch)
            if prev is None:
                self._epochs[epoch] = (seq_num, seq_num)
            else:
                first, last = prev
                self._epochs[epoch] = (min(first, seq_num), max(last, seq_num))
            # Cap memory: an epoch map larger than a few hundred entries
            # means something is very wrong (a year of testnet history).
            if len(self._epochs) > 500:
                oldest = min(self._epochs)
                self._epochs.pop(oldest, None)

    def _bft_window_summary(self, window_sec: int) -> tuple[float, float, int, int]:
        """Aggregate bft counters across the last ``window_sec`` seconds.

        Includes the in-progress bucket so the live dashboard isn't
        always lagging by up to one minute. Returns
        ``(timeout_pct, local_per_min, rounds_total, local_timeouts)``;
        zeros if no buckets cover the window yet (cold start).

        Window is expressed in wall-clock seconds; cutoff is computed
        against ``time.time()`` rather than the latest event so silence
        on the bft tailer doesn't masquerade as a moving baseline.
        """
        cutoff_ms = int(time.time() * 1000) - window_sec * 1000
        with self._lock:
            buckets = [m for m in self._bft_minutes if m.ts_minute >= cutoff_ms]
            cur_ts = self._bft_current_ts
            cur_total = self._bft_current_total
            cur_tc = self._bft_current_tc
            cur_local = self._bft_current_local
        rounds_total = sum(m.rounds_total for m in buckets)
        rounds_tc = sum(m.rounds_tc for m in buckets)
        local_timeouts = sum(m.local_timeouts for m in buckets)
        if cur_ts is not None and cur_ts >= cutoff_ms:
            rounds_total += cur_total
            rounds_tc += cur_tc
            local_timeouts += cur_local
        n_minutes = len(buckets) + (1 if cur_ts is not None else 0)
        timeout_pct = round(rounds_tc / rounds_total * 100, 2) if rounds_total else 0.0
        local_per_min = round(local_timeouts / n_minutes, 2) if n_minutes else 0.0
        return timeout_pct, local_per_min, rounds_total, local_timeouts

    def _epoch_progress(self) -> tuple[int | None, int | None, int | None, float | None]:
        """Snapshot helper: (epoch, blocks_in, typical_length, eta_sec).

        ``typical_length`` is the median size of *fully-bracketed* epochs:
        an epoch X counts only when both (X-1) and (X+1) are also in our
        map. The bracket proves we observed the boundary on both sides,
        so first/last seq for X are the real epoch limits — not the
        edges of our journal scan window. Without this filter, the
        oldest epoch in the in-memory map is systematically partial
        (scan_epoch_history starts mid-epoch X-1) and skews the median
        downward — bug observed 2026-04-26 with typical=24K vs real ~50K.
        Returns ``typical=None`` until at least one bracketed epoch
        exists; the dashboard then renders the "epoch length unknown"
        placeholder rather than a misleading bar.

        ``eta_sec`` is a linear projection off `blocks_per_sec_1m`,
        returned only when we have a typical_length to compare against.
        """
        with self._lock:
            if not self._epochs:
                return None, None, None, None
            current_epoch = max(self._epochs)
            first, last = self._epochs[current_epoch]
            blocks_in = last - first + 1
            epochs_set = set(self._epochs)
            bracketed_sizes = [
                (l - f + 1)
                for ep, (f, l) in self._epochs.items()
                if (ep - 1) in epochs_set and (ep + 1) in epochs_set
            ]
        typical = None
        if bracketed_sizes:
            bracketed_sizes.sort()
            typical = bracketed_sizes[len(bracketed_sizes) // 2]
        return current_epoch, blocks_in, typical, None  # eta filled at snapshot time

    @property
    def storage(self) -> Storage | None:
        return self._storage

    def bootstrap_from_storage(self, limit: int) -> int:
        """Load the most-recent ``limit`` blocks from storage into memory.

        Intended to be called once at startup before the collector loop
        begins. Returns the number of blocks loaded.
        """
        if self._storage is None or limit <= 0:
            return 0
        loaded = self._storage.load_recent_blocks(limit=limit)
        with self._lock:
            # blocks are returned in ascending order; append preserves that.
            for b in loaded:
                self._blocks.append(b)
            # Don't inflate blocks_seen_total with pre-existing rows — that
            # counter is scoped to "since process start" for an honest uptime
            # metric.
        return len(loaded)

    def bootstrap_bft_from_storage(self, limit: int) -> int:
        """Reload the most-recent ``limit`` minute-buckets from storage.

        Without this, every restart resets the validator-timeout %
        snapshot to zero for the first 5 minutes after boot. Loading
        the prior buckets back into the deque gives the dashboard an
        immediate accurate window — same spirit as
        ``bootstrap_from_storage`` for blocks.
        """
        if self._storage is None or limit <= 0:
            return 0
        loaded = self._storage.load_recent_bft_minutes(limit=limit)
        with self._lock:
            for m in loaded:
                self._bft_minutes.append(m)
        return len(loaded)

    def bootstrap_alerts_from_storage(self, limit: int) -> int:
        """Load the most-recent ``limit`` alerts from storage into memory.

        Without this, every service restart wipes the "recent alerts"
        panel on the dashboard — the user sees an empty card even though
        real events happened minutes earlier. Same spirit as
        ``bootstrap_from_storage`` for blocks.
        """
        if self._storage is None or limit <= 0:
            return 0
        stored = self._storage.load_recent_alerts(limit=limit)
        # StoredAlert and AlertEvent share the (rule, severity, key,
        # title, detail) shape so we rehydrate by value. Extra fields on
        # StoredAlert (id, ts) don't matter for the in-memory buffer.
        with self._lock:
            for sa in stored:
                self._alerts.append(AlertEvent(
                    rule=sa.rule,
                    severity=sa.severity,
                    key=sa.key,
                    title=sa.title,
                    detail=sa.detail,
                ))
                self._alert_ts.append(sa.ts)
        return len(stored)

    # -- writes (from collector) -------------------------------------------
    def add_block(self, block: ExecBlock) -> None:
        """Sync: in-memory append + sqlite write inline.

        Retained for tests and any consumer that isn't in an event loop.
        In the async collector loop, prefer ``add_block_async`` so the
        sqlite INSERT doesn't block the loop under load — at 2.5–10
        blocks/sec plus WAL checkpoint pressure during stress, each
        inline write was adding enough cumulative latency that
        /api/state calls occasionally timed out in the mid-afternoon
        2026-04-20 Telegram flood.
        """
        with self._lock:
            self._blocks.append(block)
            self._blocks_seen_total += 1
        if self._storage is not None:
            self._storage.write_block(block)

    async def add_block_async(self, block: ExecBlock) -> None:
        """Async-safe: in-memory under lock (fast), sqlite write in
        a worker thread so the event loop stays responsive."""
        with self._lock:
            self._blocks.append(block)
            self._blocks_seen_total += 1
        if self._storage is not None:
            await asyncio.to_thread(self._storage.write_block, block)

    def add_alert(self, alert: AlertEvent) -> None:
        ts = time.time()
        with self._lock:
            self._alerts.append(alert)
            self._alert_ts.append(ts)
        if self._storage is not None:
            # Pass the same ts we stored in-memory so /api/alerts and
            # /api/alerts/history agree to the millisecond.
            self._storage.write_alert(alert, ts=ts)

    async def add_alert_async(self, alert: AlertEvent) -> None:
        """Async-safe counterpart to add_alert — sqlite write off the loop."""
        ts = time.time()
        with self._lock:
            self._alerts.append(alert)
            self._alert_ts.append(ts)
        if self._storage is not None:
            await asyncio.to_thread(self._storage.write_alert, alert, ts)

    def add_consensus_event(self, event: ConsensusEvent) -> None:
        """Tally one bft consensus event into the current minute bucket.

        Synchronous, no I/O — pure in-memory bookkeeping. Pending
        writes (closed minutes, base-fee samples) accumulate in
        ``_bft_pending_*`` lists drained by the bft_flush_loop in
        cli.py on a 1-second cadence (iter-20 fix for write
        amplification — see ``Storage.bulk_write_bft``).

        Falls back to wall-clock now() if the event has no parsed ts —
        the parser uses 0 as a soft-failure sentinel, and putting those
        events into the 1970 bucket would silently corrupt the rollup.
        """
        ts_ms = event.ts_ms if event.ts_ms > 0 else int(time.time() * 1000)
        if event.kind is ConsensusEventKind.PROPOSAL:
            if event.block_seq is None or event.base_fee is None:
                return
            with self._lock:
                self._bft_pending_base_fee.append(BftBaseFee(
                    block_seq=event.block_seq,
                    ts_ms=ts_ms,
                    base_fee_wei=event.base_fee,
                ))
            return
        bucket = (ts_ms // _MINUTE_MS) * _MINUTE_MS
        with self._lock:
            if self._bft_current_ts is None:
                self._bft_current_ts = bucket
            elif bucket != self._bft_current_ts:
                # Minute rolled over — close the previous bucket. The
                # closed bucket lands in BOTH the in-memory deque (for
                # snapshot reads) and the pending-flush list (for
                # storage persistence on the next flush tick).
                closed = BftMinute(
                    ts_minute=self._bft_current_ts,
                    rounds_total=self._bft_current_total,
                    rounds_tc=self._bft_current_tc,
                    local_timeouts=self._bft_current_local,
                )
                self._bft_minutes.append(closed)
                self._bft_pending_minutes.append(closed)
                self._bft_current_ts = bucket
                self._bft_current_total = 0
                self._bft_current_tc = 0
                self._bft_current_local = 0
            if event.kind is ConsensusEventKind.ROUND_ADVANCE_QC:
                self._bft_current_total += 1
            elif event.kind is ConsensusEventKind.ROUND_ADVANCE_TC:
                self._bft_current_total += 1
                self._bft_current_tc += 1
            elif event.kind is ConsensusEventKind.LOCAL_TIMEOUT:
                self._bft_current_local += 1

    async def add_consensus_event_async(self, event: ConsensusEvent) -> None:
        """Async wrapper. Sync now — no I/O — kept for shape symmetry
        with ``add_block_async`` and so callers don't have to know
        whether the underlying tally is sync or async.

        Storage persistence happens on the bft_flush_loop cadence via
        ``drain_bft_pending`` + ``Storage.bulk_write_bft``.
        """
        self.add_consensus_event(event)

    def drain_bft_pending(self) -> tuple[list[BftMinute], list[BftBaseFee], BftMinute | None]:
        """Snapshot pending closed-minute writes, base-fee samples,
        and the in-progress minute. Called by the bft_flush_loop;
        consumer persists all three in one transaction via
        ``Storage.bulk_write_bft``.

        Closed minutes + base-fee samples are CONSUMED (cleared) on
        drain — the buffer fills again from new events. The in-progress
        minute is just a snapshot; it stays in-memory and will be
        re-snapshotted on the next drain. REPLACE semantics in
        ``bft_minute`` upsert keep this idempotent across many flushes
        of the same in-progress bucket.
        """
        with self._lock:
            closed = self._bft_pending_minutes[:]
            self._bft_pending_minutes.clear()
            base_fees = self._bft_pending_base_fee[:]
            self._bft_pending_base_fee.clear()
            in_progress = None
            if self._bft_current_ts is not None:
                in_progress = BftMinute(
                    ts_minute=self._bft_current_ts,
                    rounds_total=self._bft_current_total,
                    rounds_tc=self._bft_current_tc,
                    local_timeouts=self._bft_current_local,
                )
        return closed, base_fees, in_progress

    def set_probes(self, results: list[ProbeResult]) -> None:
        with self._lock:
            self._probes = {r.name: r for r in results}
            self._probes_ran_at = time.time()

    def probes(self) -> tuple[list[ProbeResult], float | None]:
        with self._lock:
            return list(self._probes.values()), self._probes_ran_at

    # -- reads (from API) --------------------------------------------------
    def recent_blocks(self, limit: int = 500) -> list[ExecBlock]:
        with self._lock:
            return list(self._blocks)[-limit:]

    def recent_alerts(self, limit: int = 50) -> list[AlertEvent]:
        with self._lock:
            return list(self._alerts)[-limit:]

    def recent_alerts_with_ts(self, limit: int = 50) -> list[tuple[AlertEvent, float]]:
        """Same as ``recent_alerts`` but each event paired with its wall-
        clock observation time (unix seconds). Separate method so tests
        that only care about event identity can keep using the simpler
        shape."""
        with self._lock:
            pairs = list(zip(self._alerts, self._alert_ts, strict=True))
        return pairs[-limit:]

    def snapshot(self) -> Snapshot:
        with self._lock:
            blocks = list(self._blocks)

        last = blocks[-1] if blocks else None
        last_seen_ms = last.timestamp_ms if last else None

        def _window(sec: int) -> list[ExecBlock]:
            if not blocks:
                return []
            cutoff_ms = blocks[-1].timestamp_ms - sec * 1000
            # blocks are appended monotonically → simple tail scan
            out = []
            for b in reversed(blocks):
                if b.timestamp_ms < cutoff_ms:
                    break
                out.append(b)
            return out

        w1m = _window(60)
        w5m = _window(300)

        bps_1m = len(w1m) / 60.0 if w1m else 0.0
        rtp_1m = _avg((b.retry_pct for b in w1m)) if w1m else 0.0
        rtp_5m = _avg((b.retry_pct for b in w5m)) if w5m else 0.0
        rtp_max_1m = max((b.retry_pct for b in w1m), default=0.0)
        tx_1m = sum(b.tx_count for b in w1m) / 60.0 if w1m else 0.0
        gas_1m = sum(b.gas_used for b in w1m) / 60.0 if w1m else 0.0

        # Parallelism ceiling: peak intrablock effective TPS over the
        # last minute. Avg is tx-weighted — a 50 000-tps block with 20
        # tx and a 1 000-tps block with 2 tx should not be averaged 1:1.
        tps_eff_peak_block = max(w1m, key=lambda b: b.tps_effective, default=None) if w1m else None
        tps_eff_peak = tps_eff_peak_block.tps_effective if tps_eff_peak_block else 0
        if w1m:
            total_tx = sum(b.tx_count for b in w1m)
            if total_tx > 0:
                tps_eff_avg = sum(b.tps_effective * b.tx_count for b in w1m) / total_tx
            else:
                tps_eff_avg = 0.0
        else:
            tps_eff_avg = 0.0
        # monad-execution emits `gpse` in **mega-gas/sec** (verified via
        # arithmetic on a sample block: gas=450276, total_us=1232 →
        # 365 Mgas/s, matches the logged gps=365). Multiply by 1e6 here
        # so the snapshot carries an absolute gas/sec number — keeps the
        # unit consistent with `tps_effective` (absolute tx/sec) and
        # lets the UI format compactly (3211 Mgas/s → "3.2B gas/sec").
        # Caught by iter-5 audit §A1.
        gas_eff_peak_block = max(w1m, key=lambda b: b.gas_per_sec_effective, default=None) if w1m else None
        gas_eff_peak = (gas_eff_peak_block.gas_per_sec_effective if gas_eff_peak_block else 0) * 1_000_000

        with self._lock:
            alerts_tail = list(zip(self._alerts, self._alert_ts, strict=True))[-10:]
        current_alerts = [
            {
                "rule": a.rule,
                "severity": a.severity.value,
                "code_color": code_color_for(a.severity).value,
                "title": a.title,
                "detail": a.detail,
                "ts_ms": int(ts * 1000),
            }
            for a, ts in alerts_tail
        ]

        reorg = self._reorg
        reorg_count = reorg.reorg_count if reorg else 0
        last_reorg_number = reorg.last_reorg_number if reorg else None
        last_reorg_old_id = reorg.last_reorg_old_id if reorg else None
        last_reorg_new_id = reorg.last_reorg_new_id if reorg else None
        last_reorg_ts_ms = reorg.last_reorg_ts_ms if reorg else None
        # 24h rolling reorg count. Sourced from storage so it's correct
        # across restarts; falls back to 0 when persistence is disabled.
        if self._storage is not None:
            recent_reorgs_24h = self._storage.count_reorgs_since(
                time.time() - 24 * 3600
            )
        else:
            recent_reorgs_24h = 0

        ref = self._reference
        reference_block = ref.block_number if ref else None
        reference_checked_ms = ref.checked_ms if ref else None
        reference_error = ref.error if ref else None
        reference_local_at_sample = ref.local_at_sample if ref else None

        # Epoch progress. Derived in a helper so the lock scope stays
        # small; ETA is projected here using the same bps_1m we just
        # computed for the rest of the throughput numbers.
        epoch_num, epoch_blocks_in, epoch_typical, _ = self._epoch_progress()
        epoch_eta_sec: float | None = None
        if (epoch_typical is not None and epoch_blocks_in is not None
                and bps_1m > 0.1):
            remaining = epoch_typical - epoch_blocks_in
            if remaining > 0:
                epoch_eta_sec = remaining / bps_1m

        # Consensus / validator-timeout health over the same 5m window
        # the dashboard uses for retry_pct trends. Foundation's headline
        # KPI is chain-wide TC %; we expose local pacemaker fires
        # alongside as the operator-side complement.
        timeout_pct, local_per_min, rounds_5m, local_5m = self._bft_window_summary(300)

        return Snapshot(
            started_at=self._started_at,
            uptime_sec=int(time.time() - self._started_at),
            blocks_seen=self._blocks_seen_total,
            last_block=last.block_number if last else None,
            last_block_seen_ms=last_seen_ms,
            blocks_per_sec_1m=round(bps_1m, 2),
            rtp_avg_1m=round(rtp_1m, 2),
            rtp_avg_5m=round(rtp_5m, 2),
            rtp_max_1m=round(rtp_max_1m, 2),
            tx_per_sec_1m=round(tx_1m, 2),
            gas_per_sec_1m=round(gas_1m, 0),
            tps_effective_peak_1m=int(tps_eff_peak),
            tps_effective_avg_1m=int(tps_eff_avg),
            tps_eff_peak_block=tps_eff_peak_block.block_number if tps_eff_peak_block else None,
            gas_per_sec_effective_peak_1m=int(gas_eff_peak),
            gas_eff_peak_block=gas_eff_peak_block.block_number if gas_eff_peak_block else None,
            reorg_count=reorg_count,
            recent_reorgs_24h=recent_reorgs_24h,
            last_reorg_number=last_reorg_number,
            last_reorg_old_id=last_reorg_old_id,
            last_reorg_new_id=last_reorg_new_id,
            last_reorg_ts_ms=last_reorg_ts_ms,
            reference_block=reference_block,
            reference_checked_ms=reference_checked_ms,
            reference_error=reference_error,
            reference_local_at_sample=reference_local_at_sample,
            current_alerts=current_alerts,
            epoch_number=epoch_num,
            epoch_blocks_in=epoch_blocks_in,
            epoch_typical_length=epoch_typical,
            epoch_eta_sec=epoch_eta_sec,
            validator_timeout_pct_5m=timeout_pct,
            local_timeout_per_min_5m=local_per_min,
            bft_rounds_observed_5m=rounds_5m,
            bft_local_timeouts_5m=local_5m,
        )


def _avg(xs):
    xs = list(xs)
    return sum(xs) / len(xs) if xs else 0.0
