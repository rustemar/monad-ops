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
from monad_ops.parser import ExecBlock
from monad_ops.rules.events import AlertEvent, code_color_for
from monad_ops.rules.reorg import ReorgRule
from monad_ops.storage import Storage


# Keep ~1 hour of blocks (testnet cadence ~2.5 blk/s → 9000 blocks/hr).
# Round up for bursty periods.
_BLOCK_BUFFER = 12_000
# Keep recent alert events for the dashboard's "recent activity" panel.
_ALERT_BUFFER = 200


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

    def _epoch_progress(self) -> tuple[int | None, int | None, int | None, float | None]:
        """Snapshot helper: (epoch, blocks_in, typical_length, eta_sec).

        ``typical_length`` is the median size of previously-closed epochs
        (i.e. any epoch whose last_seq is strictly below the current
        epoch's range — proving it was rolled over, not merely paused).
        ``eta_sec`` is a linear projection off `blocks_per_sec_1m`,
        returned only when we have a typical_length to compare against.
        """
        with self._lock:
            if not self._epochs:
                return None, None, None, None
            current_epoch = max(self._epochs)
            first, last = self._epochs[current_epoch]
            blocks_in = last - first + 1
            closed_sizes = [
                (l - f + 1)
                for ep, (f, l) in self._epochs.items()
                if ep != current_epoch
            ]
        typical = None
        if closed_sizes:
            closed_sizes.sort()
            typical = closed_sizes[len(closed_sizes) // 2]
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
        )


def _avg(xs):
    xs = list(xs)
    return sum(xs) / len(xs) if xs else 0.0
