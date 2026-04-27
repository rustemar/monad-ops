"""Reorg detector.

Fires when the same ``block_number`` is observed twice with a
*different* ``block_id``. Monad uses HotStuff-2 consensus which provides
optimistic responsiveness + pipelined finality, so single-block reorgs
on testnet are an expected background event under load — most chains see
a small steady rate that does not require operator action.

Severity policy:
  * Default = WARN. A lone reorg is a chain observation, not an incident
    the operator needs to wake up for. Painting the dashboard CRITICAL
    on every isolated reorg overloads the colour and means the header
    pill stays red on a healthy node.
  * CRITICAL = ``cluster_threshold`` reorgs within ``cluster_window_sec``.
    Clusters are the operationally interesting case (chain instability,
    correlated validator drop-off, local divergence) and warrant the
    stronger signal.

Memory bound: we keep the last ``track_window`` seen ``(number, id)``
pairs, not the whole history. Reorgs happen close in time to the
original observation or not at all; a deep-fork detector would need
parent pointers which we do not currently parse.
"""

from __future__ import annotations

from collections import OrderedDict, deque
from dataclasses import dataclass, field

from monad_ops.parser import ExecBlock
from monad_ops.rules.events import AlertEvent, Severity


@dataclass(slots=True)
class ReorgRule:
    track_window: int = 2000
    # Cluster escalation. Default 3 reorgs in 30 minutes promotes a
    # WARN-tier reorg to CRITICAL. Tuned for testnet's observed baseline:
    # the 2026-04-19 retrospective post saw 23 reorgs over multi-day
    # windows in 5 distinct clusters — single events were background, the
    # clusters were the actually-notable signal.
    cluster_window_sec: int = 30 * 60
    cluster_threshold: int = 3

    # Maps block_number -> first-seen block_id. OrderedDict so we can evict
    # the oldest entries in O(1) when we exceed track_window.
    _seen: "OrderedDict[int, str]" = field(default_factory=OrderedDict)
    # Wall-clock-ish timestamps (seconds) of reorgs inside the cluster
    # window. Bounded by cluster_threshold * 4 so a long-lived process
    # under sustained reorg load doesn't accumulate state forever; the
    # window-prune in on_block keeps it correct regardless.
    _recent_ts: "deque[float]" = field(default_factory=lambda: deque(maxlen=64))
    # Observability for the /api/state integrity panel.
    reorg_count: int = 0
    last_reorg_number: int | None = None
    last_reorg_old_id: str | None = None
    last_reorg_new_id: str | None = None
    last_reorg_ts_ms: int | None = None

    def bootstrap(
        self,
        *,
        count: int,
        last_block_number: int | None = None,
        last_old_id: str | None = None,
        last_new_id: str | None = None,
        last_ts_ms: int | None = None,
        recent_ts_seconds: list[float] | None = None,
    ) -> None:
        """Seed counters from persistent storage after a process restart.

        Called once at startup with values extracted from the sqlite
        `alerts` table. The `_seen` window is NOT rehydrated — that's a
        separate (more expensive) concern, and the dedup path inside
        `on_block` (same id → silent) handles the most common risk of
        double-firing on journal replay.

        ``recent_ts_seconds`` rehydrates the cluster-detection window so
        a restart in the middle of a cluster doesn't drop the next reorg
        back to WARN — the operator sees a coherent severity progression
        across restarts.
        """
        self.reorg_count = int(count)
        if last_block_number is not None:
            self.last_reorg_number = int(last_block_number)
        if last_old_id is not None:
            self.last_reorg_old_id = last_old_id
        if last_new_id is not None:
            self.last_reorg_new_id = last_new_id
        if last_ts_ms is not None:
            self.last_reorg_ts_ms = int(last_ts_ms)
        if recent_ts_seconds:
            self._recent_ts.clear()
            self._recent_ts.extend(sorted(recent_ts_seconds))

    def on_block(self, block: ExecBlock) -> AlertEvent | None:
        prev_id = self._seen.get(block.block_number)
        if prev_id is None:
            self._seen[block.block_number] = block.block_id
            if len(self._seen) > self.track_window:
                # OrderedDict.popitem(last=False) evicts oldest.
                self._seen.popitem(last=False)
            return None

        if prev_id == block.block_id:
            # Duplicate delivery (e.g. journal replay after restart). Not
            # a reorg; silently ignore to keep this rule quiet on routine
            # bootstrap paths.
            return None

        # Divergence — reorg or local corruption.
        self.reorg_count += 1
        self.last_reorg_number = block.block_number
        self.last_reorg_old_id = prev_id
        self.last_reorg_new_id = block.block_id
        self.last_reorg_ts_ms = block.timestamp_ms
        # Update stored id so we only fire once per (number, new_id) pair.
        self._seen[block.block_number] = block.block_id

        # Cluster check: prune old entries, append this reorg, decide
        # severity off the resulting count. Using block.timestamp_ms keeps
        # the cluster boundary deterministic in tests and consistent with
        # the timestamps the user reads in alert detail.
        ts_sec = block.timestamp_ms / 1000.0
        cutoff = ts_sec - self.cluster_window_sec
        while self._recent_ts and self._recent_ts[0] < cutoff:
            self._recent_ts.popleft()
        self._recent_ts.append(ts_sec)
        in_window = len(self._recent_ts)
        is_cluster = in_window >= self.cluster_threshold
        severity = Severity.CRITICAL if is_cluster else Severity.WARN

        if is_cluster:
            cluster_note = (
                f" Cluster: {in_window} reorgs in the last "
                f"{self.cluster_window_sec // 60}min — escalated to critical."
            )
        else:
            cluster_note = ""

        return AlertEvent(
            rule="reorg",
            severity=severity,
            # Key includes the new block_id so a chain of reorgs at the
            # same height (rare, but possible in a brief fork) each
            # produce a distinct event rather than getting collapsed by
            # the deduping sink.
            key=f"reorg:{block.block_number}:{block.block_id}",
            title="Chain reorg detected",
            detail=(
                f"Block #{block.block_number} id changed: "
                f"{_short(prev_id)} → {_short(block.block_id)}. "
                # "observed" (not "since start") — counter is bootstrapped
                # from sqlite on startup, so it reflects total lifetime of
                # the node, not uptime of the current process.
                f"Total reorgs observed: {self.reorg_count}."
                f"{cluster_note}"
            ),
        )


def _short(block_id: str) -> str:
    if len(block_id) < 14:
        return block_id
    return f"{block_id[:10]}…{block_id[-6:]}"
