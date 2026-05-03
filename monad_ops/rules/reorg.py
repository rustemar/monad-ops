"""Pre-finalization block-id divergence detector.

Fires when the same ``block_number`` is observed twice with a *different*
``block_id`` in the ``__exec_block`` log stream from monad-execution. The
``block_id`` field is the **execution-layer hash**, distinct from the
EVM-canonical block hash returned by ``eth_getBlockByNumber``: a sample
of canonical RPC blocks shows our ``block_id`` never matches the RPC
hash, on either reorged or non-reorged blocks. The two are different
namespaces.

What we measure is therefore execution-layer re-execution at the same
height pre-finalization: HotStuff-2 speculation, not a finality
violation. Monad's pipelined finality means a reorged block_number with
depth 0–1 is expected protocol behaviour; the chain still finalizes
correctly on the canonical RPC. Operators reading "reorg" should not
infer a chain rollback.

Severity policy (post-2026-05-03 reframe — see
``wiki/decisions/reorg-severity-reframe-20260503.md``):
  * Default = INFO. A single divergence event is normal HotStuff-2
    behaviour; surface it for visibility but do NOT paint the dashboard.
  * WARN = ``cluster_threshold`` divergences within ``cluster_window_sec``.
    Clusters can correlate with chain instability or correlated validator
    drop-off and are worth a glance, but still not a CRITICAL — the
    canonical chain finalizes regardless. The journal-capture artifact
    on each fire remains the operationally interesting payload.

Memory bound: we keep the last ``track_window`` seen ``(number, id)``
pairs, not the whole history. Divergences happen close in time to the
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
        severity = Severity.WARN if is_cluster else Severity.INFO

        if is_cluster:
            cluster_note = (
                f" Cluster: {in_window} divergences in the last "
                f"{self.cluster_window_sec // 60}min — escalated to WARN."
            )
        else:
            cluster_note = ""

        return AlertEvent(
            # rule key kept as "reorg" for storage / API / frontend
            # compatibility (existing alert history, journal-capture
            # button wiring, dashboard chip filters). The user-facing
            # framing — title, detail, severity tiers — is what changed
            # in the 2026-05-03 reframe.
            rule="reorg",
            severity=severity,
            # Key includes the new block_id so a chain of divergences at
            # the same height (rare, but possible in a brief fork) each
            # produce a distinct event rather than getting collapsed by
            # the deduping sink.
            key=f"reorg:{block.block_number}:{block.block_id}",
            title="Pre-finalization block-id divergence",
            detail=(
                f"Block #{block.block_number} exec-layer id changed: "
                f"{_short(prev_id)} → {_short(block.block_id)}. "
                # "observed" (not "since start") — counter is bootstrapped
                # from sqlite on startup, so it reflects total lifetime of
                # the node, not uptime of the current process.
                f"Total divergences observed: {self.reorg_count}."
                f"{cluster_note}"
                " Execution-layer observation, not a finality violation —"
                " HotStuff-2 expected behaviour at pre-finalization depth."
            ),
        )


def _short(block_id: str) -> str:
    if len(block_id) < 14:
        return block_id
    return f"{block_id[:10]}…{block_id[-6:]}"
