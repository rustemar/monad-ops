"""Reorg detector.

Fires when the same ``block_number`` is observed twice with a
*different* ``block_id``. Monad uses HotStuff-2 consensus which provides
optimistic responsiveness + pipelined finality, so a reorg on testnet
should be very rare — this rule treats any occurrence as CRITICAL.

Why bother running a detector for a ~never event:
  * Local-state divergence. If execution replays a block and arrives at
    a different id (bug, hardware bit-flip, partial disk corruption),
    this catches it before the operator notices through second-order
    effects (stuck peers, RPC inconsistency).
  * Invariant proof. The dashboard can show "N blocks observed, 0
    reorgs" as a positive signal — reassurance for operators during
    a stress test.
  * Future cheap-extension point. Same rule file is the natural home
    for "same block_number AND same block_id but different receipts"
    if we ever enrich that deeply.

Memory bound: we keep the last ``track_window`` seen ``(number, id)``
pairs, not the whole history. Reorgs happen close in time to the
original observation or not at all; a deep-fork detector would need
parent pointers which we do not currently parse.
"""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass, field

from monad_ops.parser import ExecBlock
from monad_ops.rules.events import AlertEvent, Severity


@dataclass(slots=True)
class ReorgRule:
    track_window: int = 2000

    # Maps block_number -> first-seen block_id. OrderedDict so we can evict
    # the oldest entries in O(1) when we exceed track_window.
    _seen: "OrderedDict[int, str]" = field(default_factory=OrderedDict)
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
    ) -> None:
        """Seed counters from persistent storage after a process restart.

        Called once at startup with values extracted from the sqlite
        `alerts` table. The `_seen` window is NOT rehydrated — that's a
        separate (more expensive) concern, and the dedup path inside
        `on_block` (same id → silent) handles the most common risk of
        double-firing on journal replay.
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

        return AlertEvent(
            rule="reorg",
            severity=Severity.CRITICAL,
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
            ),
        )


def _short(block_id: str) -> str:
    if len(block_id) < 14:
        return block_id
    return f"{block_id[:10]}…{block_id[-6:]}"
