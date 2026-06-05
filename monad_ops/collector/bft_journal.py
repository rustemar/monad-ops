"""Async tailer over the monad-bft journal for consensus events.

Mirror of ``collector/journal.py`` but pointed at the bft service and
parsing only consensus-state events (round advances + local timeouts).

Why a second tailer instead of folding bft into the existing one:

* **Different unit, different volume.** ``monad-execution`` emits one
  ``__exec_block`` line per block (~2/sec). ``monad-bft`` emits ~5500
  lines/sec under load (mostly ``sending keepalive packet`` DEBUG).
  A single tailer at the bft volume would starve the execution-block
  parser if either side blocks momentarily on the queue.
* **Independent failure isolation.** A journald hiccup on bft must not
  starve the exec-block reader (and vice versa). Two subprocesses,
  two supervisors in ``cli.py``, blast-radius of a single tail crash
  is the panel that depends on it.

A cheap substring pre-filter in the parser drops ~99.95% of bft lines
before any regex work happens — see ``parser/consensus.py``.

Self-healing follow (idle-respawn on a stalled ``journalctl -f``) lives
in ``tail_raw_lines`` — see ``collector/journal.py``.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

from monad_ops.collector.journal import (
    DEFAULT_IDLE_TIMEOUT_SEC,
    DEFAULT_MAX_RESPAWNS,
    DEFAULT_RESPAWN_WINDOW_SEC,
    TailError,
    tail_raw_lines,
)
from monad_ops.parser import ConsensusEvent, parse_consensus

# bft proposal lines run several KB because of the BLS signer bitvec,
# comfortably under 1 MB but well over the asyncio 64 KB default that an
# earlier epoch_probe iteration tripped silently.
_BFT_READ_LIMIT = 1_000_000


async def tail_consensus_events(
    unit: str = "monad-bft",
    lookback: str | None = None,
    follow: bool = True,
    *,
    idle_timeout_sec: float = DEFAULT_IDLE_TIMEOUT_SEC,
    max_respawns: int = DEFAULT_MAX_RESPAWNS,
    respawn_window_sec: float = DEFAULT_RESPAWN_WINDOW_SEC,
    spawn=asyncio.create_subprocess_exec,
) -> AsyncIterator[ConsensusEvent | TailError]:
    """Yield consensus events from the bft journal: round advances + local timeouts.

    Same shape as ``tail_execution_blocks`` so both tailers read the
    same in ``cli.py``.

    Modes:
      * ``follow=True, lookback=None``        — tail live from now (default).
      * ``follow=True, lookback="5 min ago"`` — emit history then tail live.
      * ``follow=False, lookback="5 min ago"`` — emit history then stop
        (used by replay/backfill, not the live service).
    """
    async for line in tail_raw_lines(
        unit,
        follow=follow,
        lookback=lookback,
        idle_timeout_sec=idle_timeout_sec,
        max_respawns=max_respawns,
        respawn_window_sec=respawn_window_sec,
        read_limit=_BFT_READ_LIMIT,
        spawn=spawn,
    ):
        if isinstance(line, TailError):
            yield line
            return
        event = parse_consensus(line)
        if event is not None:
            yield event
