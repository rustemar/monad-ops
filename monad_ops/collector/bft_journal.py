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
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

from monad_ops.collector.journal import TailError
from monad_ops.parser import ConsensusEvent, parse_consensus


async def tail_consensus_events(
    unit: str = "monad-bft",
    lookback: str | None = None,
    follow: bool = True,
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
    cmd = ["journalctl", "-u", unit, "-o", "cat", "--no-pager"]
    if follow:
        cmd.append("-f")
    if lookback is None:
        if follow:
            cmd += ["--lines=0"]
    else:
        cmd += ["--since", lookback]

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        # 1 MB StreamReader buffer — bft proposal lines run several KB
        # because of the BLS signer bitvec, comfortably under 1 MB but
        # well over the asyncio 64 KB default that an earlier
        # epoch_probe iteration tripped silently.
        limit=1_000_000,
    )

    try:
        assert proc.stdout is not None
        while True:
            raw = await proc.stdout.readline()
            if not raw:
                code = await proc.wait()
                stderr = (
                    (await proc.stderr.read()).decode(errors="replace")
                    if proc.stderr else ""
                )
                yield TailError(
                    message=f"journalctl exited rc={code}: {stderr.strip()[:500]}",
                    graceful=code < 0,
                )
                return
            line = raw.decode("utf-8", errors="replace").rstrip("\n")
            event = parse_consensus(line)
            if event is not None:
                yield event
    finally:
        if proc.returncode is None:
            proc.terminate()
            try:
                await asyncio.wait_for(proc.wait(), timeout=3)
            except asyncio.TimeoutError:
                proc.kill()
