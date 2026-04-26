"""Sidecar probe for the current Monad epoch.

The execution log (monad-execution) doesn't name epochs; that signal
lives in monad-bft journal entries like:

  "message":"generated expected system calls",
  "block_seq_num":"26633357","block_epoch":"533"

Rather than add a second full tail subprocess, we pull the last ~30s of
monad-bft output every ``probe_interval_sec`` (default 15s) and scan
for the most recent epoch/seq pair. Cheap (short output, <50ms), rare
(4/min), and keeps the probe path completely separate from the hot
execution-journal tailer so a journald hiccup on one doesn't starve
the other.

Empirical epoch length on Monad testnet as of 2026-04-20: ~50,042
blocks per epoch (observed epoch 532). We don't hardcode that number —
the ``EpochTracker`` in ``state.py`` learns typical size from closed
epochs it has seen, so the dashboard adapts if the chain changes its
rotation cadence.
"""

from __future__ import annotations

import asyncio
import re
import time
from dataclasses import dataclass


# Regex walks a single JSON log line — cheap, no JSON parse. Pins
# block_seq_num and block_epoch as adjacent fields, which is the
# layout monad-bft emits for "generated expected system calls" and
# "proposal message" lines.
_EPOCH_RX = re.compile(
    r'"block_seq_num":"(\d+)"[^}]*"block_epoch":"(\d+)"'
)


@dataclass(frozen=True, slots=True)
class EpochSample:
    """A single observation of the current epoch + seq_num.

    ``error`` is set (and the numeric fields are 0) when the probe
    could not complete — caller treats this as a soft failure and
    leaves existing state alone, same pattern as ``ReferenceSample``.
    """
    epoch: int
    seq_num: int
    checked_ms: int
    error: str | None = None


async def scan_epoch_history(
    unit: str = "monad-bft",
    since: str = "24 hours ago",
    timeout_sec: float = 600.0,
) -> list[tuple[int, int]]:
    """Full scan of ``unit`` journal, returning every (seq_num, epoch)
    tuple found in the window.

    Intended for startup seeding: populate the tracker with historical
    epoch spans so the dashboard has a typical-length estimate
    *immediately* instead of waiting for the first live rollover
    (which on Monad testnet is a ~5-hour wait). Consumer feeds each
    tuple to ``State.observe_epoch`` in arrival order.

    Reads line-by-line from stdout so 50+ MB of journal output doesn't
    blow the default StreamReader 64KB buffer — we tested an earlier
    communicate()-based implementation that returned 0 tuples because a
    single oversized json line tripped LimitOverrunError silently.
    Generous timeout (10 min) because a 24h journal at testnet cadence
    is ~180 MB and `journalctl` is single-threaded on the reader side.
    Window is 24h (was 8h until 2026-04-26): an 8h scan covers only
    ~1.4 epochs at testnet pace, so the oldest epoch is always partial
    and the typical-length median computed in state._epoch_progress
    falls to ~half the real value. 24h gives 4–5 epochs → 2–3 fully
    bracketed observations, enough for a stable median on cold start.
    """
    try:
        proc = await asyncio.create_subprocess_exec(
            "journalctl", "-u", unit, "-o", "cat", "--no-pager",
            "--since", since,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
            # 1 MB StreamReader limit — comfortably larger than any
            # single monad-bft log line we've seen (the biggest are
            # the ~2 KB proposal messages).
            limit=1_000_000,
        )
    except FileNotFoundError:
        return []
    results: list[tuple[int, int]] = []
    try:
        async def _drain():
            assert proc.stdout is not None
            while True:
                raw = await proc.stdout.readline()
                if not raw:
                    return
                line = raw.decode("utf-8", errors="replace")
                m = _EPOCH_RX.search(line)
                if m:
                    results.append((int(m.group(1)), int(m.group(2))))
        await asyncio.wait_for(_drain(), timeout=timeout_sec)
    except asyncio.TimeoutError:
        # Return whatever we got; don't block the caller forever.
        pass
    finally:
        if proc.returncode is None:
            try:
                proc.terminate()
                await asyncio.wait_for(proc.wait(), timeout=3)
            except (ProcessLookupError, asyncio.TimeoutError):
                try: proc.kill()
                except ProcessLookupError: pass
    return results


async def probe_epoch(
    unit: str = "monad-bft",
    since: str = "30 seconds ago",
    timeout_sec: float = 4.0,
) -> EpochSample:
    """Scan recent ``unit`` journal output for the latest epoch tuple.

    Returns the newest (seq_num, epoch) pair in the window. If the
    subprocess times out, the unit isn't found, or the window has no
    matching lines, the sample carries an ``error`` and the numerics
    are 0 — consumer should ignore in that case.
    """
    now_ms = int(time.time() * 1000)
    try:
        proc = await asyncio.create_subprocess_exec(
            "journalctl", "-u", unit, "-o", "cat", "--no-pager",
            "--since", since,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        out, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout_sec)
    except (FileNotFoundError, asyncio.TimeoutError) as e:
        return EpochSample(epoch=0, seq_num=0, checked_ms=now_ms, error=f"probe: {e}")
    text = out.decode("utf-8", errors="replace")
    last_seq: int | None = None
    last_epoch: int | None = None
    # finditer yields in input order; monad-bft lines are emitted in
    # time order, so the last match is the most recent.
    for m in _EPOCH_RX.finditer(text):
        last_seq = int(m.group(1))
        last_epoch = int(m.group(2))
    if last_epoch is None:
        return EpochSample(
            epoch=0, seq_num=0, checked_ms=now_ms,
            error="no epoch markers in window",
        )
    return EpochSample(
        epoch=last_epoch, seq_num=last_seq or 0, checked_ms=now_ms, error=None,
    )
