"""Async tailer over journalctl output.

Spawns ``journalctl -u <unit> -o cat -f --lines=0`` as a subprocess and
yields parsed records as they arrive. ``-o cat`` gives us the raw
MESSAGE field (no systemd prefix), which is what the parsers expect.

We deliberately shell out rather than use ``systemd.journal.Reader``
from the python-systemd package: the system Python 3.12 on this host
does not ship that extension, and adding a C-build dependency adds
operational friction for no real gain at our tailing rate.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass

from monad_ops.parser import (
    AssertionEvent,
    ExecBlock,
    parse_assertion,
    parse_exec_block,
)


@dataclass(frozen=True, slots=True)
class TailError:
    """Emitted when the journalctl subprocess exits or errors.

    The consumer may choose to restart the tailer.

    ``graceful`` is True when the subprocess was terminated by a signal
    (rc < 0, typically SIGTERM during our own shutdown). Unlike a
    non-zero positive rc, this is not an operational failure — the
    consumer should log it but must not raise a critical alert.
    """
    message: str
    graceful: bool = False


async def tail_execution_blocks(
    unit: str = "monad-execution",
    lookback: str | None = None,
    follow: bool = True,
) -> AsyncIterator[ExecBlock | AssertionEvent | TailError]:
    """Yield records from journalctl: per-block summaries + critical incidents.

    Parsers tried in order: ``parse_exec_block`` first (by far the most
    frequent line type), then ``parse_assertion``. Lines matching
    neither are silently skipped.

    Modes:
      * ``follow=True, lookback=None``       — tail live from now (default).
      * ``follow=True, lookback="5 min ago"`` — emit history, then tail live.
      * ``follow=False, lookback="5 min ago"`` — emit history, then stop.
    """
    # Requires the invoking user to be a member of ``systemd-journal``
    # (or root). We do NOT shell out via sudo — hardened systemd units
    # set NoNewPrivileges, which blocks sudo entirely.
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
    )

    try:
        assert proc.stdout is not None
        while True:
            raw = await proc.stdout.readline()
            if not raw:
                # EOF — subprocess ended.
                code = await proc.wait()
                stderr = (await proc.stderr.read()).decode(errors="replace") if proc.stderr else ""
                # Negative rc = killed by signal (our own shutdown, systemd
                # stop, or SIGKILL). Mark graceful so the consumer does
                # not fire a critical alert for our own restart.
                yield TailError(
                    message=f"journalctl exited rc={code}: {stderr.strip()[:500]}",
                    graceful=code < 0,
                )
                return
            line = raw.decode("utf-8", errors="replace").rstrip("\n")
            block = parse_exec_block(line)
            if block is not None:
                yield block
                continue
            event = parse_assertion(line)
            if event is not None:
                yield event
    finally:
        if proc.returncode is None:
            proc.terminate()
            try:
                await asyncio.wait_for(proc.wait(), timeout=3)
            except asyncio.TimeoutError:
                proc.kill()
