"""Async tailer over journalctl output.

Spawns ``journalctl -u <unit> -o cat -f --lines=0`` as a subprocess and
yields parsed records as they arrive. ``-o cat`` gives us the raw
MESSAGE field (no systemd prefix), which is what the parsers expect.

We deliberately shell out rather than use ``systemd.journal.Reader``
from the python-systemd package: the system Python 3.12 on this host
does not ship that extension, and adding a C-build dependency adds
operational friction for no real gain at our tailing rate.

**Self-healing follow.** A live ``journalctl -f`` can silently stop
delivering lines after a journald rotation: ``readline()`` then blocks
forever — no EOF, no error — while the node keeps producing. That froze
ingestion for 30h on 2026-06-05 under a monad-bft log flood (the flood
inflated journald write volume → frequent rotation → broken follows).
``tail_raw_lines`` bounds each read in follow mode and respawns the
child on silence; the EOF/``TailError`` contract is unchanged.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator
from dataclasses import dataclass

import structlog

from monad_ops.parser import (
    AssertionEvent,
    ExecBlock,
    parse_assertion,
    parse_exec_block,
)

log = structlog.stdlib.get_logger()

# Both monad units emit log lines every sub-second, so this much total
# silence on a live follow means the journalctl handle stopped following
# (rotation), not that the node went quiet.
DEFAULT_IDLE_TIMEOUT_SEC = 30.0
# If respawning isn't restoring flow (journald itself wedged, or a log
# flood rotating faster than we re-attach), escalate rather than spin
# silently: more than this many respawns within the window yields a
# non-graceful TailError so the consumer fires its CRITICAL alert.
DEFAULT_MAX_RESPAWNS = 10
DEFAULT_RESPAWN_WINDOW_SEC = 300.0


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


def _journalctl_cmd(unit: str, follow: bool, lookback: str | None) -> list[str]:
    cmd = ["journalctl", "-u", unit, "-o", "cat", "--no-pager"]
    if follow:
        cmd.append("-f")
    if lookback is None:
        if follow:
            cmd += ["--lines=0"]
    else:
        cmd += ["--since", lookback]
    return cmd


async def tail_raw_lines(
    unit: str,
    *,
    follow: bool = True,
    lookback: str | None = None,
    idle_timeout_sec: float = DEFAULT_IDLE_TIMEOUT_SEC,
    max_respawns: int = DEFAULT_MAX_RESPAWNS,
    respawn_window_sec: float = DEFAULT_RESPAWN_WINDOW_SEC,
    read_limit: int = 64 * 1024,
    spawn=asyncio.create_subprocess_exec,
    now=time.monotonic,
) -> AsyncIterator[str | TailError]:
    """Yield raw journal MESSAGE lines, self-healing a stalled follow.

    In follow mode each ``readline()`` is bounded by ``idle_timeout_sec``;
    on timeout the journalctl child is killed and respawned **live-only**
    (``--lines=0``) — never replaying ``lookback`` history, because the
    consumer's in-memory counters are not idempotent and a replay would
    double-count. A respawn therefore drops at most ``idle_timeout_sec``
    of lines.

    A genuine subprocess exit (EOF) is surfaced as ``TailError`` exactly
    as before. More than ``max_respawns`` respawns inside
    ``respawn_window_sec`` also surface a non-graceful ``TailError`` so
    the consumer escalates instead of spinning quietly.

    Backfill (``follow=False``) is bounded by EOF and never respawns.

    Requires the invoking user to be a member of ``systemd-journal`` (or
    root). We do NOT shell out via sudo — hardened systemd units set
    NoNewPrivileges, which blocks sudo entirely.
    """
    initial_cmd = _journalctl_cmd(unit, follow, lookback)
    live_cmd = _journalctl_cmd(unit, follow=True, lookback=None)
    cmd = initial_cmd
    respawn_ts: list[float] = []

    while True:
        proc = await spawn(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            limit=read_limit,
        )
        try:
            assert proc.stdout is not None
            while True:
                if follow:
                    try:
                        raw = await asyncio.wait_for(
                            proc.stdout.readline(), idle_timeout_sec
                        )
                    except asyncio.TimeoutError:
                        # Follow went silent while the node is live —
                        # the handle stopped following. Drop to the
                        # respawn path (kill happens in `finally`).
                        break
                else:
                    raw = await proc.stdout.readline()
                if not raw:
                    # EOF — subprocess ended. Negative rc = killed by
                    # signal (our own shutdown / systemd stop); mark
                    # graceful so the consumer does not alert on a
                    # restart we initiated.
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
                yield raw.decode("utf-8", errors="replace").rstrip("\n")
        finally:
            if proc.returncode is None:
                proc.terminate()
                try:
                    await asyncio.wait_for(proc.wait(), timeout=3)
                except asyncio.TimeoutError:
                    proc.kill()

        # Only reached via the idle-timeout `break` (follow mode). Record
        # the respawn and bail loudly if we're thrashing.
        t = now()
        respawn_ts.append(t)
        respawn_ts[:] = [s for s in respawn_ts if t - s <= respawn_window_sec]
        log.warning(
            "tailer.stall_respawn", unit=unit,
            idle_sec=idle_timeout_sec, recent_respawns=len(respawn_ts),
        )
        if len(respawn_ts) > max_respawns:
            yield TailError(
                message=(
                    f"{unit} follow stalled {len(respawn_ts)}x in "
                    f"{int(respawn_window_sec)}s — journald rotation / log "
                    f"flood; respawning is not restoring flow"
                ),
                graceful=False,
            )
            return
        cmd = live_cmd


async def tail_execution_blocks(
    unit: str = "monad-execution",
    lookback: str | None = None,
    follow: bool = True,
    *,
    idle_timeout_sec: float = DEFAULT_IDLE_TIMEOUT_SEC,
    max_respawns: int = DEFAULT_MAX_RESPAWNS,
    respawn_window_sec: float = DEFAULT_RESPAWN_WINDOW_SEC,
    spawn=asyncio.create_subprocess_exec,
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
    async for line in tail_raw_lines(
        unit,
        follow=follow,
        lookback=lookback,
        idle_timeout_sec=idle_timeout_sec,
        max_respawns=max_respawns,
        respawn_window_sec=respawn_window_sec,
        spawn=spawn,
    ):
        if isinstance(line, TailError):
            yield line
            return
        block = parse_exec_block(line)
        if block is not None:
            yield block
            continue
        event = parse_assertion(line)
        if event is not None:
            yield event
