"""Evidence snapshot for a waltrace-flood envelope.

The forensically interesting artifacts of the v0.14.5 waltrace bug
self-destruct: the 0-byte current chunk (whose creation time IS the
thread's death time) and the last filled chunk are both deleted by the
stock hourly ``monad-cruft`` cleanup once they age past 5 h
(``RETENTION_WAL=300min`` in ``clear-old-artifacts.sh``, ships in the
monad deb). Three of the four operators who hit this bug lost exactly
that evidence — they found an empty wal dir and a misleading
top-of-hour dir mtime.

So when ``WaltraceFloodRule`` arms, snapshot immediately:

  * the wal dir listing with nanosecond timestamps (death time, chunk
    sizes — proves/disproves death-at-1GiB-rotation for this instance),
  * a sanitized journal slice around the first error line (pre-context
    shows what the node was doing when the thread died; the flood
    itself compresses to nothing in gzip).

Artifact: ``data/waltrace/<unix_ts>.txt.gz``, same sanitize + atomic
gzip-write machinery as the per-reorg captures.
"""

from __future__ import annotations

import asyncio
import os
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

import structlog

from monad_ops.reorg_capture import (
    _format_journal_ts,
    _run_journalctl,
    _write_gz,
    sanitize_text,
)


log = structlog.stdlib.get_logger()


def waltrace_dir_for(persistence_path: str | Path) -> Path:
    """Sibling of the sqlite DB, same convention as reorg captures."""
    return Path(persistence_path).parent / "waltrace"


def _iso(epoch_sec: float) -> str:
    return datetime.fromtimestamp(epoch_sec, tz=timezone.utc).isoformat()


def list_wal_dir(wal_dir: str | Path) -> str:
    """Plain-text listing of the wal dir with ns-precision timestamps.

    No IPs or secrets live in chunk names/sizes/mtimes, so this section
    needs no sanitization. Errors come back as comment lines — a
    missing/unreadable dir is itself a finding worth recording.
    """
    out: list[str] = []
    try:
        entries = sorted(os.scandir(wal_dir), key=lambda e: e.name)
    except OSError as e:
        return f"# wal dir unreadable: {e}\n"
    for entry in entries:
        try:
            st = entry.stat()
        except OSError as e:
            out.append(f"{entry.name}\t# stat failed: {e}")
            continue
        out.append(
            f"{entry.name}\tsize={st.st_size}"
            f"\tmtime={_iso(st.st_mtime)}"
            f"\tctime={_iso(st.st_ctime)}"
        )
    try:
        dir_st = os.stat(wal_dir)
        out.append(f".\tdir-mtime={_iso(dir_st.st_mtime)}")
    except OSError:
        pass
    return "\n".join(out) + "\n"


async def capture_waltrace_evidence(
    first_error_ts: float,
    out_dir: Path,
    *,
    wal_dir: str | Path = "/home/monad/monad-bft/wal",
    pre_sec: int = 300,
    post_sec: int = 5,
    unit: str = "monad-bft",
    _runner: callable | None = None,  # test seam: (since, until, unit) -> bytes
    _lister: callable | None = None,  # test seam: (wal_dir) -> str
) -> Path | None:
    """Snapshot wal dir + journal context, write gzipped artifact.

    Unlike the reorg capture there is no flush-defer: the flood is
    minutes old by the time the rule arms and the pre-context is what
    matters. Errors log and swallow — a failed capture must never take
    the consensus loop down with it.
    """
    runner = _runner or _run_journalctl
    lister = _lister or list_wal_dir

    listing = lister(wal_dir)

    since_iso = _format_journal_ts(first_error_ts - pre_sec)
    until_iso = _format_journal_ts(first_error_ts + post_sec)
    try:
        raw = await asyncio.to_thread(runner, since_iso, until_iso, unit)
    except (subprocess.TimeoutExpired, OSError) as e:
        log.warning("waltrace_capture.journalctl_failed", err=str(e))
        raw = f"# journalctl failed: {e}\n".encode()

    journal = sanitize_text(raw.decode("utf-8", errors="replace"))

    text = (
        f"# waltrace evidence capture {_iso(time.time())}\n"
        f"# first error line at {_iso(first_error_ts)}\n"
        f"\n## wal dir listing ({wal_dir})\n{listing}"
        f"\n## journal {since_iso} .. {until_iso} UTC (sanitized)\n{journal}"
    )

    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{int(first_error_ts)}.txt.gz"
    try:
        await asyncio.to_thread(_write_gz, path, text)
    except OSError as e:
        log.warning("waltrace_capture.write_failed", path=str(path), err=str(e))
        return None

    log.info("waltrace_capture.saved", path=str(path), bytes=path.stat().st_size)
    return path
