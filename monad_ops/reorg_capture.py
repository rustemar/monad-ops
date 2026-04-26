"""Per-reorg journal trace capture.

When the reorg rule fires, schedule a deferred snapshot of the
``monad-bft`` journal across a small window centred on the reorged
block's wall-clock timestamp. The capture is fire-and-forget on a
worker task: defer until ``post_sec + flush_buffer_sec`` past the
event so journald has finished flushing the post-reorg lines, then
spawn ``journalctl --since/--until`` once and write a sanitized
gzipped JSONL artifact to ``data/reorgs/<block>-<ts>.jsonl.gz``.

Sanitization happens at write time (single source of truth, no
read-time branch). Peer IPs from the wire-auth keepalive stream
and the local OTLP loopback (``127.0.0.1:4317``) are scrubbed —
the rest of the consensus trace (validator pubkeys, block ids,
rounds, votes, base fees) is already public chain data and stays
intact.
"""

from __future__ import annotations

import asyncio
import gzip
import re
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import structlog


log = structlog.stdlib.get_logger()


# Matches IPv4 literals with an optional ``:port`` suffix. Word boundary
# on the left keeps inline hex like ``...0a3fa3f9...`` safe even when
# four hex bytes happen to render as digits-and-dots. The four ``\d{1,3}``
# groups are loose by design — we'd rather over-redact a malformed
# decimal than leak a real address through a stricter regex.
_IP_RE = re.compile(r"\b\d{1,3}(?:\.\d{1,3}){3}(?::\d+)?\b")
_IP_REDACT = "<ip>"


def sanitize_text(text: str) -> str:
    """Redact IPv4 (and ``ip:port``) from a journal text blob.

    The only sensitive fields in monad-bft logs are the ``remote_addr``
    values emitted by ``monad_wireauth::session::transport`` (peer
    keepalives, ~5 lines/sec) and the OTLP loopback connect attempts.
    Everything else is chain-level public data.
    """
    return _IP_RE.sub(_IP_REDACT, text)


@dataclass(frozen=True, slots=True)
class CaptureRequest:
    block_number: int
    block_ts_ms: int


def journal_dir_for(persistence_path: str | Path) -> Path:
    """Resolve the per-reorg journal artifact directory.

    Sibling of the sqlite DB so all on-disk state lives under one
    parent (``data/`` by default). The directory is created on demand
    by ``capture_reorg_journal``.
    """
    return Path(persistence_path).parent / "reorgs"


def artifact_path(out_dir: Path, block_number: int, block_ts_ms: int) -> Path:
    """Stable filename for a reorg's journal capture.

    ``<block_number>-<unix_ts_seconds>.jsonl.gz`` — the seconds-precision
    timestamp lets two reorgs on the same block_number (theoretically
    possible across a deep restart) coexist without overwriting.
    """
    ts_sec = int(block_ts_ms // 1000)
    return out_dir / f"{block_number}-{ts_sec}.jsonl.gz"


def _format_journal_ts(epoch_sec: float) -> str:
    """Format a UTC timestamp the way journalctl --since/--until expects."""
    return (
        datetime.fromtimestamp(epoch_sec, tz=timezone.utc)
        .strftime("%Y-%m-%d %H:%M:%S")
    )


def _run_journalctl(since_iso: str, until_iso: str, unit: str = "monad-bft") -> bytes:
    """Run ``journalctl -o cat`` over a closed window, return raw bytes.

    ``-o cat`` strips the ``shadow-anafra monad-node[PID]:`` prefix so
    the captured artifact carries only the JSON message body — no
    hostname, no PID. Fails closed: on non-zero exit we return whatever
    stdout was emitted plus the stderr appended as a comment line, so
    the operator can still see partial context if journald itself
    misbehaved.
    """
    cmd = [
        "journalctl", "-u", unit, "-o", "cat", "--no-pager",
        "--since", since_iso, "--until", until_iso,
    ]
    proc = subprocess.run(  # noqa: S603 — fixed argv, no shell
        cmd, capture_output=True, timeout=30,
    )
    if proc.returncode != 0:
        err = proc.stderr.decode("utf-8", errors="replace").strip()
        return proc.stdout + (
            f"\n# journalctl exited rc={proc.returncode}: {err}\n".encode()
        )
    return proc.stdout


async def capture_reorg_journal(
    req: CaptureRequest,
    out_dir: Path,
    *,
    pre_sec: int = 10,
    post_sec: int = 10,
    flush_buffer_sec: int = 5,
    unit: str = "monad-bft",
    _now: callable | None = None,  # test seam
    _runner: callable | None = None,  # test seam: (since, until, unit) -> bytes
) -> Path | None:
    """Defer, snapshot the journal, sanitize, write gzipped JSONL.

    Returns the artifact path on success, or ``None`` if the capture
    produced no output (journald rotated out the window before we
    asked, or unit name is wrong). Errors log and swallow — a failed
    capture must never crash the collector.
    """
    now = _now or time.time
    runner = _runner or _run_journalctl

    block_ts_sec = req.block_ts_ms / 1000.0
    wait_until = block_ts_sec + post_sec + flush_buffer_sec
    delay = wait_until - now()
    if delay > 0:
        await asyncio.sleep(delay)

    since_iso = _format_journal_ts(block_ts_sec - pre_sec)
    until_iso = _format_journal_ts(block_ts_sec + post_sec)

    try:
        raw = await asyncio.to_thread(runner, since_iso, until_iso, unit)
    except (subprocess.TimeoutExpired, OSError) as e:  # noqa: BLE001
        log.warning(
            "reorg_capture.journalctl_failed",
            block=req.block_number, err=str(e),
        )
        return None

    if not raw:
        log.info(
            "reorg_capture.empty",
            block=req.block_number,
            since=since_iso, until=until_iso,
        )
        return None

    sanitized = sanitize_text(raw.decode("utf-8", errors="replace"))

    out_dir.mkdir(parents=True, exist_ok=True)
    path = artifact_path(out_dir, req.block_number, req.block_ts_ms)
    try:
        await asyncio.to_thread(_write_gz, path, sanitized)
    except OSError as e:
        log.warning(
            "reorg_capture.write_failed",
            block=req.block_number, path=str(path), err=str(e),
        )
        return None

    log.info(
        "reorg_capture.saved",
        block=req.block_number,
        path=str(path),
        bytes=path.stat().st_size,
        since=since_iso, until=until_iso,
    )
    return path


def _write_gz(path: Path, text: str) -> None:
    """Atomic gzip write — temp file then rename so half-written files
    never appear under the published name."""
    tmp = path.with_suffix(path.suffix + ".tmp")
    with gzip.open(tmp, "wb") as fh:
        fh.write(text.encode("utf-8"))
    tmp.replace(path)


def find_artifact(out_dir: Path, block_number: int) -> Path | None:
    """Return the artifact path for ``block_number`` if one exists.

    Glob over the directory rather than rebuilding the filename from
    a known timestamp — the timestamp lives in the alert / blocks DB
    and we don't want the API layer to re-derive it.
    """
    if not out_dir.is_dir():
        return None
    matches = sorted(out_dir.glob(f"{block_number}-*.jsonl.gz"))
    return matches[-1] if matches else None
