"""Checkout metadata shared by the changes endpoint and page asset versions.

Moved out of ``api.app`` unchanged (queue item R1). Running-version snapshots
are captured once at import; live git helpers can report a later checkout tip.
"""

from __future__ import annotations

import subprocess
import time
from pathlib import Path

_THIS_DIR = Path(__file__).parent
_PKG_DIR = _THIS_DIR.parent
_REPO_DIR = _PKG_DIR.parent
_TEMPLATE_DIR = _PKG_DIR / "dashboard" / "templates"
_STATIC_DIR = _PKG_DIR / "dashboard" / "static"


def _git(*args: str) -> str | None:
    """One read-only git command against the checkout; None when git cannot answer."""
    try:
        out = subprocess.check_output(
            ["git", "-C", str(_REPO_DIR), *args],
            stderr=subprocess.DEVNULL,
            timeout=2,
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError):
        return None
    return out.decode(errors="replace").strip()


def _git_head() -> str | None:
    return _git("rev-parse", "--short", "HEAD") or None


def _git_head_full() -> str | None:
    return _git("rev-parse", "HEAD") or None


def _git_recent_commits(limit: int = 8) -> list[dict] | None:
    """Newest-first ``{commit, committed_at, subject}`` rows, or None outside a checkout.

    Lists the pushed tip when the branch tracks one, so every row exists on
    the public repo; only subjects and short hashes reach the wire.
    """
    ref = "@{u}" if _git("rev-parse", "--verify", "-q", "@{u}") else "HEAD"
    out = _git("log", f"-n{limit}", "--format=%h%x1f%ct%x1f%s", ref)
    if out is None:
        return None
    rows: list[dict] = []
    for line in out.splitlines():
        parts = line.split("\x1f", 2)
        if len(parts) != 3 or not parts[1].isdigit():
            continue
        rows.append({"commit": parts[0], "committed_at": int(parts[1]), "subject": parts[2]})
    return rows


def _asset_version() -> str:
    """Version string for cache-busting CSS/JS via ?v=... query param.

    Combines the short git HEAD hash (human-readable) with the newest
    template/static mtime (catches uncommitted edits after a restart).
    Either part can be missing; the result still changes whenever either
    changes.
    """
    git_part = _git_head() or "dev"
    mtime = 0
    for d in (_STATIC_DIR, _TEMPLATE_DIR):
        for f in d.rglob("*"):
            if f.is_file():
                try:
                    mtime = max(mtime, int(f.stat().st_mtime))
                except OSError:
                    continue
    return f"{git_part}-{mtime}" if mtime else git_part


_ASSET_VERSION = _asset_version()
# What this process is running: HEAD at import time. Compared against the live
# HEAD later so a commit made without a restart shows up as such.
_RUNNING_COMMIT = _git_head()
_RUNNING_COMMIT_FULL = _git_head_full()
_STARTED_AT = time.time()
