"""Parser for critical incident lines in monad-execution logs.

Catches assertion failures, panics, and chunk-exhaustion warnings —
the signatures that precede node stalls. All patterns are grounded in
public operator-reported incidents from the Monad Developers Discord
(Apr 2026 TN1 + mainnet) so the catalogue reflects the classes of
failure the community has actually hit, not speculation.

Reference incidents (see wiki/pains/validator.md for attribution):
  * ProofLine stall 2026-04-16: ``Assertion 'block_cache.emplace(...).second' failed``
  * Karlo Endorphine 2026-03-02: ``monad-execution/category/core/io/ring.cpp:45: ... Assertion ...``
  * Unit410 consensus panic 2025-12-04: ``thread 'main' panicked at ... high qc too far ahead of block tree root``
  * Nodes.Guru halt 2026-04-14: ``Disk usage: 0.9999. Chunks: 8100 fast, 73 slow, 1 free``

This module only detects and classifies. Dedup and alerting happen in
``rules/assertion.py``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import StrEnum


class AssertionKind(StrEnum):
    """Classes of critical line we recognize. Each has a distinct
    remediation path, so the alert sink wants to know which one.
    """
    CXX_ASSERT = "cxx_assert"        # C++ Assertion '...' failed
    RUST_PANIC = "rust_panic"         # thread '...' panicked at ...
    QC_OVERSHOOT = "qc_overshoot"     # high qc too far ahead of block tree root
    CHUNK_EXHAUSTION = "chunks"       # Disk usage: 0.99xx ... Chunks: N fast
    GENERIC_FATAL = "generic_fatal"   # catch-all FATAL / fatal error


@dataclass(frozen=True, slots=True)
class AssertionEvent:
    """One matched critical line.

    ``key`` is built deterministically from kind + a stable fragment of
    the message so the dedup layer collapses repeated emissions (common
    when a node retries-and-fails rapidly) without dropping distinct
    incidents.
    """
    kind: AssertionKind
    raw: str
    key: str           # stable dedup key
    location: str | None   # file:line or function, if captured
    summary: str       # short one-liner for alert title


# ── patterns ──────────────────────────────────────────────────────────
# Ordered most-specific → least-specific so the first match wins.

# C++ assertion. Quote excerpt from log: "Assertion 'X' failed". The
# capture group extracts X (the expression) for the key.
_CXX_ASSERT = re.compile(r"Assertion\s+'([^']+)'\s+failed", re.IGNORECASE)

# Optional file:line preceding a C++ assertion line — used to populate
# `location` when present. monad-execution typically emits:
#   /path/to/file.cpp:45: funcname: Assertion '...' failed.
_FILE_LINE = re.compile(r"([a-zA-Z0-9_./-]+\.(?:cpp|h|hpp|rs)):(\d+)")

# Rust panic line; monad-bft rust components use this.
_RUST_PANIC = re.compile(
    r"thread\s+'([^']+)'\s+panicked\s+at\s+([^:]+:\d+(?::\d+)?)(?::\s*(.+))?",
    re.IGNORECASE,
)

# Specific consensus-state panic message observed 2025-12-04 Unit410.
_QC_OVERSHOOT = re.compile(
    r"high\s+qc\s+too\s+far\s+ahead\s+of\s+block\s+tree\s+root",
    re.IGNORECASE,
)

# Chunk-exhaustion line observed 2026-04-14 Nodes.Guru. The "Disk usage"
# here is monad-execution's internal triedb-disk stat, NOT OS disk — our
# disk_usage probe covers the OS side; this covers the triedb side.
_CHUNK_EXHAUSTION = re.compile(
    r"Disk\s+usage:\s*(0\.\d{2,})\s*\.?\s*Chunks:\s*(\d+)\s*fast",
    re.IGNORECASE,
)
_CHUNK_CRITICAL_RATIO = 0.90  # below this we don't fire

# Generic fatal catch-all — last resort. Intentionally narrow: only
# fires on capitalized FATAL token at start of line-content, to avoid
# matching "...fatal error in user space..." type debug noise.
_GENERIC_FATAL = re.compile(r"\bFATAL\b[:\s]", re.IGNORECASE)


def parse_assertion(line: str) -> AssertionEvent | None:
    """Inspect one log line; return an AssertionEvent or None."""
    if not line:
        return None

    # Skip __exec_block rows — they're the normal per-block summary and
    # should never match an assertion pattern, but a conservative early
    # return is cheap.
    if "__exec_block," in line:
        return None

    # 1. QC overshoot — exact phrase check, cheapest.
    if _QC_OVERSHOOT.search(line):
        return AssertionEvent(
            kind=AssertionKind.QC_OVERSHOOT,
            raw=line,
            key=f"{AssertionKind.QC_OVERSHOOT.value}:high_qc_overshoot",
            location=_extract_file_line(line),
            summary="Consensus panic: high QC too far ahead of block tree root",
        )

    # 2. Rust panic.
    m = _RUST_PANIC.search(line)
    if m:
        thread, loc = m.group(1), m.group(2)
        detail = (m.group(3) or "").strip()
        key_tail = loc
        return AssertionEvent(
            kind=AssertionKind.RUST_PANIC,
            raw=line,
            key=f"{AssertionKind.RUST_PANIC.value}:{key_tail}",
            location=loc,
            summary=(f"Rust panic in thread '{thread}' at {loc}"
                     + (f": {detail[:120]}" if detail else "")),
        )

    # 3. C++ assertion.
    m = _CXX_ASSERT.search(line)
    if m:
        expr = m.group(1)
        file_line = _extract_file_line(line)
        key_tail = (file_line or expr)[:160]
        return AssertionEvent(
            kind=AssertionKind.CXX_ASSERT,
            raw=line,
            key=f"{AssertionKind.CXX_ASSERT.value}:{key_tail}",
            location=file_line,
            summary=f"C++ assertion failed: {expr[:160]}",
        )

    # 4. Chunk exhaustion (triedb-side disk full).
    m = _CHUNK_EXHAUSTION.search(line)
    if m:
        ratio = float(m.group(1))
        fast_chunks = int(m.group(2))
        if ratio >= _CHUNK_CRITICAL_RATIO:
            return AssertionEvent(
                kind=AssertionKind.CHUNK_EXHAUSTION,
                raw=line,
                # dedup by ratio bucket so rapidly-incrementing readings
                # near full don't spam, but crossing 0.95→0.99 re-fires.
                key=f"{AssertionKind.CHUNK_EXHAUSTION.value}:{ratio:.2f}",
                location=None,
                summary=f"TrieDB chunk exhaustion: {ratio:.4f} used, {fast_chunks} fast chunks",
            )

    # 5. Generic FATAL. Keep last so specific patterns get priority.
    if _GENERIC_FATAL.search(line):
        return AssertionEvent(
            kind=AssertionKind.GENERIC_FATAL,
            raw=line,
            # dedup key = first 80 chars of the stripped line for stability
            key=f"{AssertionKind.GENERIC_FATAL.value}:{line.strip()[:80]}",
            location=None,
            summary=f"FATAL log line: {line.strip()[:160]}",
        )

    return None


def _extract_file_line(line: str) -> str | None:
    m = _FILE_LINE.search(line)
    if not m:
        return None
    return f"{m.group(1)}:{m.group(2)}"
