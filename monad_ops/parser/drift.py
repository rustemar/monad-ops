"""Counters for log lines a parser recognised but could not extract.

A monad release that renames or drops a field does not break the tailer
loudly: the marker still matches, the required-field check rejects the
line, and the record is dropped with nothing counted anywhere. That is
the 0.14.5 shape — ``ac=``/``sc=`` disappeared from ``__exec_block`` and
ingestion went quiet for 30 hours before anyone noticed.

Two counters per kind make both halves of that failure visible:

  * ``drift`` — the marker matched but the extractor did not. Strictly
    zero at steady state on a supported release; any sustained non-zero
    value means the log schema moved under us.
  * ``ok``    — lines that parsed. Needed because a marker that vanishes
    *entirely* (a renamed record type) produces no drift at all: only a
    flat-zero ``ok`` counter shows it.

Kinds are declared up front so a starved parser reads as ``ok: 0``
rather than as a missing key. Exposed on ``/api/status/errors``.

Single-process, single-threaded asyncio — plain dict increments, no lock.
"""

from __future__ import annotations

import time

import structlog

log = structlog.stdlib.get_logger()

EXEC_BLOCK = "exec_block"
CONSENSUS_ADVANCING_ROUND = "consensus_advancing_round"
CONSENSUS_LOCAL_TIMEOUT = "consensus_local_timeout"
CONSENSUS_PROPOSAL = "consensus_proposal"

KINDS: tuple[str, ...] = (
    EXEC_BLOCK,
    CONSENSUS_ADVANCING_ROUND,
    CONSENSUS_LOCAL_TIMEOUT,
    CONSENSUS_PROPOSAL,
)

# Drift arrives in floods when it arrives at all (every block, every
# round), so log the first one per kind and then one in a thousand.
_LOG_EVERY = 1000
_SAMPLE_CHARS = 300

_ok: dict[str, int] = dict.fromkeys(KINDS, 0)
_drift: dict[str, int] = dict.fromkeys(KINDS, 0)
_last_drift_ms: dict[str, int | None] = dict.fromkeys(KINDS, None)


def record_ok(kind: str) -> None:
    _ok[kind] = _ok.get(kind, 0) + 1


def record_drift(kind: str, line: str = "", *, now=time.time) -> None:
    count = _drift.get(kind, 0) + 1
    _drift[kind] = count
    _last_drift_ms[kind] = int(now() * 1000)
    if count == 1 or count % _LOG_EVERY == 0:
        log.warning(
            "parser.drift",
            kind=kind,
            count=count,
            sample=line[:_SAMPLE_CHARS] if line else None,
        )


def snapshot() -> dict[str, object]:
    """Serializable view for the API."""
    return {
        "total": sum(_drift.values()),
        "kinds": {
            kind: {
                "ok": _ok.get(kind, 0),
                "drift": _drift.get(kind, 0),
                "last_drift_ms": _last_drift_ms.get(kind),
            }
            for kind in sorted(set(KINDS) | set(_ok) | set(_drift))
        },
    }


def reset() -> None:
    """Zero every counter — tests only."""
    _ok.clear()
    _drift.clear()
    _last_drift_ms.clear()
    _ok.update(dict.fromkeys(KINDS, 0))
    _drift.update(dict.fromkeys(KINDS, 0))
    _last_drift_ms.update(dict.fromkeys(KINDS, None))
