"""Parser for the MIP-8 dual-write state-root line.

While a node runs both timelines (MIP-8 phase A through phase C) the
node emits one of these per committed block, carrying the state root
computed by each encoding. The source line moves between releases (364
on 0.15.2, 370 on 0.16.0), so the marker keys on content only. Example,
verbatim from a testnet full node (stripped of the journal prefix):

    2026-08-10 08:26:45.656520016 [1713313] runloop_monad.cpp:364  (0.15.2)
    LOG_INFO block=52470964,
    block_id=0xf991e436e6292b8468cff59d99106fef1e09c186add5c3823c2f0c39b44497ec
    state_root primary=0xf9b4709722f14511d1bbbb43a58ecc26f43b9aa6b7f967794f3e9d8e17153657
    secondary=0xabbefa612c52dfed0c06bff28afdf043b136eac9cb3d952cb7d3a7876b7a06c0

Field glossary:
  block        — block number the roots were computed for
  primary      — state root from the on-disk primary timeline
  secondary    — state root from the on-disk secondary timeline

The two roots are expected to DIFFER: they are the same state under two
encodings (slot and page). Nothing here is a divergence check — the line
carries no agreement signal, only proof that both timelines are being
written. Agreement is verified by comparing the page root across nodes,
which this parser cannot do alone.

Marker choice matters in both directions. Keying on ``block=`` would
also claim the sibling `runloop_monad.cpp:99` "Run to block= …" line;
keying on ``state_root primary=`` would claim the single-root record
that the same binary logs *outside* the migration window. Neither carries
a pair of roots, so either mistake books drift on every block of a
healthy node. The marker requires ``secondary=``.

Outside phases A through C only that single-root form is emitted, so a
flat-zero ``dual_root`` ok-counter with zero drift is the normal reading
on most nodes.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from monad_ops.parser import drift

# Cheap substring prefilter so the per-block firehose never reaches a
# regex. Measured on 10,895 live journal lines: 408 contain "state_root"
# and all 408 are this record.
_DUAL_ROOT_PREFILTER = "state_root"

# The marker MUST require ``secondary=``. A node outside the migration
# window logs the same record with a single root — verified on this host
# before phase A:
#
#   runloop_monad.cpp:372 … block=52206281, block_id=0x507b… \
#   state_root primary=0xc404…
#
# Gating on ``state_root primary=`` alone claims that line too, then
# fails extraction and books drift on every block — 978 per five minutes
# on a node that is behaving perfectly. That would break the one thing
# the drift counter promises, which is reading zero at steady state.
#
# It must also be no stricter than ``_ROOTS_RE`` about whitespace, or the
# ``\s+`` below is unreachable and a formatting-only change upstream
# becomes a silent miss instead of visible drift.
_DUAL_ROOT_MARKER_RE = re.compile(r"\bstate_root\s+primary=\S+\s+secondary=")

# Block number appears before the marker, both roots after it. Roots are
# matched loosely (0x + hex) rather than pinned to 64 nibbles so a future
# width change degrades into a value we still report, not into drift.
_BLOCK_RE = re.compile(r"\bblock=(\d+)")
_ROOTS_RE = re.compile(
    r"state_root\s+primary=(0x[0-9a-fA-F]+)\s+secondary=(0x[0-9a-fA-F]+)"
)


@dataclass(frozen=True, slots=True)
class DualRoot:
    """One parsed dual-write state-root record."""

    block_number: int
    primary_root: str
    secondary_root: str


def parse_dual_root(line: str) -> DualRoot | None:
    """Parse a dual-write state-root line, or return None.

    Returns None without touching any counter when the marker is absent,
    so the ordinary firehose of unrelated lines stays free.
    """
    if _DUAL_ROOT_PREFILTER not in line:
        return None
    if _DUAL_ROOT_MARKER_RE.search(line) is None:
        return None

    roots = _ROOTS_RE.search(line)
    block = _BLOCK_RE.search(line)
    if roots is None or block is None:
        drift.record_drift(drift.DUAL_ROOT, line)
        return None

    drift.record_ok(drift.DUAL_ROOT)
    return DualRoot(
        block_number=int(block.group(1)),
        primary_root=roots.group(1),
        secondary_root=roots.group(2),
    )
