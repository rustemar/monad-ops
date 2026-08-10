"""Tests for the MIP-8 dual-write state-root parser.

The captured line is inlined rather than added to a fixture file so the
exact bytes this was written against stay next to the assertions.
"""

from __future__ import annotations

import pytest

from monad_ops.parser import drift
from monad_ops.parser.dual_root import parse_dual_root

# Verbatim from monad-execution on this node, 2026-08-10, one day after
# the MIP-8 phase A migration. journalctl -o cat, so no syslog prefix.
_LINE = (
    "2026-08-10 08:26:45.656520016 [1713313] runloop_monad.cpp:364 LOG_INFO\t"
    "block=52470964, "
    "block_id=0xf991e436e6292b8468cff59d99106fef1e09c186add5c3823c2f0c39b44497ec "
    "state_root "
    "primary=0xf9b4709722f14511d1bbbb43a58ecc26f43b9aa6b7f967794f3e9d8e17153657 "
    "secondary=0xabbefa612c52dfed0c06bff28afdf043b136eac9cb3d952cb7d3a7876b7a06c0"
)

# The single-root form the SAME binary logs outside the migration
# window (runloop_monad.cpp:372), captured on this host the morning
# before phase A. A marker keyed on "state_root primary=" claims this
# one and then fails extraction, booking drift on every block of a node
# that is behaving perfectly.
_PRE_MIGRATION_LINE = (
    "2026-08-09 10:00:00.050035524 [2260757] runloop_monad.cpp:372 LOG_INFO\t"
    "block=52206281, "
    "block_id=0x507b36a111b2b7498c4755a94c0f0cb068a360a002f719cdfa3dda9af501599d "
    "state_root "
    "primary=0xc40451c3038b66bfd090a3cd1eae560e5fb6fb46551a7fbbf95919770ca78a5d"
)

# The sibling runloop_monad.cpp:99 record, which also carries a block
# number and a block id but no roots. A marker keyed on "block=" would
# claim this one and manufacture drift on every block.
_SIBLING_LINE = (
    "2026-04-18 12:59:15.016411973 [20453] runloop_monad.cpp:99 LOG_INFO        "
    "Run to block= 26243795, block_id "
    "0x2a4cc963b4a016ab0c13da94ea9c8f90c9ae36db4a8b706b3c8e747e9b53f9ba, "
    "number of transactions      2, tps =  2564, gps =  216 M, rss =   6032 MB"
)


@pytest.fixture(autouse=True)
def _reset_drift():
    drift.reset()
    yield
    drift.reset()


def test_parses_the_captured_line() -> None:
    ev = parse_dual_root(_LINE)
    assert ev is not None
    assert ev.block_number == 52470964
    assert ev.primary_root == (
        "0xf9b4709722f14511d1bbbb43a58ecc26f43b9aa6b7f967794f3e9d8e17153657"
    )
    assert ev.secondary_root == (
        "0xabbefa612c52dfed0c06bff28afdf043b136eac9cb3d952cb7d3a7876b7a06c0"
    )
    # The two roots are the same state under two encodings; equal roots
    # would be the surprising case, not the healthy one.
    assert ev.primary_root != ev.secondary_root


def test_success_counts_ok_and_no_drift() -> None:
    parse_dual_root(_LINE)
    snap = drift.snapshot()["kinds"][drift.DUAL_ROOT]
    assert snap["ok"] == 1
    assert snap["drift"] == 0


def test_sibling_run_to_block_line_is_not_claimed() -> None:
    assert parse_dual_root(_SIBLING_LINE) is None
    snap = drift.snapshot()["kinds"][drift.DUAL_ROOT]
    assert snap == {"ok": 0, "drift": 0, "last_drift_ms": None}


def test_unrelated_line_touches_no_counter() -> None:
    assert parse_dual_root("nothing to see here") is None
    assert drift.snapshot()["total"] == 0


def test_marker_present_but_unextractable_counts_drift() -> None:
    # Marker matched, roots malformed — this is the shape the counter
    # exists for: the log schema moved and we must not go quiet.
    assert parse_dual_root("block=1 state_root primary=zzz secondary=zzz") is None
    snap = drift.snapshot()["kinds"][drift.DUAL_ROOT]
    assert snap["ok"] == 0
    assert snap["drift"] == 1


def test_missing_block_number_counts_drift() -> None:
    line = (
        "state_root primary=0xaa secondary=0xbb"
    )
    assert parse_dual_root(line) is None
    assert drift.snapshot()["kinds"][drift.DUAL_ROOT]["drift"] == 1


def test_dual_root_is_a_registered_drift_kind() -> None:
    # Registered up front so a node that never migrated reads as ok: 0
    # rather than as a missing key.
    assert drift.DUAL_ROOT in drift.KINDS
    assert drift.DUAL_ROOT in drift.snapshot()["kinds"]


def test_single_root_line_from_a_non_migrated_node_is_not_claimed() -> None:
    """The regression that matters most.

    Every node outside phases A-C logs this on every block. Claiming it
    would put the drift counter permanently off zero, which is the one
    reading the whole counter exists to provide.
    """
    assert parse_dual_root(_PRE_MIGRATION_LINE) is None
    snap = drift.snapshot()["kinds"][drift.DUAL_ROOT]
    assert snap == {"ok": 0, "drift": 0, "last_drift_ms": None}


def test_whitespace_reformat_is_still_parsed_not_silently_dropped() -> None:
    # The logger already mixes tabs and spaces on this very line, so a
    # separator change upstream is realistic. The marker must be no
    # stricter about whitespace than the extraction regex.
    line = _LINE.replace("state_root primary=", "state_root\tprimary=")
    ev = parse_dual_root(line)
    assert ev is not None
    assert ev.block_number == 52470964
    assert drift.snapshot()["kinds"][drift.DUAL_ROOT]["drift"] == 0
