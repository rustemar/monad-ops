"""Tests for State._epoch_progress typical-length calculation.

The median is computed only from *fully bracketed* epochs — those with
both their predecessor and successor present in the in-memory map.
Without that filter, the oldest epoch in the map is always partial
(scan_epoch_history starts mid-epoch) and skews the median ~half. Bug
observed live 2026-04-26: typical=24K reported vs real ~50K, with
blocks_in already overrun, leaving the bar pegged at 100% indefinitely.
"""

from __future__ import annotations

from monad_ops.state import State


def test_no_epochs_returns_all_none() -> None:
    s = State()
    assert s._epoch_progress() == (None, None, None, None)


def test_single_epoch_no_typical() -> None:
    """Only one epoch known — no closed sample of any kind, typical=None."""
    s = State()
    s.observe_epoch(epoch=558, seq_num=27_854_966)
    s.observe_epoch(epoch=558, seq_num=27_900_000)
    cur, blocks_in, typical, _ = s._epoch_progress()
    assert cur == 558
    assert blocks_in == 27_900_000 - 27_854_966 + 1
    assert typical is None


def test_two_epochs_oldest_partial_no_bracketed() -> None:
    """557 → 558 with 558 current: 557 has no predecessor in map, so it
    is NOT bracketed. typical must stay None — the 557 span we observed
    is the tail-only fragment from journal-since-Nh, not a real epoch
    length."""
    s = State()
    # Partial 557: only saw last 22.5K blocks (real epoch ~50K).
    s.observe_epoch(epoch=557, seq_num=27_832_407)
    s.observe_epoch(epoch=557, seq_num=27_854_965)
    # Current 558.
    s.observe_epoch(epoch=558, seq_num=27_854_966)
    s.observe_epoch(epoch=558, seq_num=27_902_438)
    cur, blocks_in, typical, _ = s._epoch_progress()
    assert cur == 558
    assert blocks_in == 27_902_438 - 27_854_966 + 1
    assert typical is None, "557 is not bracketed (no predecessor) → must be excluded"


def test_three_epochs_middle_is_bracketed() -> None:
    """556 → 557 → 558 with 558 current: 557 is fully bracketed (556
    and 558 both in map), so its size is the median."""
    s = State()
    # Partial 556 (oldest, no predecessor in map): excluded.
    s.observe_epoch(epoch=556, seq_num=27_810_000)
    s.observe_epoch(epoch=556, seq_num=27_832_406)
    # Bracketed 557: real boundaries on both sides.
    s.observe_epoch(epoch=557, seq_num=27_832_407)
    s.observe_epoch(epoch=557, seq_num=27_882_448)  # ~50,042 blocks
    # Current 558.
    s.observe_epoch(epoch=558, seq_num=27_882_449)
    s.observe_epoch(epoch=558, seq_num=27_900_000)
    cur, blocks_in, typical, _ = s._epoch_progress()
    assert cur == 558
    assert typical == 27_882_448 - 27_832_407 + 1  # exact length of 557
    assert typical == 50_042


def test_multiple_bracketed_uses_median() -> None:
    """Four sequential closed epochs around current — two are bracketed,
    median picks the larger when both sizes differ."""
    s = State()
    sizes = {
        555: (27_750_000, 27_799_999),
        556: (27_800_000, 27_849_876),  # bracketed (555+557 in map)
        557: (27_849_877, 27_900_000),  # bracketed (556+558 in map)
        558: (27_900_001, 27_910_000),  # current
    }
    for ep, (f, l) in sizes.items():
        s.observe_epoch(epoch=ep, seq_num=f)
        s.observe_epoch(epoch=ep, seq_num=l)
    cur, blocks_in, typical, _ = s._epoch_progress()
    assert cur == 558
    # Bracketed: 556 (49,877) and 557 (50,124). Median index = 1 (sorted).
    assert typical == 50_124


def test_carried_typical_length_used_when_no_bracketed_yet() -> None:
    """Just after a service restart that crossed an epoch boundary: only
    the brand-new epoch is observed, no bracketing possible. Live
    typical would be None — fall back to the carried value persisted by
    the previous run, so the dashboard's progress bar isn't blank for
    the duration of the journal backfill."""
    s = State()
    s._carried_typical_length = 50_042  # set by bootstrap_carried_epoch_length()
    s.observe_epoch(epoch=589, seq_num=29_445_000)
    s.observe_epoch(epoch=589, seq_num=29_445_185)
    cur, blocks_in, typical, _ = s._epoch_progress()
    assert cur == 589
    assert blocks_in == 186
    assert typical == 50_042  # carried, not None


def test_live_bracketed_overrides_carried() -> None:
    """Once a fresh bracketed observation arrives (e.g. after the
    journal backfill or a live rollover), it takes precedence over the
    carried value silently — operators always see the most accurate
    current estimate."""
    s = State()
    s._carried_typical_length = 49_000  # stale-ish from previous run
    # Fresh map with a bracketed epoch.
    s.observe_epoch(epoch=556, seq_num=27_800_000)
    s.observe_epoch(epoch=557, seq_num=27_849_877)
    s.observe_epoch(epoch=557, seq_num=27_900_000)  # 50,124 blocks
    s.observe_epoch(epoch=558, seq_num=27_900_001)
    cur, blocks_in, typical, _ = s._epoch_progress()
    assert typical == 50_124  # live, not carried


def test_observe_epoch_persists_typical_to_meta() -> None:
    """When a fresh bracketed observation produces a new typical, it
    must be written to the storage meta table so the next service
    restart can carry it forward."""
    from unittest.mock import MagicMock

    storage = MagicMock()
    storage.get_meta.return_value = None
    s = State(storage=storage)
    # Single epoch — no bracketing, no persist.
    s.observe_epoch(epoch=556, seq_num=27_800_000)
    s.observe_epoch(epoch=556, seq_num=27_849_876)
    storage.put_meta.assert_not_called()
    # Add bracketing context.
    s.observe_epoch(epoch=557, seq_num=27_849_877)
    s.observe_epoch(epoch=557, seq_num=27_900_000)
    s.observe_epoch(epoch=558, seq_num=27_900_001)
    # On the 558 observation, 557 became bracketed — typical=50,124
    # should have been persisted.
    storage.put_meta.assert_called_with("epoch_typical_length", "50124")


def test_bootstrap_carried_epoch_length_loads_from_meta() -> None:
    """bootstrap_carried_epoch_length reads the meta value and primes
    `_carried_typical_length` so the first snapshot after restart
    already has an estimate to render against."""
    from unittest.mock import MagicMock

    storage = MagicMock()
    storage.get_meta.return_value = "50042"
    s = State(storage=storage)
    assert s._carried_typical_length is None
    s.bootstrap_carried_epoch_length()
    assert s._carried_typical_length == 50_042

    # Missing meta key → no-op.
    storage.get_meta.return_value = None
    s2 = State(storage=storage)
    s2.bootstrap_carried_epoch_length()
    assert s2._carried_typical_length is None

    # Garbage value → no-op (don't crash).
    storage.get_meta.return_value = "not-an-int"
    s3 = State(storage=storage)
    s3.bootstrap_carried_epoch_length()
    assert s3._carried_typical_length is None
