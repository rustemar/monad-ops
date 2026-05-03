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
    restart can carry it forward. Current-epoch + first_seq are also
    persisted on every new (epoch, first_seq) pair — they don't depend
    on bracketing."""
    from unittest.mock import MagicMock

    storage = MagicMock()
    storage.get_meta.return_value = None
    s = State(storage=storage)
    # Single epoch — no bracketing, but current_epoch + first_seq are
    # still persisted (they're independent of typical_length).
    s.observe_epoch(epoch=556, seq_num=27_800_000)
    written_so_far = {c.args[0] for c in storage.put_meta.call_args_list}
    assert "epoch_typical_length" not in written_so_far  # no bracketing yet
    assert "epoch_current" in written_so_far
    assert "epoch_current_first_seq" in written_so_far
    # Add bracketing context.
    s.observe_epoch(epoch=556, seq_num=27_849_876)
    s.observe_epoch(epoch=557, seq_num=27_849_877)
    s.observe_epoch(epoch=557, seq_num=27_900_000)
    s.observe_epoch(epoch=558, seq_num=27_900_001)
    # On the 558 observation, 557 became bracketed — typical=50,124
    # should have been persisted.
    written = [
        (c.args[0], c.args[1]) for c in storage.put_meta.call_args_list
    ]
    assert ("epoch_typical_length", "50124") in written


def test_bootstrap_carried_epoch_length_loads_from_meta() -> None:
    """bootstrap_carried_epoch_length reads the meta values and primes
    the carry fields so the first snapshot after restart already has
    an estimate to render against."""
    from unittest.mock import MagicMock

    storage = MagicMock()
    # Keyed lookup so each meta key returns its own value.
    storage.get_meta.side_effect = lambda k: {
        "epoch_typical_length": "50042",
        "epoch_current": "589",
        "epoch_current_first_seq": "29404927",
    }.get(k)
    s = State(storage=storage)
    assert s._carried_typical_length is None
    s.bootstrap_carried_epoch_length()
    assert s._carried_typical_length == 50_042
    assert s._carried_current_epoch == 589
    assert s._carried_current_first_seq == 29_404_927

    # All meta missing → no-op on each field.
    storage.get_meta.side_effect = lambda k: None
    s2 = State(storage=storage)
    s2.bootstrap_carried_epoch_length()
    assert s2._carried_typical_length is None
    assert s2._carried_current_epoch is None
    assert s2._carried_current_first_seq is None

    # Garbage values → no-op (don't crash, don't poison).
    storage.get_meta.side_effect = lambda k: "not-an-int"
    s3 = State(storage=storage)
    s3.bootstrap_carried_epoch_length()
    assert s3._carried_typical_length is None
    assert s3._carried_current_epoch is None


def test_carried_first_seq_used_on_first_observation() -> None:
    """After restart, when the live probe's first sample matches the
    carried current_epoch, blocks_in is computed against the carried
    first_seq — not the just-sampled seq. Without this, the dashboard
    reads as ~1 block_in for the duration of the journal backfill."""
    s = State()
    s._carried_current_epoch = 589
    s._carried_current_first_seq = 29_404_927
    # Live probe returns the current latest seq.
    s.observe_epoch(epoch=589, seq_num=29_452_500)
    cur, blocks_in, _, _ = s._epoch_progress()
    assert cur == 589
    assert blocks_in == 29_452_500 - 29_404_927 + 1


def test_carried_first_seq_ignored_when_epoch_differs() -> None:
    """If we restarted across an epoch boundary, the carried
    (epoch=588, first_seq=...) shouldn't apply to the new epoch=589.
    Live probe must drive _epochs[589] from the just-sampled seq."""
    s = State()
    s._carried_current_epoch = 588  # stale
    s._carried_current_first_seq = 27_900_000
    s.observe_epoch(epoch=589, seq_num=29_452_500)
    cur, blocks_in, _, _ = s._epoch_progress()
    assert cur == 589
    assert blocks_in == 1  # fresh, ignoring stale carry


def test_observe_epoch_persists_current_epoch_and_first_seq() -> None:
    """In addition to typical_length, observe_epoch must persist the
    current epoch number and its first_seq so the next restart can
    carry them forward."""
    from unittest.mock import MagicMock

    storage = MagicMock()
    storage.get_meta.return_value = None
    s = State(storage=storage)
    s.observe_epoch(epoch=589, seq_num=29_404_927)
    s.observe_epoch(epoch=589, seq_num=29_452_500)
    # Both keys must have been written.
    written = {
        c.args[0]: c.args[1] for c in storage.put_meta.call_args_list
    }
    assert written.get("epoch_current") == "589"
    assert written.get("epoch_current_first_seq") == "29404927"
