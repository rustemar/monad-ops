"""Partial journal prefixes must not teach a truncated epoch size."""

from __future__ import annotations

import pytest

from monad_ops.state import State

_PARTIAL = [(720, 8), (800, 8), (801, 9), (825, 9)]
_CLOSED_PREFIX = [(720, 8), (800, 8), (801, 9), (900, 9), (901, 10), (925, 10)]


class MetaStore:
    def __init__(self, carried=100):
        self.values = {} if carried is None else {"epoch_typical_length": str(carried)}
        self.writes = []

    def get_meta(self, key):
        return self.values.get(key)

    def put_meta(self, key, value):
        self.writes.append((key, value))
        self.values[key] = value


def make_state(carried=100):
    store = MetaStore(carried)
    state = State(storage=store)
    state.bootstrap_carried_epoch_length()
    return state, store


def typical_writes(store):
    return [value for key, value in store.writes if key == "epoch_typical_length"]


@pytest.mark.parametrize("live_suffix", [False, True])
def test_partial_tail_cannot_override_carried_estimate(live_suffix):
    state, store = make_state()
    if live_suffix:
        state.observe_epoch(9, 899)
    state.observe_epoch(10, 930)

    assert state.observe_epoch_history(_PARTIAL, complete=False) == 9

    assert state._epoch_progress() == (10, 1, 100, None)
    assert state._epochs[9] == (801, 899 if live_suffix else 825)
    assert store.values["epoch_typical_length"] == "100"
    assert typical_writes(store) == []


def test_partial_tail_stays_excluded_after_later_live_rollover():
    state, store = make_state()
    state.observe_epoch_history(_PARTIAL, complete=False)
    assert state._epoch_progress() == (9, 25, 100, None)
    state.observe_epoch(9, 875)
    assert state._epoch_progress() == (9, 75, 100, None)
    state.observe_epoch(10, 930)

    assert state._epoch_progress() == (10, 1, 100, None)
    assert state._incomplete_epoch_tails == {9}
    assert typical_writes(store) == []


def test_cold_start_does_not_invent_an_estimate_from_partial_tail():
    state, store = make_state(carried=None)
    state.observe_epoch(10, 930)
    state.observe_epoch_history(_PARTIAL, complete=False)

    assert state._epoch_progress() == (10, 1, None, None)
    assert "epoch_typical_length" not in store.values


@pytest.mark.parametrize("complete", [False, True])
def test_closed_prefix_learns_once_after_whole_batch(complete):
    state, store = make_state(carried=150)
    state.observe_epoch(10, 950)
    tail = state.observe_epoch_history(_CLOSED_PREFIX, complete=complete)

    assert tail == (None if complete else 10)
    assert state._epoch_progress() == (10, 50, 100, None)
    assert typical_writes(store) == ["100"]  # Never persist intermediate 1.
    assert state._incomplete_epoch_tails == (set() if complete else {10})


@pytest.mark.parametrize("observations", [[], [(0, 9), (10, 0), (-1, 8)]])
def test_no_valid_history_is_a_no_op(observations):
    state, store = make_state()
    state.observe_epoch(10, 950)
    before = (state._epochs.copy(), store.writes[:])

    assert state.observe_epoch_history(observations, complete=False) is None
    assert (state._epochs, store.writes) == before
    assert state._incomplete_epoch_tails == set()


def test_max_epoch_not_last_arrival_defines_incomplete_tail():
    state, store = make_state(carried=150)
    state.observe_epoch(10, 950)
    observations = [_CLOSED_PREFIX[i] for i in (5, 2, 0, 4, 1, 3)]

    assert state.observe_epoch_history(observations, complete=False) == 10
    assert state._epoch_progress() == (10, 50, 100, None)
    assert typical_writes(store) == ["100"]


def test_epoch_cap_prunes_flags_after_whole_batch():
    state = State()
    state._epochs = {epoch: (epoch * 100, epoch * 100 + 99) for epoch in range(1, 501)}
    state._incomplete_epoch_tails = {1, 499}

    state.observe_epoch_history([(1, 1), (50_100, 501)], complete=False)

    assert len(state._epochs) == 500
    assert 1 not in state._epochs
    assert state._incomplete_epoch_tails == {499, 501}
    assert state._incomplete_epoch_tails <= state._epochs.keys()


def test_malformed_batch_does_not_publish_partial_ranges_or_flags():
    state, store = make_state()
    state.observe_epoch(10, 950)
    before = (state._epochs.copy(), store.writes[:])

    with pytest.raises(TypeError):
        state.observe_epoch_history([(720, 8), ("bad", 9)], complete=True)

    assert (state._epochs, store.writes) == before
    assert state._incomplete_epoch_tails == set()


def test_existing_carried_value_is_not_claimed_repaired():
    state, store = make_state(carried=25)
    state.observe_epoch(10, 930)
    state.observe_epoch_history(_PARTIAL, complete=False)

    assert state._epoch_progress()[2] == 25
    assert typical_writes(store) == []


def test_current_epoch_and_first_sequence_never_regress_from_older_history():
    state, store = make_state()
    state._carried_current_epoch = 10
    state._carried_current_first_seq = 901
    state.observe_epoch(10, 950)
    state.observe_epoch_history(_PARTIAL, complete=False)

    assert state._epoch_progress() == (10, 50, 100, None)
    assert store.values["epoch_current"] == "10"
    assert store.values["epoch_current_first_seq"] == "901"


def test_completed_out_of_order_batch_retains_highest_500_epochs():
    state = State()
    state._epochs = {epoch: (epoch * 100, epoch * 100 + 99) for epoch in range(1, 501)}
    state.observe_epoch_history(
        [(50_250, 502), (50_100, 501), (39_999, 400), (1, 1)], complete=True,
    )

    assert set(state._epochs) == set(range(3, 503))
    assert state._epochs[400] == (39_999, 40_099)
    assert state._epoch_progress() == (502, 1, 100, None)
    assert state._incomplete_epoch_tails == set()


def test_persistence_failure_keeps_final_memory_view_and_retries_later():
    state, store = make_state(carried=150)
    original_put = store.put_meta

    def fail_write(key, value):
        raise OSError("offline fixture write failure")

    store.put_meta = fail_write
    state.observe_epoch_history(_CLOSED_PREFIX, complete=False)
    assert state._epoch_progress() == (10, 25, 100, None)
    assert store.values["epoch_typical_length"] == "150"
    assert state._persisted_typical_length == 150

    store.put_meta = original_put
    state.observe_epoch(10, 950)
    assert store.values["epoch_typical_length"] == "100"
    assert typical_writes(store) == ["100"]
