"""State.version_pending_since reads the version rule's persisted clock."""

from __future__ import annotations

import json

from monad_ops.state import State
from monad_ops.storage import Storage


def test_pending_since_comes_from_the_rule_state_for_that_version(tmp_path) -> None:
    storage = Storage(tmp_path / "s.db")
    state = State(storage=storage)
    assert state.version_pending_since("0.16.3") is None
    storage.put_meta("version_watch_state", json.dumps(
        {"pending_version": "0.16.3", "pending_since_ts": 1789500000.0}))
    assert state.version_pending_since("0.16.3") == 1789500000.0
    # A newer release must not inherit the previous one's clock.
    assert state.version_pending_since("0.16.4") is None
    # An outstanding release with no clock (state written before the clock existed).
    storage.put_meta("version_watch_state", json.dumps(
        {"pending_version": "0.16.3", "pending_since_ts": 0.0}))
    assert state.version_pending_since("0.16.3") is None
    storage.put_meta("version_watch_state", "not json")
    assert state.version_pending_since("0.16.3") is None
    storage.close()


def test_pending_since_is_none_without_persistence_or_version() -> None:
    assert State(storage=None).version_pending_since("0.16.3") is None
