"""Tests for the bft consensus accumulator + storage layer.

Covers:
  * Storage round-trip for the bft_minute table (insert, replace,
    window aggregate, prune-on-retention).
  * State.add_consensus_event minute-bucketing + roll-over.
  * State snapshot fields (validator_timeout_pct_5m etc.) including
    the in-progress bucket so the live dashboard isn't a minute behind.
  * State.bootstrap_bft_from_storage rehydration after a restart.
"""

from __future__ import annotations

import time
from pathlib import Path
from unittest.mock import patch

import pytest

from monad_ops.parser import ConsensusEvent, ConsensusEventKind
from monad_ops.state import State
from monad_ops.storage import BftMinute, Storage


# Fixed reference timestamp inside a known minute boundary so tests
# don't depend on wall-clock drift. 2026-04-24T06:00:00Z = 1777003200000.
_T0_MS = 1_777_003_200_000  # minute boundary
_MIN_MS = 60_000


def _mk_event(
    kind: ConsensusEventKind,
    *,
    offset_ms: int = 0,
    round_num: int = 27_597_178,
    epoch: int | None = 549,
) -> ConsensusEvent:
    return ConsensusEvent(
        kind=kind,
        round=round_num,
        epoch=None if kind is ConsensusEventKind.LOCAL_TIMEOUT else epoch,
        ts_ms=_T0_MS + offset_ms,
        leader=("0x" + "ab" * 32) if kind is ConsensusEventKind.LOCAL_TIMEOUT else None,
        next_leader=("0x" + "cd" * 32) if kind is ConsensusEventKind.LOCAL_TIMEOUT else None,
    )


# ── Storage layer ────────────────────────────────────────────────────────

def test_bft_minute_round_trip(tmp_path: Path) -> None:
    """upsert_bft_minute writes; load_recent_bft_minutes reads back ascending."""
    storage = Storage(tmp_path / "state.db")
    minutes = [
        BftMinute(ts_minute=_T0_MS + i * _MIN_MS,
                  rounds_total=150 + i,
                  rounds_tc=i,
                  local_timeouts=i // 2)
        for i in range(5)
    ]
    for m in minutes:
        storage.upsert_bft_minute(m)

    loaded = storage.load_recent_bft_minutes(limit=10)
    assert len(loaded) == 5
    assert [m.ts_minute for m in loaded] == [_T0_MS + i * _MIN_MS for i in range(5)]
    assert loaded[2].rounds_tc == 2
    storage.close()


def test_bft_minute_replace_semantics(tmp_path: Path) -> None:
    """Re-writing the same ts_minute REPLACES — collector flushes the
    in-progress minute incrementally, so each upsert overwrites."""
    storage = Storage(tmp_path / "state.db")
    storage.upsert_bft_minute(BftMinute(_T0_MS, 100, 1, 0))
    storage.upsert_bft_minute(BftMinute(_T0_MS, 150, 2, 1))  # cumulative

    loaded = storage.load_recent_bft_minutes(limit=10)
    assert len(loaded) == 1
    assert loaded[0].rounds_total == 150
    assert loaded[0].rounds_tc == 2
    assert loaded[0].local_timeouts == 1
    storage.close()


def test_bft_window_aggregate_computes_timeout_pct(tmp_path: Path) -> None:
    """load_bft_window sums across buckets and returns the
    chain-wide validator-timeout %."""
    storage = Storage(tmp_path / "state.db")
    # 3 minutes, 100 rounds each, 3 TC total → 1.0% timeout pct
    storage.upsert_bft_minute(BftMinute(_T0_MS,            100, 1, 0))
    storage.upsert_bft_minute(BftMinute(_T0_MS + _MIN_MS,  100, 1, 2))
    storage.upsert_bft_minute(BftMinute(_T0_MS + 2*_MIN_MS, 100, 1, 1))

    summary = storage.load_bft_window(_T0_MS, _T0_MS + 3 * _MIN_MS)
    assert summary["minutes"] == 3
    assert summary["rounds_total"] == 300
    assert summary["rounds_tc"] == 3
    assert summary["local_timeouts"] == 3
    assert summary["validator_timeout_pct"] == 1.0
    assert summary["local_timeout_per_min"] == 1.0
    storage.close()


def test_list_bft_minutes_returns_dicts_with_precomputed_pct(tmp_path: Path) -> None:
    """list_bft_minutes serves the per-minute series for the chart.
    Each row carries a precomputed timeout_pct so the dashboard
    doesn't repeat the division per point."""
    storage = Storage(tmp_path / "state.db")
    storage.upsert_bft_minute(BftMinute(_T0_MS,            200, 4, 0))    # 2%
    storage.upsert_bft_minute(BftMinute(_T0_MS + _MIN_MS,  100, 0, 0))    # 0%
    storage.upsert_bft_minute(BftMinute(_T0_MS + 2*_MIN_MS, 50, 5, 1))    # 10%

    rows = storage.list_bft_minutes(_T0_MS, _T0_MS + 3 * _MIN_MS)
    assert len(rows) == 3
    assert rows[0]["timeout_pct"] == 2.0
    assert rows[1]["timeout_pct"] == 0.0
    assert rows[2]["timeout_pct"] == 10.0
    # Ascending by timestamp — chart consumer relies on order.
    assert rows[0]["t"] < rows[1]["t"] < rows[2]["t"]
    storage.close()


def test_list_bft_minutes_filters_by_window(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "state.db")
    storage.upsert_bft_minute(BftMinute(_T0_MS - _MIN_MS, 100, 1, 0))  # before
    storage.upsert_bft_minute(BftMinute(_T0_MS,           100, 1, 0))  # inside
    storage.upsert_bft_minute(BftMinute(_T0_MS + _MIN_MS, 100, 1, 0))  # inside
    storage.upsert_bft_minute(BftMinute(_T0_MS + 5*_MIN_MS, 100, 1, 0))  # after

    rows = storage.list_bft_minutes(_T0_MS, _T0_MS + _MIN_MS)
    assert len(rows) == 2
    storage.close()


def test_bft_window_empty_returns_zeros(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "state.db")
    summary = storage.load_bft_window(_T0_MS, _T0_MS + 5 * _MIN_MS)
    assert summary["minutes"] == 0
    assert summary["rounds_total"] == 0
    assert summary["validator_timeout_pct"] == 0.0
    assert summary["local_timeout_per_min"] == 0.0
    storage.close()


def test_prune_older_than_drops_old_bft_minutes(tmp_path: Path) -> None:
    """Retention sweep must include bft_minute or the rollup grows
    unbounded after the operator turns on prune."""
    storage = Storage(tmp_path / "state.db")
    now_ms = int(time.time() * 1000)
    # one row well inside retention, one well outside
    fresh = now_ms - _MIN_MS               # 1 minute ago
    stale = now_ms - 30 * 86400_000         # 30 days ago
    storage.upsert_bft_minute(BftMinute(fresh, 100, 0, 0))
    storage.upsert_bft_minute(BftMinute(stale, 100, 0, 0))

    deleted = storage.prune_older_than(keep_days=7)
    assert deleted.get("bft_minute", 0) >= 1

    remaining = storage.load_recent_bft_minutes(limit=10)
    assert len(remaining) == 1
    assert remaining[0].ts_minute == fresh
    storage.close()


# ── State accumulator ────────────────────────────────────────────────────

def test_add_consensus_event_increments_current_bucket() -> None:
    """All events inside one minute land in the in-progress bucket; the
    snapshot's 5m window includes the in-progress bucket so the live
    dashboard isn't a minute behind."""
    state = State()
    # 95 QC + 5 TC + 2 local timeouts in the same minute
    for _ in range(95):
        state.add_consensus_event(_mk_event(ConsensusEventKind.ROUND_ADVANCE_QC))
    for _ in range(5):
        state.add_consensus_event(_mk_event(ConsensusEventKind.ROUND_ADVANCE_TC))
    for _ in range(2):
        state.add_consensus_event(_mk_event(ConsensusEventKind.LOCAL_TIMEOUT))

    # Pin time.time so the 5m window reliably covers the bucket.
    with patch("monad_ops.state.time.time", return_value=(_T0_MS + 30_000) / 1000):
        snap = state.snapshot()
    assert snap.bft_rounds_observed_5m == 100
    assert snap.bft_local_timeouts_5m == 2
    assert snap.validator_timeout_pct_5m == 5.0   # 5 / 100 * 100
    assert snap.local_timeout_per_min_5m == 2.0   # 2 events / 1 minute


def test_minute_rollover_closes_bucket_and_starts_fresh() -> None:
    """Crossing a minute boundary closes the prior bucket into the
    deque and resets counters for the new minute."""
    state = State()
    state.add_consensus_event(_mk_event(ConsensusEventKind.ROUND_ADVANCE_QC, offset_ms=0))
    state.add_consensus_event(_mk_event(ConsensusEventKind.ROUND_ADVANCE_TC, offset_ms=0))
    # Cross into the next minute
    state.add_consensus_event(_mk_event(ConsensusEventKind.ROUND_ADVANCE_QC, offset_ms=_MIN_MS + 1))

    with patch("monad_ops.state.time.time", return_value=(_T0_MS + _MIN_MS + 30_000) / 1000):
        snap = state.snapshot()
    # 5m window covers both buckets: 2 + 1 = 3 rounds, 1 TC.
    assert snap.bft_rounds_observed_5m == 3
    assert snap.validator_timeout_pct_5m == round(1 / 3 * 100, 2)


def test_snapshot_zero_when_no_events() -> None:
    """Cold start: no bft data yet → all four fields read as 0,
    not None and not stale."""
    state = State()
    snap = state.snapshot()
    assert snap.validator_timeout_pct_5m == 0.0
    assert snap.local_timeout_per_min_5m == 0.0
    assert snap.bft_rounds_observed_5m == 0
    assert snap.bft_local_timeouts_5m == 0


def test_window_excludes_buckets_outside_5m_horizon() -> None:
    """A bucket older than the 5m horizon must not contribute — proves
    the per-bucket cutoff filter actually filters."""
    state = State()
    # One event 10 minutes ago.
    old = _mk_event(ConsensusEventKind.ROUND_ADVANCE_TC,
                    offset_ms=-10 * _MIN_MS)
    state.add_consensus_event(old)
    # One event right now.
    new = _mk_event(ConsensusEventKind.ROUND_ADVANCE_QC, offset_ms=0)
    state.add_consensus_event(new)

    # Pin the clock to "now == _T0_MS"; 5m window covers offsets ≥ -300_000.
    with patch("monad_ops.state.time.time", return_value=_T0_MS / 1000):
        snap = state.snapshot()
    # Only the QC at offset 0 is inside the window.
    assert snap.bft_rounds_observed_5m == 1
    assert snap.validator_timeout_pct_5m == 0.0


def test_bootstrap_bft_from_storage_rehydrates_buckets(tmp_path: Path) -> None:
    """A restarted process should see prior minute buckets immediately,
    not start cold for 5 minutes."""
    storage = Storage(tmp_path / "state.db")
    # Seed three minutes of pre-restart data
    base = _T0_MS - 3 * _MIN_MS
    for i in range(3):
        storage.upsert_bft_minute(BftMinute(
            ts_minute=base + i * _MIN_MS,
            rounds_total=120,
            rounds_tc=2,
            local_timeouts=1,
        ))

    state = State(storage=storage)
    n = state.bootstrap_bft_from_storage(limit=300)
    assert n == 3

    # Pin clock to T0 so the 5m window includes all three seeded minutes.
    with patch("monad_ops.state.time.time", return_value=_T0_MS / 1000):
        snap = state.snapshot()
    assert snap.bft_rounds_observed_5m == 360
    assert snap.bft_local_timeouts_5m == 3
    assert snap.validator_timeout_pct_5m == round(6 / 360 * 100, 2)
    storage.close()
