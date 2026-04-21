"""Round-trip tests for SQLite persistence layer."""

from __future__ import annotations

from pathlib import Path

import pytest

from monad_ops.parser import ExecBlock
from monad_ops.rules.events import AlertEvent, Severity
from monad_ops.state import State
from monad_ops.storage import Storage


def _mk_block(n: int, rtp: float = 0.0, tx: int = 10, retried: int = 0) -> ExecBlock:
    return ExecBlock(
        block_number=n,
        block_id=f"0x{n:064x}",
        timestamp_ms=1776_000_000_000 + n * 400,
        tx_count=tx,
        retried=retried,
        retry_pct=rtp,
        state_reset_us=73,
        tx_exec_us=653,
        commit_us=448,
        total_us=1232,
        tps_effective=4594,
        tps_avg=2435,
        gas_used=450276,
        gas_per_sec_effective=689,
        gas_per_sec_avg=365,
        active_chunks=1085212,
        slow_chunks=10000000,
    )


def test_write_and_load_blocks_round_trip(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "state.db")
    blocks = [_mk_block(100 + i, rtp=float(i)) for i in range(5)]
    for b in blocks:
        storage.write_block(b)

    loaded = storage.load_recent_blocks(limit=10)
    assert len(loaded) == 5
    assert [b.block_number for b in loaded] == [100, 101, 102, 103, 104]
    assert loaded[2].retry_pct == 2.0
    assert loaded[-1].block_id.startswith("0x")
    storage.close()


def test_insert_or_ignore_is_idempotent(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "state.db")
    b = _mk_block(500)
    storage.write_block(b)
    storage.write_block(b)  # dup
    storage.write_block(b)  # dup
    assert storage.block_count() == 1
    storage.close()


def test_range_query_by_block_and_ts(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "state.db")
    for i in range(20):
        storage.write_block(_mk_block(1000 + i, rtp=i * 3.0))

    r = storage.load_blocks_range(from_block=1005, to_block=1010)
    assert [b.block_number for b in r] == [1005, 1006, 1007, 1008, 1009, 1010]

    ts_cut = 1776_000_000_000 + 1015 * 400
    r2 = storage.load_blocks_range(from_ts_ms=ts_cut)
    assert [b.block_number for b in r2] == [1015, 1016, 1017, 1018, 1019]
    storage.close()


def test_alerts_round_trip(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "state.db")
    ev = AlertEvent(
        rule="retry_spike",
        severity=Severity.WARN,
        key="retry_spike:60",
        title="retry spike",
        detail="avg retry_pct 60% > 50% threshold",
    )
    storage.write_alert(ev, ts=1_700_000_000.0)
    rows = storage.load_recent_alerts(limit=10)
    assert len(rows) == 1
    assert rows[0].rule == "retry_spike"
    assert rows[0].severity is Severity.WARN
    assert rows[0].ts == 1_700_000_000.0
    storage.close()


def test_state_bootstrap_from_storage(tmp_path: Path) -> None:
    db = tmp_path / "state.db"
    # Seed a DB with 3 blocks, then create a fresh State bound to the same DB.
    seeded = Storage(db)
    for i in range(3):
        seeded.write_block(_mk_block(9000 + i))
    seeded.close()

    storage = Storage(db)
    state = State(storage=storage)
    n = state.bootstrap_from_storage(limit=100)
    assert n == 3
    recent = state.recent_blocks(limit=100)
    assert [b.block_number for b in recent] == [9000, 9001, 9002]

    # Newly-added block is persisted through State, too.
    state.add_block(_mk_block(9003))
    storage.close()

    reopened = Storage(db)
    assert reopened.block_count() == 4
    reopened.close()


def test_state_without_storage_is_still_functional() -> None:
    state = State(storage=None)
    state.add_block(_mk_block(1))
    assert state.recent_blocks(limit=10)[0].block_number == 1
    # bootstrap is a no-op with no storage.
    assert state.bootstrap_from_storage(limit=100) == 0


def test_load_reorg_history_returns_count_and_last(tmp_path: Path) -> None:
    """Storage.load_reorg_history() must count all reorg rows and return
    the newest one for ReorgRule.bootstrap() to consume on restart.
    """
    storage = Storage(tmp_path / "state.db")
    # Two reorgs + an unrelated retry_spike alert to prove the filter works.
    storage.write_alert(AlertEvent(
        rule="reorg", severity=Severity.CRITICAL, key="reorg:100:0xnew",
        title="Chain reorg detected",
        detail="Block #100 id changed: 0xaaaa…0000 → 0xbbbb…1111. Total reorgs observed: 1.",
    ), ts=1_776_000_000.0)
    storage.write_alert(AlertEvent(
        rule="retry_spike", severity=Severity.WARN, key="retry_spike:warn",
        title="retry", detail="irrelevant",
    ), ts=1_776_000_001.0)
    storage.write_alert(AlertEvent(
        rule="reorg", severity=Severity.CRITICAL, key="reorg:200:0xnew2",
        title="Chain reorg detected",
        detail="Block #200 id changed: 0xcccc…2222 → 0xdddd…3333. Total reorgs observed: 2.",
    ), ts=1_776_000_002.0)

    count, last = storage.load_reorg_history()
    assert count == 2
    assert last is not None
    assert last.rule == "reorg"
    assert "Block #200" in last.detail
    assert last.ts == 1_776_000_002.0
    storage.close()


def test_state_bootstrap_alerts_from_storage(tmp_path: Path) -> None:
    """State.bootstrap_alerts_from_storage() rehydrates the in-memory
    alerts deque after a restart, so the dashboard "recent alerts"
    panel doesn't silently empty itself.
    """
    db = tmp_path / "state.db"
    seeded = Storage(db)
    for i, sev in enumerate([Severity.WARN, Severity.RECOVERED, Severity.CRITICAL]):
        seeded.write_alert(AlertEvent(
            rule="retry_spike" if sev != Severity.CRITICAL else "reorg",
            severity=sev,
            key=f"k:{i}",
            title=f"t{i}",
            detail=f"d{i}",
        ), ts=1_776_000_000.0 + i)
    seeded.close()

    storage = Storage(db)
    state = State(storage=storage)
    n = state.bootstrap_alerts_from_storage(limit=10)
    assert n == 3
    recent = state.recent_alerts(limit=10)
    assert len(recent) == 3
    # Oldest first (matches load_recent_alerts reversed-ascending contract).
    assert recent[0].severity is Severity.WARN
    assert recent[-1].severity is Severity.CRITICAL
    assert recent[-1].rule == "reorg"
    storage.close()


def test_prune_older_than_removes_old_rows_keeps_recent(tmp_path: Path) -> None:
    """Retention pass deletes rows in blocks/tx_contract_block/alerts
    whose timestamp predates the cutoff, and leaves newer rows intact."""
    import time
    from monad_ops.storage import ContractBlockAgg

    storage = Storage(tmp_path / "state.db")
    now = time.time()

    # Seed 3 blocks: one 30d old, one 10d old, one 1h old.
    # _mk_block's timestamp_ms is fabricated, so override each after insert
    # by writing through a custom helper — easier to just write a bespoke
    # block row here since the factory hard-codes ts.
    def _block_at(n, ts_sec):
        import dataclasses
        return dataclasses.replace(_mk_block(n), timestamp_ms=int(ts_sec * 1000))

    old_b = _block_at(100, now - 30 * 86400)   # 30 days ago
    mid_b = _block_at(200, now - 10 * 86400)   # 10 days ago
    new_b = _block_at(300, now - 3600)         # 1 hour ago
    storage.write_block(old_b)
    storage.write_block(mid_b)
    storage.write_block(new_b)

    # Aggregates attached to each block.
    storage.write_tx_contract_block([
        ContractBlockAgg(block_number=100, to_addr="0xaaa", tx_count=5, total_gas=1000),
        ContractBlockAgg(block_number=200, to_addr="0xbbb", tx_count=3, total_gas=500),
        ContractBlockAgg(block_number=300, to_addr="0xccc", tx_count=7, total_gas=2000),
    ])

    # Alerts: one 30d old, one 1h old.
    storage.write_alert(
        AlertEvent(rule="r", severity=Severity.WARN, key="k1", title="t1", detail="d1"),
        ts=now - 30 * 86400,
    )
    storage.write_alert(
        AlertEvent(rule="r", severity=Severity.WARN, key="k2", title="t2", detail="d2"),
        ts=now - 3600,
    )

    # Prune everything older than 14 days: expect the 30d rows to go,
    # the 10d-and-newer to stay.
    deleted = storage.prune_older_than(keep_days=14)
    assert deleted["blocks"] == 1
    assert deleted["tx_contract_block"] == 1
    assert deleted["alerts"] == 1

    # Verify what remains.
    remaining_blocks = [b.block_number for b in storage.load_recent_blocks(10)]
    assert remaining_blocks == [200, 300]
    assert storage.tx_contract_block_count() == 2
    remaining_alerts = storage.load_recent_alerts(10)
    assert len(remaining_alerts) == 1
    assert remaining_alerts[0].key == "k2"
    storage.close()


def test_sampled_blocks_bins_into_target_points(tmp_path: Path) -> None:
    """sampled_blocks collapses many blocks into a bounded point series."""
    import dataclasses
    storage = Storage(tmp_path / "state.db")
    # 1000 blocks with ts 1000, 1001, ..., 1999 ms. Spans 999ms.
    # Requesting 10 points -> bin_ms ≈ 99, so 10 bins each ~100 blocks.
    for n in range(1000):
        storage.write_block(dataclasses.replace(
            _mk_block(n, rtp=float(n % 100)),
            timestamp_ms=1000 + n,
        ))
    result = storage.sampled_blocks(from_ts_ms=1000, to_ts_ms=2000, target_points=10)
    # Should be at most 10 rows (may be one extra from boundary rounding).
    assert 8 <= len(result) <= 11
    # First bin starts near the window start.
    assert result[0]["t"] == 1000
    # n_first < n_last within each bin.
    for bin_ in result:
        assert bin_["n_first"] <= bin_["n_last"]
        # Averages are floats, not ints.
        assert isinstance(bin_["rtp_avg"], float)
    storage.close()


def test_sampled_blocks_empty_window_returns_empty(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "state.db")
    # No data in window, and to < from rejects early.
    assert storage.sampled_blocks(from_ts_ms=0, to_ts_ms=1000) == []
    assert storage.sampled_blocks(from_ts_ms=1000, to_ts_ms=999) == []
    storage.close()


def test_sampled_blocks_rtp_p95_between_avg_and_max(tmp_path: Path) -> None:
    """rtp_p95 lives between avg and max and ignores single-block outliers."""
    import dataclasses
    storage = Storage(tmp_path / "state.db")
    # One bin of 100 blocks: 99 @ 10% retry, 1 @ 100% (the outlier).
    #   avg = (99*10 + 100)/100 = 10.9
    #   max = 100
    #   p95 ≈ 10 (linear interp between block 95 and 96 in sorted order,
    #   both sit at 10% because only the last block is the outlier)
    for n in range(100):
        rtp = 100.0 if n == 99 else 10.0
        storage.write_block(dataclasses.replace(
            _mk_block(n, rtp=rtp),
            timestamp_ms=1000 + n,
        ))
    result = storage.sampled_blocks(from_ts_ms=1000, to_ts_ms=1100, target_points=1)
    assert len(result) == 1
    bin_ = result[0]
    # All three metrics are present and float.
    assert isinstance(bin_["rtp_p95"], float)
    # p95 must sit at or below max, and crucially be nowhere near the
    # 100% outlier — that's the whole point of using p95 over max for
    # the envelope. (Note: avg can exceed p95 when the distribution is
    # skewed by a rare large outlier, so we don't assert avg<=p95.)
    assert bin_["rtp_p95"] <= bin_["rtp_max"]
    assert bin_["rtp_max"] == 100.0
    assert bin_["rtp_p95"] < 50.0  # single outlier must NOT pull p95 up
    storage.close()


def test_prune_older_than_noop_when_keep_days_is_zero(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "state.db")
    storage.write_block(_mk_block(1))
    result = storage.prune_older_than(keep_days=0)
    assert result == {}
    assert storage.block_count() == 1
    storage.close()


def test_load_reorg_history_empty_db(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "state.db")
    count, last = storage.load_reorg_history()
    assert count == 0
    assert last is None
    storage.close()


def _seed_reorg(
    storage: Storage,
    *,
    block_number: int,
    old_id: str,
    new_id: str,
    ts: float,
) -> None:
    """Simulate a reorg as written by the real pipeline.

    Blocks table: INSERT OR IGNORE keeps the first-seen id → that's
    the "before" id. Alerts table: the rule builds
    ``reorg:<number>:<new_id>`` as the key.
    """
    storage.write_block(_mk_block(block_number))
    # Override the default mk_block id with the "before" id so the
    # test expects exactly that value. Use raw SQL since write_block
    # is INSERT OR IGNORE.
    with storage._lock:  # noqa: SLF001 — test harness
        storage._conn.execute(
            "UPDATE blocks SET block_id = ? WHERE block_number = ?",
            (old_id, block_number),
        )
    storage.write_alert(
        AlertEvent(
            rule="reorg",
            severity=Severity.CRITICAL,
            key=f"reorg:{block_number}:{new_id}",
            title="Chain reorg detected",
            detail=f"Block #{block_number} id changed: {old_id[:10]}… → {new_id[:10]}…. Total reorgs observed: 1.",
        ),
        ts=ts,
    )


def test_list_reorgs_reconstructs_both_ids(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "state.db")
    old_id = "0x" + "a" * 64
    new_id = "0x" + "b" * 64
    _seed_reorg(
        storage, block_number=100,
        old_id=old_id, new_id=new_id, ts=1776_000_000.0,
    )

    rows = storage.list_reorgs()
    assert len(rows) == 1
    r = rows[0]
    assert r["block_number"] == 100
    assert r["block_id_before"] == old_id
    assert r["block_id_after"] == new_id
    assert r["block"] is not None
    assert r["block"]["tx_count"] == 10  # from _mk_block default
    storage.close()


def test_list_reorgs_returns_newest_first(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "state.db")
    for i, ts in enumerate([1776_000_000.0, 1776_001_000.0, 1776_002_000.0]):
        _seed_reorg(
            storage, block_number=100 + i,
            old_id="0x" + f"{i:064x}", new_id="0x" + f"{i + 10:064x}",
            ts=ts,
        )
    rows = storage.list_reorgs()
    # Newest-first
    assert [r["block_number"] for r in rows] == [102, 101, 100]
    storage.close()


def test_list_reorgs_handles_missing_block_row(tmp_path: Path) -> None:
    """If the reorged block was never persisted (bootstrap edge case or
    post-retention pruning), list_reorgs must still surface the alert
    with block=None rather than silently dropping it or crashing.
    """
    storage = Storage(tmp_path / "state.db")
    new_id = "0x" + "c" * 64
    # Write alert WITHOUT writing a blocks row.
    storage.write_alert(
        AlertEvent(
            rule="reorg",
            severity=Severity.CRITICAL,
            key=f"reorg:777:{new_id}",
            title="Chain reorg detected",
            detail="Block #777 id changed: 0x...… → 0x...…. Total reorgs observed: 1.",
        ),
        ts=1776_000_000.0,
    )
    rows = storage.list_reorgs()
    assert len(rows) == 1
    assert rows[0]["block_number"] == 777
    assert rows[0]["block_id_after"] == new_id
    assert rows[0]["block"] is None  # no blocks row → no metrics
    assert rows[0]["block_id_before"] is None
    storage.close()


def test_get_reorg_trace_returns_neighbors(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "state.db")
    # Populate 10 blocks: 95..104.
    for n in range(95, 105):
        storage.write_block(_mk_block(n, tx=n))  # distinct tx_count per block
    old_id = "0x" + "a" * 64
    new_id = "0x" + "b" * 64
    # Override the reorged block's id to old_id.
    with storage._lock:  # noqa: SLF001
        storage._conn.execute(
            "UPDATE blocks SET block_id = ? WHERE block_number = 100",
            (old_id,),
        )
    storage.write_alert(
        AlertEvent(
            rule="reorg", severity=Severity.CRITICAL,
            key=f"reorg:100:{new_id}",
            title="Chain reorg detected",
            detail="Block #100 id changed: ... → ...",
        ),
        ts=1776_000_000.0,
    )

    trace = storage.get_reorg_trace(100, window=3)
    assert trace is not None
    assert trace["block_number"] == 100
    assert trace["block_id_before"] == old_id
    assert trace["block_id_after"] == new_id
    assert trace["window"] == 3
    # 3 before + reorg + 3 after = 7 blocks.
    assert [b["block_number"] for b in trace["blocks"]] == [97, 98, 99, 100, 101, 102, 103]
    # tx_count distinguishable — verify each row's data, not just shape.
    assert [b["tx_count"] for b in trace["blocks"]] == [97, 98, 99, 100, 101, 102, 103]
    storage.close()


def test_get_reorg_trace_returns_none_for_unknown(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "state.db")
    assert storage.get_reorg_trace(12345) is None
    storage.close()


def test_get_reorg_trace_window_zero_returns_only_reorged_block(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "state.db")
    for n in range(95, 105):
        storage.write_block(_mk_block(n))
    storage.write_alert(
        AlertEvent(
            rule="reorg", severity=Severity.CRITICAL,
            key="reorg:100:0x" + "b" * 64,
            title="t", detail="d",
        ),
        ts=1.0,
    )
    trace = storage.get_reorg_trace(100, window=0)
    assert trace is not None
    assert len(trace["blocks"]) == 1
    assert trace["blocks"][0]["block_number"] == 100
    storage.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
