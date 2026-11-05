"""Slow contract-profile attribution without wall-clock delays or live data."""

from __future__ import annotations

from pathlib import Path

import pytest
from structlog.testing import capture_logs

from monad_ops.storage import Storage, enrichment
from tests.test_storage import _mk_block


class Clock:
    now = 0

    def advance_ms(self, value):
        self.now += value * 1_000_000

    def read(self):
        return self.now


class WaitingLock:
    def __init__(self, lock, clock):
        self.lock, self.clock = lock, clock

    def __enter__(self):
        self.clock.advance_ms(100)
        self.lock.acquire()

    def __exit__(self, *args):
        self.lock.release()


class TimedCursor:
    def __init__(self, cursor, clock, fetch_ms):
        self.cursor, self.clock, self.fetch_ms = cursor, clock, fetch_ms

    def fetchone(self):
        self.clock.advance_ms(self.fetch_ms)
        return self.cursor.fetchone()

    def fetchall(self):
        self.clock.advance_ms(self.fetch_ms)
        return self.cursor.fetchall()


class TimedConnection:
    def __init__(self, connection, clock):
        self.connection, self.clock = connection, clock
        self.durations = iter(((110, 40), (30, 20), (200, 60), (5, 35)))

    def execute(self, sql, parameters):
        execute_ms, fetch_ms = next(self.durations)
        self.clock.advance_ms(execute_ms)
        return TimedCursor(self.connection.execute(sql, parameters), self.clock, fetch_ms)


def test_slow_profile_attributes_shared_lock_and_all_sql_fetches(tmp_path, monkeypatch):
    from monad_ops.storage import ContractBlockAgg

    storage = Storage(tmp_path / "profile.db")
    address = "0x" + "ab" * 20
    storage.write_block(_mk_block(100, tx=10, rtp=50.0))
    storage.write_tx_contract_block([ContractBlockAgg(100, address, 5, 21000)])
    bounds = {"from_ts_ms": 0, "to_ts_ms": 10**18}
    queries = []
    storage._conn.set_trace_callback(queries.append)
    expected = storage.get_contract_detail(address, **bounds)
    original_queries = queries[:]
    queries.clear()
    connection, lock = storage._conn, storage._lock
    clock = Clock()
    monkeypatch.setattr(enrichment, "monotonic_ns", clock.read)
    storage._conn = TimedConnection(connection, clock)
    storage._lock = WaitingLock(lock, clock)
    try:
        with capture_logs() as logs:
            actual = storage.get_contract_detail(address, **bounds)
    finally:
        storage._conn, storage._lock = connection, lock
        storage.close()

    assert actual == expected
    assert queries == original_queries
    assert len(queries) == 4
    assert logs == [{
        "event": "contract.detail.slow",
        "log_level": "info",
        "elapsed_ms": 600.0,
        "lock_wait_ms": 100.0,
        "stats_ms": 150.0,
        "peak_ms": 50.0,
        "rank_ms": 260.0,
        "hourly_ms": 40.0,
        "window_ms": 10**18,
    }]


@pytest.mark.parametrize("elapsed_ns,logged", [(499_999_999, False), (500_000_000, True)])
def test_slow_profile_log_threshold(tmp_path: Path, monkeypatch, elapsed_ns, logged):
    storage = Storage(tmp_path / "profile.db")
    ticks = iter([0] * 7 + [elapsed_ns])
    monkeypatch.setattr(enrichment, "monotonic_ns", lambda: next(ticks))
    try:
        with capture_logs() as logs:
            result = storage.get_contract_detail("0x" + "00" * 20, from_ts_ms=0, to_ts_ms=1)
    finally:
        storage.close()

    assert result["stats"]["blocks_appeared"] == 0
    assert result["rank"] is None
    assert bool(logs) is logged
    if logged:
        assert len(logs) == 1
        assert logs[0]["elapsed_ms"] == 500.0
