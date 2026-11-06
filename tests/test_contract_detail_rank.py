"""Exact profile ranking against the previous independent SQL formulation."""

from __future__ import annotations

from dataclasses import replace

import pytest

from monad_ops.storage import Storage
from tests.test_storage import _mk_block

_A, _B, _C, _D = ("0x" + char * 40 for char in "abcd")

# Keep the old separate target aggregation as an independent oracle.
_REFERENCE_RANK_SQL = """
WITH my AS (
    SELECT SUM(tcb.tx_count) AS my_tx, SUM(tcb.total_gas) AS my_gas
    FROM blocks b
    CROSS JOIN tx_contract_block tcb
        ON tcb.block_number = b.block_number AND tcb.to_addr = ?
    WHERE b.timestamp_ms BETWEEN ? AND ?
), peers AS (
    SELECT tcb.to_addr, SUM(tcb.tx_count) AS total_tx,
           SUM(tcb.total_gas) AS total_gas
    FROM tx_contract_block tcb
    JOIN blocks b ON b.block_number = tcb.block_number
    WHERE b.timestamp_ms BETWEEN ? AND ?
    GROUP BY tcb.to_addr
)
SELECT
    (SELECT 1 + COUNT(*) FROM peers, my WHERE peers.total_tx > my.my_tx) AS rank_tx,
    (SELECT 1 + COUNT(*) FROM peers, my WHERE peers.total_gas > my.my_gas) AS rank_gas,
    (SELECT COUNT(*) FROM peers) AS peers_total
"""


class RecordingConnection:
    def __init__(self, connection):
        self.connection = connection
        self.calls = []

    def execute(self, sql, parameters):
        self.calls.append((sql, parameters))
        return self.connection.execute(sql, parameters)


@pytest.mark.parametrize("rows,bounds,expected", [
    ([], (100, 200), None),
    ([(1, 100, _B, 3, 30)], (100, 200), None),
    ([(1, 100, _A, 0, 0), (2, 200, _B, 1, 1)], (100, 200), (2, 2, 2)),
    ([(1, 100, _A, 2, 10), (1, 100, _B, 2, 10),
      (1, 100, _C, 3, 5), (1, 100, _D, 1, 20)], (100, 200), (2, 2, 4)),
    ([(1, 100, _A, 8, 2), (1, 100, _B, 7, 3),
      (1, 100, _C, 6, 4)], (100, 200), (1, 3, 3)),
    ([(1, 99, _A, 999, 999), (2, 100, _A, 2, 2),
      (3, 200, _A, 3, 3), (4, 150, _B, 4, 4),
      (5, 201, _C, 999, 999), (6, None, _D, 999, 999)],
     (100, 200), (1, 1, 2)),
    ([(1, 100, _A, 2, 2), (2, 101, _B, 3, 3)], (100, 100), (1, 1, 1)),
    ([(1, 100, _A, 2, 2)], (200, 100), None),
    ([(1, 100, _A, 0.75, 1.75), (1, 100, _B, 0.5, 1.5)],
     (100, 200), (1, 1, 2)),
    ([(1, 100, _A, 2**53, 2**53 + 1), (1, 100, _B, 2**53 + 1, 2**53)],
     (100, 200), (2, 1, 2)),
    ([(1, 100, _A, -2, -4), (1, 100, _B, -1, -5)],
     (100, 200), (2, 1, 2)),
    ([(1, None, _A, 999, 999)], (100, 200), None),
], ids=[
    "empty", "missing-target", "zero", "ties", "different-ranks", "inclusive-and-orphan",
    "equal-bounds", "reversed-bounds", "raw-fractional-sums", "exact-large-integers",
    "negative-values", "orphan-only",
])
def test_profile_rank_matches_previous_sql(tmp_path, rows, bounds, expected):
    storage = Storage(tmp_path / "profile.db")
    connection = storage._conn
    recorder = RecordingConnection(connection)
    try:
        for number, timestamp, address, tx_count, gas in rows:
            if timestamp is not None:
                storage.write_block(replace(_mk_block(number), timestamp_ms=timestamp))
            connection.execute(
                "INSERT INTO tx_contract_block VALUES (?, ?, ?, ?)",
                (number, address, tx_count, gas),
            )
        storage._conn = recorder
        result = storage.get_contract_detail(
            "  " + _A.upper() + "  ", from_ts_ms=bounds[0], to_ts_ms=bounds[1],
        )
        reference = connection.execute(
            _REFERENCE_RANK_SQL, (_A, *bounds, *bounds),
        ).fetchone()
        rank_sql, rank_parameters = recorder.calls[2]
        raw_rank = connection.execute(rank_sql, rank_parameters).fetchone()

        assert tuple(raw_rank) == tuple(reference)
        actual = result["rank"]
        assert actual == (
            dict(zip(("by_tx", "by_gas", "peers"), expected, strict=True))
            if expected is not None else None
        )
        if not rows or all(address != _A or timestamp is None
                           for _, timestamp, address, _, _ in rows):
            assert rank_parameters[-2:] == (None, None)
    finally:
        storage._conn = connection
        storage.close()


def test_empty_stats_window_preserves_independent_hourly_history(tmp_path):
    storage = Storage(tmp_path / "profile.db")
    try:
        storage._conn.execute(
            "INSERT INTO contract_hour VALUES (?, ?, ?, ?, ?, ?, ?)",
            (3_600_000, _A, 12, 3, 150.0, 24, 48),
        )
        result = storage.get_contract_detail(
            _A, from_ts_ms=7_200_000, to_ts_ms=10_800_000,
        )
        assert result["rank"] is None
        assert result["stats"]["blocks_appeared"] == 0
        assert result["hourly"] == [{
            "hour_ms": 3_600_000, "ba": 12, "rb": 3, "tx": 24, "gas": 48,
        }]
    finally:
        storage.close()
