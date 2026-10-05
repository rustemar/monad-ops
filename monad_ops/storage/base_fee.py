"""Base-fee series and window queries over the facade's shared resources."""

from __future__ import annotations

import sqlite3
from _thread import LockType


class _BaseFeeQueryMixin:
    """Base-fee queries using the connection and lock owned by ``Storage``."""

    _conn: sqlite3.Connection
    _lock: LockType

    def list_bft_base_fee(
        self,
        from_ts_ms: int,
        to_ts_ms: int,
        *,
        limit: int = 5000,
    ) -> list[dict]:
        """Per-block base-fee rows in [from_ts_ms, to_ts_ms].

        Returns dicts with block_seq, t (ms), base_fee_gwei (precomputed
        from wei for chart consumers). Hard limit caps the response at
        5000 rows — at testnet's ~2.5 blocks/sec that's ~33 minutes;
        wider visible ranges should switch to a sampled-bins approach
        if the chart ever needs them.
        """
        with self._lock:
            rows = self._conn.execute(
                """SELECT block_seq, ts_ms, base_fee_wei
                   FROM bft_base_fee
                   WHERE ts_ms >= ? AND ts_ms <= ?
                   ORDER BY ts_ms ASC
                   LIMIT ?""",
                (int(from_ts_ms), int(to_ts_ms), max(1, int(limit))),
            ).fetchall()
        return [
            {
                "block_seq": int(r["block_seq"]),
                "t": int(r["ts_ms"]),
                # Convert wei → gwei for the chart. base_fee values on
                # testnet sit at the 100-gwei floor (= 1e11 wei) and
                # well within JS Number safe integer range either way,
                # so this conversion never loses precision.
                "base_fee_gwei": int(r["base_fee_wei"]) / 1_000_000_000,
            }
            for r in rows
        ]

    def sampled_bft_base_fee(
        self,
        from_ts_ms: int,
        to_ts_ms: int,
        *,
        target_points: int = 300,
    ) -> list[dict]:
        """Downsampled base-fee series for arbitrary windows.

        Mirrors ``sampled_blocks`` shape: bucket the window into fixed-
        width time bins, return per-bin avg + min/max for the chart
        envelope. Lets the dashboard show a 24h base-fee curve without
        moving 200 K rows.
        """
        if to_ts_ms <= from_ts_ms:
            return []
        target_points = max(1, min(int(target_points), 2000))
        span_ms = to_ts_ms - from_ts_ms
        bin_ms = max(1, span_ms // target_points)
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT
                    MIN(ts_ms)              AS t,
                    MIN(block_seq)          AS n_first,
                    MAX(block_seq)          AS n_last,
                    AVG(base_fee_wei)       AS avg_wei,
                    MIN(base_fee_wei)       AS min_wei,
                    MAX(base_fee_wei)       AS max_wei,
                    COUNT(*)                AS samples
                FROM bft_base_fee
                WHERE ts_ms BETWEEN ? AND ?
                GROUP BY (ts_ms - ?) / ?
                ORDER BY t ASC
                """,
                (int(from_ts_ms), int(to_ts_ms), int(from_ts_ms), int(bin_ms)),
            ).fetchall()
        return [
            {
                "t": int(r["t"]),
                "n_first": int(r["n_first"]),
                "n_last": int(r["n_last"]),
                "base_fee_gwei_avg": float(r["avg_wei"] or 0) / 1_000_000_000,
                "base_fee_gwei_min": float(r["min_wei"] or 0) / 1_000_000_000,
                "base_fee_gwei_max": float(r["max_wei"] or 0) / 1_000_000_000,
                "samples": int(r["samples"]),
            }
            for r in rows
        ]

    def load_base_fee_window(self, from_ts_ms: int, to_ts_ms: int) -> dict:
        """Base-fee aggregate over an arbitrary time window.

        Used by /api/window_summary so a Foundation-replay query for
        epochs 532/533/534 returns the headline fee numbers (avg/min/
        max in gwei + sample count) without pulling per-block rows.
        """
        with self._lock:
            row = self._conn.execute(
                """SELECT COUNT(*)        AS samples,
                          AVG(base_fee_wei) AS avg_wei,
                          MIN(base_fee_wei) AS min_wei,
                          MAX(base_fee_wei) AS max_wei,
                          MIN(ts_ms)        AS first_ts,
                          MAX(ts_ms)        AS last_ts
                   FROM bft_base_fee
                   WHERE ts_ms >= ? AND ts_ms <= ?""",
                (int(from_ts_ms), int(to_ts_ms)),
            ).fetchone()
        n = int(row["samples"] or 0)
        if n == 0:
            return {
                "samples": 0,
                "base_fee_gwei_avg": 0.0,
                "base_fee_gwei_min": 0.0,
                "base_fee_gwei_max": 0.0,
                "first_ts": None,
                "last_ts": None,
            }
        return {
            "samples": n,
            "base_fee_gwei_avg": round(float(row["avg_wei"]) / 1_000_000_000, 4),
            "base_fee_gwei_min": round(float(row["min_wei"]) / 1_000_000_000, 4),
            "base_fee_gwei_max": round(float(row["max_wei"]) / 1_000_000_000, 4),
            "first_ts": int(row["first_ts"]),
            "last_ts": int(row["last_ts"]),
        }
