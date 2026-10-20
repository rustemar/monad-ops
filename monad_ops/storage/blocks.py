"""Block inventory and detail queries over shared storage resources."""

from __future__ import annotations

import sqlite3
from _thread import LockType

from monad_ops.parser import ExecBlock

# gpse is logged in mega-gas/sec; scale on read so the stored column stays one unit.
_GAS_PER_SEC_SCALE = 1_000_000


class _BlockQueryMixin:
    """Block queries using the connection and lock owned by ``Storage``."""

    _conn: sqlite3.Connection
    _lock: LockType

    def block_count(self) -> int:
        with self._lock:
            row = self._conn.execute("SELECT COUNT(*) AS n FROM blocks").fetchone()
        return int(row["n"])

    def get_block_detail(
        self,
        block_number: int,
        *,
        neighbor_window: int = 25,
        top_contracts_limit: int = 5,
    ) -> dict | None:
        """Everything needed to render a single-block popup.

        Returns ``None`` if the block isn't persisted. Otherwise: the
        block row itself, the top-N contracts in that block ordered by
        tx_count (with their share of the block's total tx), and a
        compact neighbor window for a sparkline. Three cheap queries in
        one trip — each hits a covered index.
        """
        neighbor_window = max(0, min(int(neighbor_window), 500))
        top_contracts_limit = max(1, min(int(top_contracts_limit), 50))
        n = int(block_number)
        with self._lock:
            block_row = self._conn.execute(
                "SELECT * FROM blocks WHERE block_number = ?", (n,)
            ).fetchone()
            if block_row is None:
                return None

            top_rows = self._conn.execute(
                """SELECT to_addr, tx_count, total_gas
                   FROM tx_contract_block
                   WHERE block_number = ?
                   ORDER BY tx_count DESC, total_gas DESC
                   LIMIT ?""",
                (n, top_contracts_limit),
            ).fetchall()

            lo = n - neighbor_window
            hi = n + neighbor_window
            neighbor_rows = self._conn.execute(
                """SELECT block_number, timestamp_ms, tx_count, retry_pct,
                          gas_used, tps_effective
                   FROM blocks
                   WHERE block_number BETWEEN ? AND ?
                   ORDER BY block_number ASC""",
                (lo, hi),
            ).fetchall()

        block_tx_total = int(block_row["tx_count"]) or 0
        top_contracts = [
            {
                "to_addr": r["to_addr"],
                "tx_count": int(r["tx_count"]),
                "total_gas": int(r["total_gas"]),
                "share": round(int(r["tx_count"]) / block_tx_total, 4)
                         if block_tx_total > 0 else 0.0,
            }
            for r in top_rows
        ]
        neighbors = [
            {
                "n": int(r["block_number"]),
                "t": int(r["timestamp_ms"]),
                "tx": int(r["tx_count"]),
                "rtp": float(r["retry_pct"]),
                "gas": int(r["gas_used"]),
                "tpse": int(r["tps_effective"]),
            }
            for r in neighbor_rows
        ]
        block = {
            "n": int(block_row["block_number"]),
            "block_id": block_row["block_id"],
            "t": int(block_row["timestamp_ms"]),
            "tx": int(block_row["tx_count"]),
            "retried": int(block_row["retried"]),
            "rtp": float(block_row["retry_pct"]),
            "gas": int(block_row["gas_used"]),
            "tpse": int(block_row["tps_effective"]),
            "gpse": int(block_row["gas_per_sec_effective"]),
            "sr_us": int(block_row["state_reset_us"]),
            "te_us": int(block_row["tx_exec_us"]),
            "cm_us": int(block_row["commit_us"]),
            "tot_us": int(block_row["total_us"]),
        }
        return {
            "block": block,
            "top_contracts": top_contracts,
            "neighbors": neighbors,
        }

    def load_recent_blocks(self, limit: int) -> list[ExecBlock]:
        """Return the most-recent ``limit`` blocks in ascending order."""
        with self._lock:
            rows = self._conn.execute(
                """SELECT * FROM blocks
                   ORDER BY block_number DESC
                   LIMIT ?""",
                (int(limit),),
            ).fetchall()
        rows.reverse()
        return [_row_to_block(r) for r in rows]

    def load_blocks_range(
        self,
        from_block: int | None = None,
        to_block: int | None = None,
        from_ts_ms: int | None = None,
        to_ts_ms: int | None = None,
        limit: int = 5000,
    ) -> list[ExecBlock]:
        """Query blocks by block_number range or timestamp range."""
        clauses: list[str] = []
        params: list[object] = []
        if from_block is not None:
            clauses.append("block_number >= ?")
            params.append(int(from_block))
        if to_block is not None:
            clauses.append("block_number <= ?")
            params.append(int(to_block))
        if from_ts_ms is not None:
            clauses.append("timestamp_ms >= ?")
            params.append(int(from_ts_ms))
        if to_ts_ms is not None:
            clauses.append("timestamp_ms <= ?")
            params.append(int(to_ts_ms))

        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        sql = (
            f"SELECT * FROM blocks {where} "
            f"ORDER BY block_number ASC LIMIT ?"
        )
        params.append(max(1, min(int(limit), 50_000)))
        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()
        return [_row_to_block(r) for r in rows]

    def block_metrics_aggregate(
        self,
        *,
        from_ts_ms: int,
        to_ts_ms: int,
    ) -> dict:
        """Aggregate metrics for a window, computed in SQL.

        Replaces the older pattern of loading every block row into
        Python and computing peak/avg/sum from attributes — that
        materialised ~18 K row objects for a 2 h window and ~200 K for
        24 h. Moving the aggregation into a single SQL pass keeps the
        response fixed-size regardless of window span, which is what
        allows ``/api/window_summary`` to accept ranges up to 30 days
        without returning megabytes of per-block rows.

        Returns the dict shape expected by the endpoint (empty-window
        default is the caller's concern).
        """
        sql = """
            SELECT
                COUNT(*)                                AS blocks,
                MIN(timestamp_ms)                       AS first_ts,
                MAX(timestamp_ms)                       AS last_ts,
                MAX(retry_pct)                          AS peak_rtp,
                AVG(retry_pct)                          AS avg_rtp,
                MAX(gas_per_sec_effective)              AS peak_gps,
                SUM(gas_used)                           AS total_gas,
                MAX(tps_effective)                      AS peak_tps,
                SUM(tx_count)                           AS total_tx,
                SUM(retried)                            AS total_retried
            FROM blocks
            WHERE timestamp_ms >= ? AND timestamp_ms <= ?
        """
        with self._lock:
            row = self._conn.execute(sql, (int(from_ts_ms), int(to_ts_ms))).fetchone()
        n = int(row["blocks"] or 0)
        if n == 0:
            return {
                "blocks": 0, "span_ms": 0,
                "peak_rtp": 0.0, "avg_rtp": 0.0,
                "peak_gps": 0, "avg_gps": 0,
                "peak_tps": 0, "avg_tps": 0.0,
                "total_gas": 0, "total_tx": 0,
                "total_retried": 0, "retried_fraction": 0.0,
            }
        span_ms = int(row["last_ts"]) - int(row["first_ts"])
        span_sec = max(1, span_ms / 1000.0)
        total_tx = int(row["total_tx"] or 0)
        total_retried = int(row["total_retried"] or 0)
        total_gas = int(row["total_gas"] or 0)
        return {
            "blocks": n,
            "span_ms": span_ms,
            "peak_rtp": round(float(row["peak_rtp"] or 0), 2),
            "avg_rtp": round(float(row["avg_rtp"] or 0), 2),
            "peak_gps": int(row["peak_gps"] or 0) * _GAS_PER_SEC_SCALE,
            "avg_gps": int(total_gas / span_sec),
            "peak_tps": int(row["peak_tps"] or 0),
            "avg_tps": round(total_tx / span_sec, 2),
            "total_gas": total_gas,
            "total_tx": total_tx,
            "total_retried": total_retried,
            "retried_fraction": round(
                total_retried / total_tx if total_tx else 0.0, 4
            ),
        }

    def sampled_blocks(
        self,
        *,
        from_ts_ms: int,
        to_ts_ms: int,
        target_points: int = 300,
    ) -> list[dict]:
        """Downsampled blocks over an arbitrary time window.

        Returns at most ``target_points`` rows by bucketing the window
        into fixed-width time bins (``bin_ms = span / target_points``)
        and aggregating inside each bin. One SQL statement, uses
        ``idx_blocks_ts`` for range, avoids loading per-block data into
        Python — safe to call with a 7-day window (~1.5M blocks).

        Fields mirror the chart consumers:
          * n_first / n_last — first & last block_number in the bin
          * t                 — bin start ms (min timestamp in bin)
          * tx, rt, rtp_avg, rtp_p95, rtp_max — per-block metrics;
            p95 is the "most blocks were this bad or worse" signal
            used for the chart envelope (robust to single-block spikes)
          * tps_eff_max, gas_max     — parallelism ceilings in-bin
          * state_reset_us, tx_exec_us, commit_us — phase averages

        Empty bins are simply absent (sparse series); the client fills
        gaps if needed. `target_points=1` is valid (returns one row).
        """
        if to_ts_ms <= from_ts_ms:
            return []
        target_points = max(1, min(int(target_points), 2000))
        span_ms = to_ts_ms - from_ts_ms
        bin_ms = max(1, span_ms // target_points)
        with self._lock:
            rows = self._conn.execute(
                f"""
                SELECT
                    MIN(block_number)          AS n_first,
                    MAX(block_number)          AS n_last,
                    MIN(timestamp_ms)          AS t,
                    AVG(tx_count)              AS tx,
                    AVG(retried)               AS rt,
                    AVG(retry_pct)             AS rtp_avg,
                    percentile(retry_pct, 0.95) AS rtp_p95,
                    MAX(retry_pct)             AS rtp_max,
                    MAX(tps_effective)         AS tps_eff_max,
                    AVG(tps_effective)         AS tps_eff_avg,
                    MAX(gas_per_sec_effective) * {_GAS_PER_SEC_SCALE} AS gas_max,
                    AVG(state_reset_us)        AS state_reset_us,
                    AVG(tx_exec_us)            AS tx_exec_us,
                    AVG(commit_us)             AS commit_us,
                    MAX(storage_cache_size)    AS storage_cache_size_max,
                    MAX(active_chunks)         AS active_chunks_max,
                    COUNT(*)                   AS samples
                FROM blocks
                WHERE timestamp_ms BETWEEN ? AND ?
                GROUP BY (timestamp_ms - ?) / ?
                ORDER BY t ASC
                """,
                (int(from_ts_ms), int(to_ts_ms), int(from_ts_ms), int(bin_ms)),
            ).fetchall()
        return [dict(r) for r in rows]

    def write_block(self, b: ExecBlock) -> None:
        with self._lock:
            self._conn.execute(
                """INSERT OR IGNORE INTO blocks (
                    block_number, block_id, timestamp_ms, tx_count, retried,
                    retry_pct, state_reset_us, tx_exec_us, commit_us, total_us,
                    tps_effective, tps_avg, gas_used, gas_per_sec_effective,
                    gas_per_sec_avg, active_chunks, storage_cache_size
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    b.block_number, b.block_id, b.timestamp_ms, b.tx_count, b.retried,
                    b.retry_pct, b.state_reset_us, b.tx_exec_us, b.commit_us, b.total_us,
                    b.tps_effective, b.tps_avg, b.gas_used, b.gas_per_sec_effective,
                    b.gas_per_sec_avg, b.active_chunks, b.storage_cache_size,
                ),
            )


def _row_to_block(r: sqlite3.Row) -> ExecBlock:
    return ExecBlock(
        block_number=r["block_number"],
        block_id=r["block_id"],
        timestamp_ms=r["timestamp_ms"],
        tx_count=r["tx_count"],
        retried=r["retried"],
        retry_pct=r["retry_pct"],
        state_reset_us=r["state_reset_us"],
        tx_exec_us=r["tx_exec_us"],
        commit_us=r["commit_us"],
        total_us=r["total_us"],
        tps_effective=r["tps_effective"],
        tps_avg=r["tps_avg"],
        gas_used=r["gas_used"],
        gas_per_sec_effective=r["gas_per_sec_effective"],
        gas_per_sec_avg=r["gas_per_sec_avg"],
        active_chunks=r["active_chunks"],
        storage_cache_size=r["storage_cache_size"],
    )
