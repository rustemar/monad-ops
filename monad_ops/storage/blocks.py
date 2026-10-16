"""Block inventory and detail queries over shared storage resources."""

from __future__ import annotations

import sqlite3
from _thread import LockType


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
