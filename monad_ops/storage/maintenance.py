"""Daily SQLite retention and planner maintenance.

The facade creates the shared connection, path and writer lock; planner
analysis continues to use its own connection without that lock.
"""

from __future__ import annotations

import sqlite3
import time
from _thread import LockType
from pathlib import Path


class _MaintenanceMixin:
    """Maintenance operations using the resources owned by ``Storage``."""

    _path: Path
    _conn: sqlite3.Connection
    _lock: LockType

    def prune_older_than(self, *, keep_days: int) -> dict[str, int]:
        """Delete rows older than ``keep_days`` across retention-managed tables.

        Tables pruned (cutoff derived from wall-clock now):
          * ``blocks``            — by ``timestamp_ms``
          * ``tx_contract_block`` — by ``block_number`` (joined to blocks)
          * ``tx_enrichment``     — same (empty today, kept for the
                                    future hot-tier)
          * ``contract_hour``     — by ``hour_ms``. Pruned separately
                                    with a longer retention window
                                    ``keep_days * 30`` so the hourly
                                    rollup doubles as a long-horizon
                                    archive: the raw table can shed
                                    rows at 48 h while the rollup keeps
                                    months of history cheaply.
          * ``alerts``            — by ``ts``

        Returns per-table deleted-row counts. Safe to run concurrently
        with the collector — sqlite's WAL mode handles readers cleanly
        and the writer serializes through ``self._lock``.

        Not VACUUMing automatically: free pages are reclaimed by
        subsequent inserts. Operator can run ``VACUUM INTO`` offline
        once per month if on-disk size matters more than cadence.
        """
        if keep_days <= 0:
            return {}
        cutoff_ms = int((time.time() - keep_days * 86400) * 1000)
        cutoff_s = cutoff_ms / 1000.0
        deleted: dict[str, int] = {}
        with self._lock:
            # Find the corresponding block_number cutoff once, so
            # tx_contract_block and tx_enrichment prune without a JOIN.
            row = self._conn.execute(
                "SELECT MIN(block_number) AS n FROM blocks WHERE timestamp_ms >= ?",
                (cutoff_ms,),
            ).fetchone()
            block_cutoff = int(row["n"]) if row and row["n"] is not None else None

            if block_cutoff is not None:
                cur = self._conn.execute(
                    "DELETE FROM tx_contract_block WHERE block_number < ?",
                    (block_cutoff,),
                )
                deleted["tx_contract_block"] = cur.rowcount
                cur = self._conn.execute(
                    "DELETE FROM tx_enrichment WHERE block_number < ?",
                    (block_cutoff,),
                )
                deleted["tx_enrichment"] = cur.rowcount
                cur = self._conn.execute(
                    "DELETE FROM blocks WHERE block_number < ?",
                    (block_cutoff,),
                )
                deleted["blocks"] = cur.rowcount
            # contract_hour prunes on its own, longer window. At ~500
            # rows/hour the rollup is cheap enough to keep for months
            # even after the raw tx_contract_block is gone.
            hour_cutoff = int((time.time() - keep_days * 30 * 86400) * 1000)
            cur = self._conn.execute(
                "DELETE FROM contract_hour WHERE hour_ms < ?",
                (hour_cutoff,),
            )
            deleted["contract_hour"] = cur.rowcount
            cur = self._conn.execute(
                "DELETE FROM alerts WHERE ts < ?",
                (cutoff_s,),
            )
            deleted["alerts"] = cur.rowcount
            # bft_minute prunes on the same window as blocks. At 1440
            # rows/day a 7-day retention is ~10 K rows total — could be
            # kept much longer cheaply, but matching block retention
            # keeps mental model simple ("the dashboard sees the same
            # window for both layers").
            cur = self._conn.execute(
                "DELETE FROM bft_minute WHERE ts_minute < ?",
                (cutoff_ms,),
            )
            deleted["bft_minute"] = cur.rowcount
            # bft_base_fee — same retention as blocks. At ~2.5 blocks/sec
            # 7 days of data is ~1.5 M rows; trivial to keep, but prune
            # in lockstep with the rest so an operator with tighter
            # retention sees a uniform window.
            cur = self._conn.execute(
                "DELETE FROM bft_base_fee WHERE ts_ms < ?",
                (cutoff_ms,),
            )
            deleted["bft_base_fee"] = cur.rowcount
        return deleted

    def refresh_planner_stats(self, *, analysis_limit: int = 1000) -> dict[str, int]:
        """Re-run a bounded ``ANALYZE`` so ``sqlite_stat1`` tracks the real
        table sizes.

        The stats were last gathered by hand in April; five months later
        they claimed 6.5M rows for a 124M-row ``tx_contract_block``, and
        the planner picks join orders from those numbers. ``analysis_limit``
        samples that many rows per index and extrapolates, which keeps a
        pass on a 30 GB file under a second.

        Runs on its own connection so it never sits inside ``self._lock``
        in front of the collector's writes.

        Returns ``{table: estimated_rows}`` for every table with stats.
        """
        conn = sqlite3.connect(str(self._path), check_same_thread=False)
        try:
            conn.execute("PRAGMA busy_timeout = 5000")
            conn.execute(f"PRAGMA analysis_limit = {int(analysis_limit)}")
            conn.execute("ANALYZE")
            rows = conn.execute(
                "SELECT tbl, stat FROM sqlite_stat1 WHERE idx IS NOT NULL"
            ).fetchall()
        finally:
            conn.close()
        est: dict[str, int] = {}
        for tbl, stat in rows:
            n = int(str(stat).split()[0])
            est[tbl] = max(est.get(tbl, 0), n)
        return est
