"""Enrichment inventory queries over shared storage resources."""

from __future__ import annotations

import sqlite3
from _thread import LockType
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from monad_ops.storage import ContractRetryStat

_HOUR_MS = 3_600_000

# Minimum per-hour blocks_appeared for a (hour, to_addr) row to be kept
# in the rollup. See contract_hour schema comment for the measurement
# that justifies this threshold.
_CONTRACT_HOUR_MIN_BA = 10


class _EnrichmentQueryMixin:
    """Enrichment queries using the connection and lock owned by ``Storage``."""

    _conn: sqlite3.Connection
    _lock: LockType

    def tx_enrichment_count(self) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) AS n FROM tx_enrichment"
            ).fetchone()
        return int(row["n"])

    def tx_contract_block_count(self) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) AS n FROM tx_contract_block"
            ).fetchone()
        return int(row["n"])

    def contract_hour_count(self) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) AS n FROM contract_hour"
            ).fetchone()
        return int(row["n"])

    def contract_hour_range(self) -> tuple[int | None, int | None]:
        """Return (min_hour_ms, max_hour_ms) present in the rollup, or
        ``(None, None)`` if empty. Used by the rebuild task to decide
        whether a full backfill is needed."""
        with self._lock:
            row = self._conn.execute(
                "SELECT MIN(hour_ms) AS lo, MAX(hour_ms) AS hi FROM contract_hour"
            ).fetchone()
        if row is None or row["lo"] is None:
            return (None, None)
        return (int(row["lo"]), int(row["hi"]))

    def enrichment_has_block(self, block_number: int) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM tx_enrichment WHERE block_number = ? LIMIT 1",
                (int(block_number),),
            ).fetchone()
        return row is not None

    def rebuild_contract_hour(
        self, from_hour_ms: int, to_hour_ms: int
    ) -> int:
        """Rebuild rollup for hours in ``[from_hour_ms, to_hour_ms)``.

        DELETE+INSERT within a single transaction so concurrent readers
        never see a half-written bucket. Hours outside the window are
        untouched.

        Returns the number of rollup rows written.

        Cost model:
          * 2 h window on live data ≈ 2 × 9 000 blocks × ~50 contracts
            = ~900 K source rows → ~1 000 rollup rows. ~200 ms.
          * Full backfill of 20 M source rows → ~80 K rollup rows.
            ~60 s first time; only run once (or after a schema reset).
        """
        if to_hour_ms <= from_hour_ms:
            return 0
        # Align bounds to hour boundaries so a caller that passes raw
        # timestamps still gets clean buckets. (from_hour_ms floored,
        # to_hour_ms ceiled.)
        from_aligned = (int(from_hour_ms) // _HOUR_MS) * _HOUR_MS
        to_aligned = -(-int(to_hour_ms) // _HOUR_MS) * _HOUR_MS
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._conn.execute(
                    "DELETE FROM contract_hour "
                    "WHERE hour_ms >= ? AND hour_ms < ?",
                    (from_aligned, to_aligned),
                )
                cur = self._conn.execute(
                    """
                    INSERT INTO contract_hour (
                        hour_ms, to_addr,
                        blocks_appeared, retried_blocks,
                        sum_rtp, tx_count, total_gas
                    )
                    SELECT
                        (b.timestamp_ms / ?) * ?                              AS hour_ms,
                        tcb.to_addr                                            AS to_addr,
                        COUNT(*)                                               AS blocks_appeared,
                        SUM(CASE WHEN b.retry_pct > 0 THEN 1 ELSE 0 END)       AS retried_blocks,
                        SUM(b.retry_pct)                                       AS sum_rtp,
                        SUM(tcb.tx_count)                                      AS tx_count,
                        SUM(tcb.total_gas)                                     AS total_gas
                    FROM tx_contract_block tcb
                    JOIN blocks b ON b.block_number = tcb.block_number
                    WHERE b.timestamp_ms >= ? AND b.timestamp_ms < ?
                    GROUP BY hour_ms, tcb.to_addr
                    HAVING COUNT(*) >= ?
                    """,
                    (_HOUR_MS, _HOUR_MS, from_aligned, to_aligned, _CONTRACT_HOUR_MIN_BA),
                )
                written = cur.rowcount
                self._conn.execute("COMMIT")
                return int(written) if written is not None else 0
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    def backfill_contract_hour(self) -> int:
        """Rebuild the rollup for every hour that has source data.

        Called once on startup if ``contract_hour`` is empty. Derives the
        hour range from ``blocks.timestamp_ms`` so partial datasets
        backfill correctly.

        Expensive (20 M-row scan at current scale) — runs in a worker
        thread via ``asyncio.to_thread``; callers must not block the
        event loop on it.
        """
        with self._lock:
            row = self._conn.execute(
                "SELECT MIN(timestamp_ms) AS lo, MAX(timestamp_ms) AS hi FROM blocks"
            ).fetchone()
        if row is None or row["lo"] is None:
            return 0
        lo = int(row["lo"])
        hi = int(row["hi"])
        return self.rebuild_contract_hour(lo, hi + _HOUR_MS)

    def top_retried_contracts_rollup(
        self,
        since_ts_ms: int | None = None,
        until_ts_ms: int | None = None,
        min_appearances: int = 3,
        limit: int = 50,
    ) -> list[ContractRetryStat]:
        """Top retried contracts from the hourly rollup.

        Semantically equivalent to ``top_retried_contracts`` with
        timestamp filters, but scans ~1 000× fewer rows because the
        per-(block, addr) data is pre-aggregated into hourly buckets.

        Boundary notes:
          * Filters are aligned to hour boundaries — the effective window
            is ``[floor(since, 1h), ceil(until, 1h))``. A "last 1 h"
            query crossing an hour boundary can span up to 2 buckets.
            For ranking purposes this is acceptable distortion; exact
            per-block precision requires ``top_retried_contracts``.
          * ``blocks_appeared`` counts distinct blocks (per the invariant
            of one tx_contract_block row per (block, to_addr)); rollup
            SUMs preserve this.
          * ``avg_rtp_of_blocks`` = Σsum_rtp / Σblocks_appeared — the
            correctly weighted mean across buckets.
        """
        clauses: list[str] = []
        params: list[object] = []
        if since_ts_ms is not None:
            from_aligned = (int(since_ts_ms) // _HOUR_MS) * _HOUR_MS
            clauses.append("hour_ms >= ?")
            params.append(from_aligned)
        if until_ts_ms is not None:
            to_aligned = -(-int(until_ts_ms) // _HOUR_MS) * _HOUR_MS
            clauses.append("hour_ms < ?")
            params.append(to_aligned)
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""

        sql = f"""
            SELECT to_addr                                               AS to_addr,
                   SUM(blocks_appeared)                                  AS blocks_appeared,
                   SUM(retried_blocks)                                   AS retried_blocks,
                   CASE WHEN SUM(blocks_appeared) > 0
                        THEN SUM(sum_rtp) * 1.0 / SUM(blocks_appeared)
                        ELSE 0.0
                   END                                                   AS avg_rtp_of_blocks,
                   SUM(tx_count)                                         AS tx_count,
                   SUM(total_gas)                                        AS total_gas
            FROM contract_hour
            {where}
            GROUP BY to_addr
            HAVING SUM(blocks_appeared) >= ?
            ORDER BY retried_blocks DESC, avg_rtp_of_blocks DESC
            LIMIT ?
        """
        final_params = params + [
            int(min_appearances),
            max(1, min(int(limit), 500)),
        ]
        with self._lock:
            rows = self._conn.execute(sql, final_params).fetchall()
        return [
            ContractRetryStat(
                to_addr=r["to_addr"],
                blocks_appeared=int(r["blocks_appeared"]),
                retried_blocks=int(r["retried_blocks"]),
                avg_rtp_of_blocks=float(r["avg_rtp_of_blocks"] or 0.0),
                tx_count=int(r["tx_count"]),
                total_gas=int(r["total_gas"]),
            )
            for r in rows
        ]
