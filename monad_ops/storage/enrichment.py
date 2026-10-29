"""Enrichment inventory queries over shared storage resources."""

from __future__ import annotations

import sqlite3
from _thread import LockType
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from monad_ops.storage import (
        ContractBlockAgg,
        ContractRetryStat,
        TxEnrichment,
        _classify_pattern,
    )

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

    def top_retried_contracts(
        self,
        since_block: int | None = None,
        since_ts_ms: int | None = None,
        until_block: int | None = None,
        until_ts_ms: int | None = None,
        min_appearances: int = 3,
        limit: int = 50,
    ) -> list[ContractRetryStat]:
        """Rank contracts by how often they appear in retried blocks.

        Correlation, not causation: the execution log reports a block's
        aggregate retry_pct but not which specific txs conflicted. A
        contract that appears consistently in high-rtp blocks is a
        strong suspect; a contract that only ever appears in rtp=0
        blocks is presumptively low-conflict (storage-slot independent).

        Counting: ``blocks_appeared`` and ``retried_blocks`` are both
        counted per-distinct-block so a contract with multiple txs in
        the same block is not double-counted. ``avg_rtp_of_blocks`` is
        the mean retry_pct across distinct blocks the contract touches.
        ``tx_count`` and ``total_gas`` aggregate every individual tx.
        """
        clauses: list[str] = []
        params: list[object] = []
        if since_block is not None:
            clauses.append("tcb.block_number >= ?")
            params.append(int(since_block))
        if since_ts_ms is not None:
            clauses.append("b.timestamp_ms >= ?")
            params.append(int(since_ts_ms))
        if until_block is not None:
            clauses.append("tcb.block_number <= ?")
            params.append(int(until_block))
        if until_ts_ms is not None:
            clauses.append("b.timestamp_ms <= ?")
            params.append(int(until_ts_ms))
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""

        # Read from the per-(block, to_addr) aggregate. One row per
        # (block, contract) means blocks_appeared = COUNT(*) directly —
        # no DISTINCT/CTE gymnastics needed, no double-counting possible
        # by construction. Per-tx totals come from SUM() of the rolled-up
        # tx_count / total_gas columns.
        sql = f"""
            SELECT tcb.to_addr                                              AS to_addr,
                   COUNT(*)                                                 AS blocks_appeared,
                   SUM(CASE WHEN b.retry_pct > 0 THEN 1 ELSE 0 END)         AS retried_blocks,
                   AVG(b.retry_pct)                                         AS avg_rtp_of_blocks,
                   SUM(tcb.tx_count)                                        AS tx_count,
                   SUM(tcb.total_gas)                                       AS total_gas
            FROM tx_contract_block tcb
            JOIN blocks b ON b.block_number = tcb.block_number
            {where}
            GROUP BY tcb.to_addr
            HAVING COUNT(*) >= ?
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

    def write_tx_enrichment(self, rows: list[TxEnrichment]) -> int:
        """Insert a batch of tx_enrichment rows. Returns the number attempted.

        INSERT OR IGNORE keeps this idempotent if the same block is
        enriched twice (e.g. after a service restart replays the queue).
        """
        if not rows:
            return 0
        params = [
            (r.block_number, r.tx_index, r.tx_hash, r.from_addr, r.to_addr,
             r.contract_created, r.status, r.gas_used)
            for r in rows
        ]
        with self._lock:
            self._conn.executemany(
                """INSERT OR IGNORE INTO tx_enrichment (
                    block_number, tx_index, tx_hash, from_addr, to_addr,
                    contract_created, status, gas_used
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                params,
            )
        return len(params)

    def write_tx_contract_block(self, rows: list[ContractBlockAgg]) -> int:
        """Upsert a batch of (block, to_addr) aggregate rows.

        INSERT OR REPLACE (not IGNORE) so that a re-enrichment of the
        same block overwrites any prior partial write. Safe because a
        block's receipts are deterministic — re-computing yields the
        same tx_count/total_gas.
        """
        if not rows:
            return 0
        params = [(r.block_number, r.to_addr, r.tx_count, r.total_gas) for r in rows]
        with self._lock:
            self._conn.executemany(
                """INSERT OR REPLACE INTO tx_contract_block (
                    block_number, to_addr, tx_count, total_gas
                ) VALUES (?, ?, ?, ?)""",
                params,
            )
        return len(params)

    def get_contract_detail(
        self,
        to_addr: str,
        *,
        from_ts_ms: int,
        to_ts_ms: int,
        dominance_thresholds_pct: tuple[int, ...] = (10, 30, 50),
        hourly_series_hours: int = 48,
    ) -> dict:
        """Full contract profile over a time window for the popup.

        Source tables:
          * ``tx_contract_block ⋈ blocks`` — exact per-block dominance,
            peak block, first/last seen, retry correlation buckets.
          * ``contract_hour`` — cheap hourly activity sparkline.

        Dominance is the contract's share of the block's tx_count. The
        correlation buckets report ``avg(retry_pct)`` and sample count
        for blocks where the contract's share met each threshold. The
        "baseline" is the average over every block the contract touched
        in the window — a like-for-like comparison (same set of blocks,
        different filter), not a chain-wide average, so the story the
        numbers tell is about dominance, not about hot moments.

        Returns a dict even when the contract has no activity in the
        window — callers get a well-formed empty shape rather than None.
        """
        addr = to_addr.strip().lower()
        from_ts = int(from_ts_ms)
        to_ts = int(to_ts_ms)
        # Build CASE expressions for dominance buckets. The threshold
        # compares tcb.tx_count * 100 >= b.tx_count * threshold — integer
        # math throughout, no float precision surprises at bucket edges.
        thresholds = tuple(
            sorted(set(int(t) for t in dominance_thresholds_pct if 0 < int(t) <= 100))
        )
        dom_case_exprs = []
        for t in thresholds:
            dom_case_exprs.append(
                f"SUM(CASE WHEN tcb.tx_count * 100 >= b.tx_count * {t} "
                f"THEN b.retry_pct ELSE 0 END) AS sum_rtp_dom_{t}"
            )
            dom_case_exprs.append(
                f"SUM(CASE WHEN tcb.tx_count * 100 >= b.tx_count * {t} "
                f"THEN 1 ELSE 0 END) AS count_dom_{t}"
            )
        dom_cols = ",\n                ".join(dom_case_exprs)
        # CROSS JOIN pins blocks first: idx_txcb_to has no time bound, so the natural plan
        # walks the contract's whole lifetime.
        sql_stats = f"""
            SELECT
                COUNT(*)                                          AS blocks_appeared,
                SUM(CASE WHEN b.retry_pct > 0 THEN 1 ELSE 0 END)  AS retried_blocks,
                AVG(b.retry_pct)                                  AS avg_rtp,
                SUM(tcb.tx_count)                                 AS tx_count,
                SUM(tcb.total_gas)                                AS total_gas,
                MIN(tcb.block_number)                             AS first_block,
                MAX(tcb.block_number)                             AS last_block,
                MIN(b.timestamp_ms)                               AS first_ts,
                MAX(b.timestamp_ms)                               AS last_ts,
                {dom_cols}
            FROM blocks b
            CROSS JOIN tx_contract_block tcb
                 ON tcb.block_number = b.block_number
                AND tcb.to_addr = ?
            WHERE b.timestamp_ms BETWEEN ? AND ?
        """
        with self._lock:
            stat_row = self._conn.execute(
                sql_stats, (addr, from_ts, to_ts)
            ).fetchone()

            peak_row = self._conn.execute(
                """SELECT tcb.block_number, tcb.tx_count, tcb.total_gas,
                          b.tx_count AS block_tx, b.retry_pct, b.timestamp_ms
                   FROM blocks b
                   CROSS JOIN tx_contract_block tcb
                        ON tcb.block_number = b.block_number
                       AND tcb.to_addr = ?
                   WHERE b.timestamp_ms BETWEEN ? AND ?
                   ORDER BY tcb.tx_count DESC, tcb.total_gas DESC
                   LIMIT 1""",
                (addr, from_ts, to_ts),
            ).fetchone()

            # Rank among all contracts in the window, by tx_count and by
            # total_gas. Counts contracts strictly ahead of this one, so
            # rank is 1-based and stable under ties (ties share the same
            # rank as the lowest among them).
            rank_row = self._conn.execute(
                """WITH my AS (
                     SELECT SUM(tcb.tx_count)   AS my_tx,
                            SUM(tcb.total_gas)  AS my_gas
                     FROM blocks b
                     CROSS JOIN tx_contract_block tcb
                          ON tcb.block_number = b.block_number
                         AND tcb.to_addr = ?
                     WHERE b.timestamp_ms BETWEEN ? AND ?
                   ),
                   peers AS (
                     SELECT tcb.to_addr,
                            SUM(tcb.tx_count)   AS total_tx,
                            SUM(tcb.total_gas)  AS total_gas
                     FROM tx_contract_block tcb
                     JOIN blocks b ON b.block_number = tcb.block_number
                     WHERE b.timestamp_ms BETWEEN ? AND ?
                     GROUP BY tcb.to_addr
                   )
                   SELECT
                     (SELECT 1 + COUNT(*) FROM peers, my
                        WHERE peers.total_tx > my.my_tx)     AS rank_tx,
                     (SELECT 1 + COUNT(*) FROM peers, my
                        WHERE peers.total_gas > my.my_gas)   AS rank_gas,
                     (SELECT COUNT(*) FROM peers)            AS peers_total""",
                (addr, from_ts, to_ts, from_ts, to_ts),
            ).fetchone()

            # Hourly sparkline — last N hours of contract_hour. Rollup
            # filter ba>=10 means hours where this contract had fewer
            # than 10 blocks are absent; for top-of-list contracts this
            # is invisible, for long-tail contracts the sparkline may
            # show gaps. Caveat documented in the schema.
            hourly_hours = max(1, min(int(hourly_series_hours), 24 * 30))
            hourly_from = to_ts - hourly_hours * 3_600_000
            hourly_from_aligned = (hourly_from // 3_600_000) * 3_600_000
            hourly_rows = self._conn.execute(
                """SELECT hour_ms, blocks_appeared, retried_blocks,
                          tx_count, total_gas
                   FROM contract_hour
                   WHERE to_addr = ? AND hour_ms >= ? AND hour_ms <= ?
                   ORDER BY hour_ms ASC""",
                (addr, hourly_from_aligned, to_ts),
            ).fetchall()

        blocks_appeared = int(stat_row["blocks_appeared"] or 0)
        tx_count_total = int(stat_row["tx_count"] or 0)
        total_gas = int(stat_row["total_gas"] or 0)

        stats = {
            "blocks_appeared": blocks_appeared,
            "retried_blocks": int(stat_row["retried_blocks"] or 0),
            "retried_ratio": round(
                int(stat_row["retried_blocks"] or 0) / blocks_appeared, 4
            ) if blocks_appeared else 0.0,
            "avg_rtp_in_blocks": round(float(stat_row["avg_rtp"] or 0.0), 2),
            "tx_count": tx_count_total,
            "total_gas": total_gas,
            "avg_gas_per_tx": int(total_gas / tx_count_total)
                              if tx_count_total else 0,
            "first_block": int(stat_row["first_block"])
                            if stat_row["first_block"] is not None else None,
            "last_block": int(stat_row["last_block"])
                           if stat_row["last_block"] is not None else None,
            "first_ts": int(stat_row["first_ts"])
                         if stat_row["first_ts"] is not None else None,
            "last_ts": int(stat_row["last_ts"])
                        if stat_row["last_ts"] is not None else None,
        }

        dominance = []
        for t in thresholds:
            cnt = int(stat_row[f"count_dom_{t}"] or 0)
            sum_rtp = float(stat_row[f"sum_rtp_dom_{t}"] or 0.0)
            dominance.append({
                "threshold_pct": t,
                "blocks": cnt,
                "avg_rtp": round(sum_rtp / cnt, 2) if cnt else 0.0,
            })

        peak_block = None
        if peak_row is not None and peak_row["block_number"] is not None:
            peak_tx = int(peak_row["tx_count"])
            block_tx = int(peak_row["block_tx"] or 0)
            peak_block = {
                "n": int(peak_row["block_number"]),
                "t": int(peak_row["timestamp_ms"]),
                "tx_count": peak_tx,
                "total_gas": int(peak_row["total_gas"]),
                "block_tx": block_tx,
                "share": round(peak_tx / block_tx, 4) if block_tx else 0.0,
                "rtp": float(peak_row["retry_pct"]),
            }

        rank = None
        if rank_row is not None:
            rank = {
                "by_tx": int(rank_row["rank_tx"]) if rank_row["rank_tx"] else None,
                "by_gas": int(rank_row["rank_gas"]) if rank_row["rank_gas"] else None,
                "peers": int(rank_row["peers_total"] or 0),
            }

        hourly = [
            {
                "hour_ms": int(r["hour_ms"]),
                "ba": int(r["blocks_appeared"]),
                "rb": int(r["retried_blocks"]),
                "tx": int(r["tx_count"]),
                "gas": int(r["total_gas"]),
            }
            for r in hourly_rows
        ]

        # Hide rank on an empty contract — returning 1/158 for a contract
        # with zero activity misleads the popup into claiming a top slot.
        if blocks_appeared == 0:
            rank = None

        pattern = _classify_pattern(blocks_appeared, tx_count_total)

        return {
            "to_addr": addr,
            "window": {"from_ts_ms": from_ts, "to_ts_ms": to_ts},
            "stats": stats,
            "dominance": dominance,
            "peak_block": peak_block,
            "rank": rank,
            "hourly": hourly,
            "pattern": pattern,
        }
