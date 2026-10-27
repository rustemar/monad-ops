"""Consensus-minute series and window queries over shared storage resources."""

from __future__ import annotations

import sqlite3
from _thread import LockType
from collections.abc import Iterable
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from monad_ops.storage import BftBaseFee, BftMinute


class _ConsensusQueryMixin:
    """Consensus queries using the connection and lock owned by ``Storage``."""

    _conn: sqlite3.Connection
    _lock: LockType

    def list_bft_minutes(
        self,
        from_ts_ms: int,
        to_ts_ms: int,
        *,
        limit: int = 10_500,
    ) -> list[dict]:
        """Per-minute rows in [from_ts_ms, to_ts_ms] for the chart.

        Returns dicts (not BftMinute) with ``timeout_pct`` precomputed
        so the dashboard doesn't have to repeat the division per point.
        Default limit holds the chart toolbar's 7d max (10080 buckets +
        slack); the API endpoint also bounds from/to span before calling.
        """
        with self._lock:
            rows = self._conn.execute(
                """SELECT ts_minute, rounds_total, rounds_tc, local_timeouts,
                          decrypt_fails, session_timeouts, timestamp_invalids
                   FROM bft_minute
                   WHERE ts_minute >= ? AND ts_minute <= ?
                   ORDER BY ts_minute ASC
                   LIMIT ?""",
                (int(from_ts_ms), int(to_ts_ms), max(1, int(limit))),
            ).fetchall()
        out = []
        for r in rows:
            total = int(r["rounds_total"])
            tc = int(r["rounds_tc"])
            out.append({
                "t": int(r["ts_minute"]),
                "rounds_total": total,
                "rounds_tc": tc,
                "local_timeouts": int(r["local_timeouts"]),
                "timeout_pct": round(tc / total * 100, 2) if total else 0.0,
                "decrypt_fails": int(r["decrypt_fails"]),
                "session_timeouts": int(r["session_timeouts"]),
                "timestamp_invalids": int(r["timestamp_invalids"]),
            })
        return out

    def load_bft_window(self, from_ts_ms: int, to_ts_ms: int) -> dict:
        """Aggregate bft counters over an arbitrary time window.

        Used by /api/window_summary so a Foundation-replay query for
        epochs 532/533/534 returns a chain-wide validator-timeout %
        for that exact window. SUMs over minute buckets so cost is
        O(window-minutes), not O(events).
        """
        with self._lock:
            row = self._conn.execute(
                """SELECT
                       SUM(rounds_total)   AS rt,
                       SUM(rounds_tc)      AS tc,
                       SUM(local_timeouts) AS lt,
                       COUNT(*)            AS minutes,
                       MIN(ts_minute)      AS first_minute,
                       MAX(ts_minute)      AS last_minute
                   FROM bft_minute
                   WHERE ts_minute >= ? AND ts_minute <= ?""",
                (int(from_ts_ms), int(to_ts_ms)),
            ).fetchone()
        rounds_total = int(row["rt"] or 0)
        rounds_tc = int(row["tc"] or 0)
        local_timeouts = int(row["lt"] or 0)
        minutes = int(row["minutes"] or 0)
        return {
            "minutes": minutes,
            "rounds_total": rounds_total,
            "rounds_tc": rounds_tc,
            "local_timeouts": local_timeouts,
            "validator_timeout_pct": round(
                rounds_tc / rounds_total * 100, 2
            ) if rounds_total else 0.0,
            "local_timeout_per_min": round(
                local_timeouts / minutes, 2
            ) if minutes else 0.0,
            "first_minute": int(row["first_minute"]) if row["first_minute"] is not None else None,
            "last_minute": int(row["last_minute"]) if row["last_minute"] is not None else None,
        }

    def upsert_bft_minute(self, m: BftMinute) -> None:
        """Insert or replace one minute-bucket of consensus counters.

        REPLACE semantics so the collector can flush the in-progress
        minute incrementally — each event observed within the minute
        rewrites the row with cumulative counts. The next minute starts
        a fresh row by virtue of the PK being the floored timestamp.
        """
        with self._lock:
            self._conn.execute(
                """INSERT OR REPLACE INTO bft_minute
                   (ts_minute, rounds_total, rounds_tc, local_timeouts,
                    decrypt_fails, session_timeouts, timestamp_invalids)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (int(m.ts_minute), int(m.rounds_total),
                 int(m.rounds_tc), int(m.local_timeouts),
                 int(m.decrypt_fails), int(m.session_timeouts),
                 int(m.timestamp_invalids)),
            )

    def load_recent_bft_minutes(self, limit: int) -> list[BftMinute]:
        """Return the most-recent ``limit`` minute buckets ascending.

        Used by State.bootstrap_bft_from_storage so a service restart
        immediately has 5m / 1h windows of consensus data without
        waiting for the live tailer to fill them.
        """
        with self._lock:
            rows = self._conn.execute(
                """SELECT ts_minute, rounds_total, rounds_tc, local_timeouts,
                          decrypt_fails, session_timeouts, timestamp_invalids
                   FROM bft_minute
                   ORDER BY ts_minute DESC
                   LIMIT ?""",
                (max(1, int(limit)),),
            ).fetchall()
        rows.reverse()
        return [
            BftMinute(
                ts_minute=int(r["ts_minute"]),
                rounds_total=int(r["rounds_total"]),
                rounds_tc=int(r["rounds_tc"]),
                local_timeouts=int(r["local_timeouts"]),
                decrypt_fails=int(r["decrypt_fails"]),
                session_timeouts=int(r["session_timeouts"]),
                timestamp_invalids=int(r["timestamp_invalids"]),
            )
            for r in rows
        ]

    def bulk_write_bft(
        self,
        minutes: Iterable[BftMinute],
        base_fees: Iterable[BftBaseFee],
    ) -> tuple[int, int]:
        """Write a batch of bft minute upserts + base-fee inserts in one txn.

        Drives the iter-20 write-amplification fix: instead of one
        ``upsert_bft_minute`` + one ``insert_bft_base_fee`` per event
        (~5 storage writes/sec at testnet cadence), the bft_flush_loop
        drains a 1-second buffer through this single-transaction path,
        cutting lock-acquire cost ~5x and giving the contract_hour
        rebuild + retention sweeps a quieter window to grab the writer
        lock when they need it.

        Idempotent — REPLACE on bft_minute means the in-progress minute
        can be flushed every second without growing rows; IGNORE on
        bft_base_fee dedup the same proposal seen across rebroadcasts
        and across overlapping flush batches.

        Returns (minutes_written, base_fees_written) as a coarse
        observability signal for the flush-loop logger.
        """
        minutes = list(minutes)
        base_fees = list(base_fees)
        if not minutes and not base_fees:
            return (0, 0)
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                if minutes:
                    self._conn.executemany(
                        """INSERT OR REPLACE INTO bft_minute
                           (ts_minute, rounds_total, rounds_tc, local_timeouts,
                            decrypt_fails, session_timeouts, timestamp_invalids)
                           VALUES (?, ?, ?, ?, ?, ?, ?)""",
                        [
                            (int(m.ts_minute), int(m.rounds_total),
                             int(m.rounds_tc), int(m.local_timeouts),
                             int(m.decrypt_fails), int(m.session_timeouts),
                             int(m.timestamp_invalids))
                            for m in minutes
                        ],
                    )
                if base_fees:
                    self._conn.executemany(
                        """INSERT OR IGNORE INTO bft_base_fee
                           (block_seq, ts_ms, base_fee_wei, author)
                           VALUES (?, ?, ?, ?)""",
                        [
                            (int(b.block_seq), int(b.ts_ms),
                             int(b.base_fee_wei), b.author)
                            for b in base_fees
                        ],
                    )
                self._conn.execute("COMMIT")
                return (len(minutes), len(base_fees))
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    def insert_bft_base_fee(self, sample: BftBaseFee) -> None:
        """Insert one base-fee observation, ignoring duplicates.

        IGNORE (not REPLACE) because monad-bft rebroadcasts proposals
        2-3× per round; the base_fee value is fixed at proposal time
        so the first observation is canonical and later rebroadcasts
        carry the same wei value. IGNORE also prevents a malicious
        rewrite if a peer ever sends an inconsistent rebroadcast.
        """
        with self._lock:
            self._conn.execute(
                """INSERT OR IGNORE INTO bft_base_fee
                   (block_seq, ts_ms, base_fee_wei, author)
                   VALUES (?, ?, ?, ?)""",
                (int(sample.block_seq), int(sample.ts_ms),
                 int(sample.base_fee_wei), sample.author),
            )
