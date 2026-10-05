"""Consensus-minute series and window queries over shared storage resources."""

from __future__ import annotations

import sqlite3
from _thread import LockType


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
