"""Reorg counters and timeline queries over shared storage resources."""

from __future__ import annotations

import sqlite3
from _thread import LockType


class _ReorgQueryMixin:
    """Reorg queries using the connection and lock owned by ``Storage``."""

    _conn: sqlite3.Connection
    _lock: LockType

    def count_reorgs_since(self, since_ts: float) -> int:
        """Count reorg alerts with ``ts >= since_ts``.

        Used by the dashboard's "recent reorgs" badge — it asks "how many
        reorgs in the last 24h?" without scanning the full alert history.
        """
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) AS n FROM alerts "
                "WHERE rule = 'reorg' AND ts >= ?",
                (float(since_ts),),
            ).fetchone()
        return int(row["n"]) if row else 0

    def count_cluster_reorgs_since(self, since_ts: float) -> int:
        """Count cluster-grade reorgs (WARN severity) since ``since_ts``.

        Post-2026-05-03 reframe, the WARN tier on reorgs is reserved for
        cluster events (≥3 within 30 min) — single divergences are INFO.
        Surfacing the cluster count alongside the total lets the
        dashboard distinguish "noisy day" from "actual instability
        burst" without a second SQL pass per render.
        """
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) AS n FROM alerts "
                "WHERE rule = 'reorg' AND severity = 'warn' AND ts >= ?",
                (float(since_ts),),
            ).fetchone()
        return int(row["n"]) if row else 0

    def list_reorg_timestamps_since(self, since_ts: float) -> list[float]:
        """Return ``ts`` (unix seconds) of reorg alerts since ``since_ts``.

        Used by ReorgRule to rehydrate its cluster-detection window after
        a process restart — without it, the next reorg post-restart would
        always be classified as a single (WARN) event even when several
        already fired inside the cluster window.
        """
        with self._lock:
            rows = self._conn.execute(
                "SELECT ts FROM alerts "
                "WHERE rule = 'reorg' AND ts >= ? ORDER BY ts ASC",
                (float(since_ts),),
            ).fetchall()
        return [float(r["ts"]) for r in rows]

    def list_reorg_minutes(
        self,
        from_ts_ms: int,
        to_ts_ms: int,
    ) -> list[dict]:
        """Per-minute reorg-event counts split single (info) vs cluster.

        Pre-2026-05-03 critical-tier rows are bucketed as cluster so the
        chart reflects post-reframe semantics, not historical labels.
        Only minutes with at least one event are returned (sparse).
        """
        from_sec = from_ts_ms / 1000.0
        to_sec = to_ts_ms / 1000.0
        with self._lock:
            rows = self._conn.execute(
                """SELECT CAST(ts AS INTEGER) AS ts_int, severity
                   FROM alerts
                   WHERE rule = 'reorg' AND ts >= ? AND ts <= ?
                   ORDER BY ts ASC""",
                (from_sec, to_sec),
            ).fetchall()
        buckets: dict[int, dict[str, int]] = {}
        for r in rows:
            minute_ms = (int(r["ts_int"]) // 60) * 60 * 1000
            b = buckets.setdefault(minute_ms, {"single": 0, "cluster": 0})
            sev = r["severity"]
            if sev == "info":
                b["single"] += 1
            elif sev in ("warn", "critical"):
                b["cluster"] += 1
        return [
            {"t": ts, "single": v["single"], "cluster": v["cluster"]}
            for ts, v in sorted(buckets.items())
        ]
