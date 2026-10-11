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

    def list_reorg_alerts_for_capture(self, since_sec: float) -> list[tuple[int, int]]:
        """(block_number, block_ts_ms) for reorg alerts since ``since_sec``.

        Drives the startup backfill that recovers captures cancelled by
        an ill-timed restart. block_ts_ms preferred from the blocks table;
        falls back to alert.ts*1000 if the block was pruned."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT key, ts FROM alerts WHERE rule='reorg' AND ts >= ?",
                (float(since_sec),),
            ).fetchall()
            parsed: list[tuple[int, float]] = []
            for r in rows:
                parts = r["key"].split(":", 2)
                if len(parts) >= 2 and parts[0] == "reorg" and parts[1].isdigit():
                    parsed.append((int(parts[1]), float(r["ts"])))
            if not parsed:
                return []
            block_nums = list({bn for bn, _ in parsed})
            ph = ",".join("?" * len(block_nums))
            block_ts = dict(self._conn.execute(
                f"SELECT block_number, timestamp_ms FROM blocks WHERE block_number IN ({ph})",
                block_nums,
            ).fetchall())
        return [(bn, int(block_ts.get(bn, ts * 1000))) for bn, ts in parsed]


    def find_reorg_near_block(
        self,
        block_number: int,
        *,
        window: int = 10,
    ) -> int | None:
        """Nearest reorg alert's block_number within ±window, or None.

        Reads from the append-only alerts table. Reorgs are rare (a
        handful per week on testnet) so a full scan of ``rule='reorg'``
        rows is cheap; parsing happens client-side because the
        composite key ``reorg:<number>:<new_id>`` is canonical.
        """
        window = max(0, min(int(window), 1000))
        n = int(block_number)
        with self._lock:
            rows = self._conn.execute(
                "SELECT key FROM alerts WHERE rule = 'reorg'"
            ).fetchall()
        best: int | None = None
        best_delta: int | None = None
        for r in rows:
            key = r["key"] or ""
            parts = key.split(":", 2)
            if len(parts) < 3:
                continue
            try:
                rn = int(parts[1])
            except ValueError:
                continue
            d = abs(rn - n)
            if d <= window and (best_delta is None or d < best_delta):
                best = rn
                best_delta = d
        return best
