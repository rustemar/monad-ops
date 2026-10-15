"""Reorg counters and timeline queries over shared storage resources."""

from __future__ import annotations

import sqlite3
from _thread import LockType
from collections.abc import Iterable
from typing import TYPE_CHECKING


class _ReorgQueryMixin:
    """Reorg queries using the connection and lock owned by ``Storage``."""

    _conn: sqlite3.Connection
    _lock: LockType

    if TYPE_CHECKING:
        def get_proposers(self, block_seqs: Iterable[int]) -> dict[int, str]: ...

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

    def list_reorgs(self, limit: int = 200) -> list[dict]:
        """List reorg events enriched with the reorged block's metrics.

        Each row reconstructs both block_ids: the "before" id from the
        ``blocks`` table (first-seen via INSERT OR IGNORE, so it holds
        the original observation) and the "after" id from the alert's
        ``key`` column (format ``reorg:<number>:<full_new_id>``). The
        alert ``detail`` field only carries truncated hex, so the key
        parse is the canonical source for the full new id.

        Block metrics (tx_count, retry_pct, gas_used, timing) come from
        whichever version of the block was persisted — for reorgs that
        is the "before" version. Callers should treat per-block metrics
        here as the losing-fork view, not the canonical chain view.

        Returns newest-first so dashboards can render without a reverse
        pass. Rows lacking a corresponding ``blocks`` entry (bootstrap
        edge cases, blocks evicted by retention) still appear with
        ``block: None`` so the caller can distinguish "no metrics" from
        "no reorg".
        """
        with self._lock:
            rows = self._conn.execute(
                """SELECT a.id AS alert_id, a.ts AS alert_ts, a.key, a.detail,
                          b.block_number, b.block_id AS block_id_before,
                          b.timestamp_ms, b.tx_count, b.retried, b.retry_pct,
                          b.gas_used
                   FROM alerts a
                   LEFT JOIN blocks b ON b.block_number = CAST(
                       substr(a.key,
                              length('reorg:') + 1,
                              instr(substr(a.key, length('reorg:') + 1), ':') - 1
                       ) AS INTEGER
                   )
                   WHERE a.rule = 'reorg'
                   ORDER BY a.ts DESC
                   LIMIT ?""",
                (max(1, min(int(limit), 5000)),),
            ).fetchall()

        out: list[dict] = []
        for r in rows:
            # Parse "reorg:<number>:<full_new_id>" — the key is always
            # well-formed because the rule constructs it directly.
            parts = (r["key"] or "").split(":", 2)
            block_number = int(parts[1]) if len(parts) >= 3 else None
            block_id_after = parts[2] if len(parts) >= 3 else None

            block_view: dict | None = None
            if r["block_number"] is not None:
                block_view = {
                    "block_number": r["block_number"],
                    "block_id_before": r["block_id_before"],
                    "timestamp_ms": r["timestamp_ms"],
                    "tx_count": r["tx_count"],
                    "retried": r["retried"],
                    "retry_pct": r["retry_pct"],
                    "gas_used": r["gas_used"],
                }

            out.append({
                "alert_id": r["alert_id"],
                "alert_ts_ms": int(r["alert_ts"] * 1000),
                "block_number": block_number,
                "block_id_before": block_view["block_id_before"] if block_view else None,
                "block_id_after": block_id_after,
                "block": block_view,  # full metrics for the losing-fork view, or None
                "detail": r["detail"],
            })
        return out

    def get_reorg_trace(
        self,
        block_number: int,
        *,
        window: int = 30,
    ) -> dict | None:
        """Return a reorg event + N blocks on each side from ``blocks``.

        ``window`` is clamped to [0, 500]. The returned neighbors span
        ``[block_number - window, block_number + window]`` as available
        in the blocks table (gaps at startup or post-retention are
        simply omitted — the caller sees the contiguous range that
        exists). Neighbors are returned in ascending block_number order
        so renderers can chart timing without a sort.

        Returns ``None`` if no reorg alert exists for this block_number.
        """
        window = max(0, min(int(window), 500))
        key_prefix = f"reorg:{int(block_number)}:"
        with self._lock:
            alert_row = self._conn.execute(
                """SELECT id, ts, key, title, detail
                   FROM alerts
                   WHERE rule = 'reorg' AND key LIKE ?
                   ORDER BY ts DESC LIMIT 1""",
                (key_prefix + "%",),
            ).fetchone()
            if not alert_row:
                return None

            lo = int(block_number) - window
            hi = int(block_number) + window
            block_rows = self._conn.execute(
                """SELECT block_number, block_id, timestamp_ms, tx_count,
                          retried, retry_pct, state_reset_us, tx_exec_us,
                          commit_us, total_us, tps_effective, tps_avg,
                          gas_used, gas_per_sec_effective, gas_per_sec_avg,
                          active_chunks, storage_cache_size
                   FROM blocks
                   WHERE block_number BETWEEN ? AND ?
                   ORDER BY block_number ASC""",
                (lo, hi),
            ).fetchall()

        # Reconstruct the new block_id from the alert key.
        parts = (alert_row["key"] or "").split(":", 2)
        block_id_after = parts[2] if len(parts) >= 3 else None

        # Pull the "before" id out of the block row we already have, if any.
        reorged_block = next(
            (r for r in block_rows if r["block_number"] == int(block_number)),
            None,
        )
        block_id_before = reorged_block["block_id"] if reorged_block else None

        # Leader attribution across the whole window, not just the
        # reorged block: the forensic question is whether one validator
        # proposed both sides of the split, or whether the neighbours
        # changed hands around it. One batched lookup for the window.
        blocks = [dict(r) for r in block_rows]
        proposers = self.get_proposers(
            [b["block_number"] for b in blocks] + [int(block_number)]
        )
        for b in blocks:
            b["proposer"] = proposers.get(b["block_number"])

        return {
            "alert": {
                "id": alert_row["id"],
                "ts_ms": int(alert_row["ts"] * 1000),
                "title": alert_row["title"],
                "detail": alert_row["detail"],
            },
            "block_number": int(block_number),
            "block_id_before": block_id_before,
            "block_id_after": block_id_after,
            "proposer": proposers.get(int(block_number)),
            "window": window,
            "blocks": blocks,
        }
