"""SQLite persistence for blocks and alerts.

Minimal on-disk layer so the stress-test window (2026-04-20) survives
service restarts and can be queried retrospectively for per-event
analysis. One writer, many readers — the collector loop is the sole
writer; API handlers are read-only.

Design:
  * WAL mode + synchronous=NORMAL — safe enough for operational data,
    fast enough for 2.5 blocks/sec without batching.
  * INSERT OR IGNORE on (block_number) — idempotent against replay and
    the occasional duplicate log line.
  * Alerts are append-only with an autoincrement id.
  * No retention logic for the MVP: the stress test is a one-shot event
    and we want to keep the data. Trim manually later if needed.
"""

from __future__ import annotations

import sqlite3
import time
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from threading import Lock

from monad_ops.parser import ExecBlock
from monad_ops.rules.events import AlertEvent, Severity


_SCHEMA = """
CREATE TABLE IF NOT EXISTS blocks (
    block_number          INTEGER PRIMARY KEY,
    block_id              TEXT    NOT NULL,
    timestamp_ms          INTEGER NOT NULL,
    tx_count              INTEGER NOT NULL,
    retried               INTEGER NOT NULL,
    retry_pct             REAL    NOT NULL,
    state_reset_us        INTEGER NOT NULL,
    tx_exec_us            INTEGER NOT NULL,
    commit_us             INTEGER NOT NULL,
    total_us              INTEGER NOT NULL,
    tps_effective         INTEGER NOT NULL,
    tps_avg               INTEGER NOT NULL,
    gas_used              INTEGER NOT NULL,
    gas_per_sec_effective INTEGER NOT NULL,
    gas_per_sec_avg       INTEGER NOT NULL,
    active_chunks         INTEGER NOT NULL,
    slow_chunks           INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_blocks_ts ON blocks (timestamp_ms);

-- Covering index for tx_contract_block ⋈ blocks on block_number. Lets
-- the contracts aggregate ("top_retried") satisfy its b.retry_pct read
-- from index pages only, skipping the PK lookup into the main blocks
-- table. ~15% faster on warm runs; meaningful only for large windows
-- where we visit many blocks.
CREATE INDEX IF NOT EXISTS idx_blocks_cov ON blocks (block_number, retry_pct);

CREATE TABLE IF NOT EXISTS alerts (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          REAL    NOT NULL,
    rule        TEXT    NOT NULL,
    severity    TEXT    NOT NULL,
    key         TEXT    NOT NULL,
    title       TEXT    NOT NULL,
    detail      TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_alerts_ts ON alerts (ts);

CREATE TABLE IF NOT EXISTS tx_enrichment (
    block_number     INTEGER NOT NULL,
    tx_index         INTEGER NOT NULL,
    tx_hash          TEXT    NOT NULL,
    from_addr        TEXT    NOT NULL,
    to_addr          TEXT,
    contract_created TEXT,
    status           INTEGER NOT NULL,
    gas_used         INTEGER NOT NULL,
    PRIMARY KEY (block_number, tx_index)
);

CREATE INDEX IF NOT EXISTS idx_txe_to ON tx_enrichment (to_addr);
CREATE INDEX IF NOT EXISTS idx_txe_block ON tx_enrichment (block_number);

-- Per-(block, to_addr) aggregate. Written by the enricher alongside (or
-- instead of) raw per-tx rows. Collapses N transactions hitting the
-- same contract in one block into a single row — typical 5–50x
-- compression vs tx_enrichment, and removes the O(18M) scan that
-- blocked the event loop during the 2026-04-20 stress test.
-- Dashboard queries (top_retried, window_summary) read ONLY this table;
-- tx_enrichment becomes an optional debug hot-tier.
CREATE TABLE IF NOT EXISTS tx_contract_block (
    block_number INTEGER NOT NULL,
    to_addr      TEXT    NOT NULL,
    tx_count     INTEGER NOT NULL,
    total_gas    INTEGER NOT NULL,
    PRIMARY KEY (block_number, to_addr)
);

CREATE INDEX IF NOT EXISTS idx_txcb_to ON tx_contract_block (to_addr);

-- Hourly contract rollup. Derived from (tx_contract_block ⋈ blocks),
-- bucketed by floor(block.timestamp_ms, 1h). Dashboard polling
-- (/api/contracts/top_retried) reads THIS table, not tx_contract_block
-- — at 20 M+ raw rows a single 24 h aggregate scan saturated the CPU.
--
-- Write-time filter: only rows with ``blocks_appeared >= 10`` are kept.
-- Measured on the 46 h stress-test dataset: the filter preserves 100 %
-- of the top-20 ranking (the dashboard default) and 92 % of the top-100
-- tail across 1 h / 6 h / 12 h / 24 h / 7 d windows. Contracts dropped
-- by the filter are those with fewer than 10 per-hour appearances —
-- effectively one-shot spam addresses and slow-burn contracts that
-- never rank against high-activity players. For precise post-event
-- analysis, ``/api/window_summary`` stays on the raw tx_contract_block
-- path.
--
-- Row budget under this filter:
--   Normal hour       ~20-40 rows
--   Stress-test hour  ~1 000 rows
--   Bot-flood hour    ~300-700 rows
-- → 30-day retention ≈ 300 K rows (vs. 72 M unfiltered).
--
-- Maintained by a periodic rebuild task (rebuild_contract_hour) — NOT
-- written inline by the enricher. The enricher still writes raw rows
-- to tx_contract_block; the rebuild derives contract_hour from it.
-- Decoupling the two paths means a re-enrichment of a block (rare,
-- but possible when receipts_unavailable finally resolves) naturally
-- corrects the rollup on the next rebuild pass.
CREATE TABLE IF NOT EXISTS contract_hour (
    hour_ms          INTEGER NOT NULL,   -- floor(block.timestamp_ms, 3_600_000)
    to_addr          TEXT    NOT NULL,
    blocks_appeared  INTEGER NOT NULL,
    retried_blocks   INTEGER NOT NULL,   -- subset where block.retry_pct > 0
    sum_rtp          REAL    NOT NULL,   -- Σ block.retry_pct over blocks_appeared
    tx_count         INTEGER NOT NULL,
    total_gas        INTEGER NOT NULL,
    PRIMARY KEY (hour_ms, to_addr)
);

CREATE INDEX IF NOT EXISTS idx_contract_hour_ts ON contract_hour (hour_ms);
CREATE INDEX IF NOT EXISTS idx_contract_hour_to ON contract_hour (to_addr);
"""

_HOUR_MS = 3_600_000

# Minimum per-hour blocks_appeared for a (hour, to_addr) row to be kept
# in the rollup. See contract_hour schema comment for the measurement
# that justifies this threshold.
_CONTRACT_HOUR_MIN_BA = 10


@dataclass(frozen=True, slots=True)
class StoredAlert:
    id: int
    ts: float
    rule: str
    severity: Severity
    key: str
    title: str
    detail: str


@dataclass(frozen=True, slots=True)
class TxEnrichment:
    """Per-transaction receipt data, attributed to a block."""
    block_number: int
    tx_index: int
    tx_hash: str
    from_addr: str
    to_addr: str | None          # None for contract-creation txs
    contract_created: str | None # set for contract-creation txs
    status: int                  # 0 (reverted) or 1 (success)
    gas_used: int


@dataclass(frozen=True, slots=True)
class ContractBlockAgg:
    """Aggregate row: one per (block, to_addr) destination.

    The write-time rollup of a block's receipts that backs the contract
    dashboards. Collapses all txs to the same `to_addr` inside one block
    into `tx_count` + `total_gas`. Contract-creation txs (`to_addr` is
    None in the raw receipt) are excluded at aggregation time because
    no dashboard query uses them.
    """
    block_number: int
    to_addr: str
    tx_count: int
    total_gas: int


@dataclass(frozen=True, slots=True)
class ContractRetryStat:
    """Aggregate row for the /api/contracts/top_retried endpoint."""
    to_addr: str
    blocks_appeared: int
    retried_blocks: int              # blocks where retry_pct > 0
    avg_rtp_of_blocks: float
    tx_count: int
    total_gas: int


class _PercentileAgg:
    """SQLite custom aggregate: percentile(value, p) with p in [0, 1].

    Used for ``rtp_p95`` in ``sampled_blocks`` so charts can surface "most
    blocks in this bin were this bad or worse" without the single-block
    outlier saturation of MAX(). Linear interpolation between the two
    surrounding ranks matches NumPy's default ("linear") method.

    Arity is two because SQLite aggregates receive arguments per-row; the
    percentile fraction is passed alongside each value (all values within
    a GROUP BY share the same p).
    """
    __slots__ = ("_values", "_p")

    def __init__(self) -> None:
        self._values: list[float] = []
        self._p: float = 0.95

    def step(self, value: float | None, p: float | None) -> None:
        if value is None:
            return
        if p is not None:
            # Clamp to [0, 1]; last write wins (all rows share p per group).
            self._p = max(0.0, min(1.0, float(p)))
        self._values.append(float(value))

    def finalize(self) -> float | None:
        n = len(self._values)
        if n == 0:
            return None
        if n == 1:
            return self._values[0]
        self._values.sort()
        # Linear interpolation: rank = p * (n - 1), fractional part blends
        # neighbors. For n=750, p=0.95 → rank ≈ 711.55 → blend of blocks
        # 711 and 712 in sorted order.
        rank = self._p * (n - 1)
        lo = int(rank)
        hi = min(lo + 1, n - 1)
        frac = rank - lo
        return self._values[lo] * (1.0 - frac) + self._values[hi] * frac


class Storage:
    """Thin wrapper over a SQLite connection.

    Not thread-safe for concurrent writes; guarded by a single Lock
    since the collector loop is the sole writer. API reads go through
    short-lived cursors that SQLite handles safely in WAL mode.
    """

    def __init__(self, path: Path | str) -> None:
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        # check_same_thread=False: FastAPI handlers may read from a
        # different thread than the collector. We still serialize writes
        # through the lock.
        self._conn = sqlite3.connect(
            str(self._path),
            check_same_thread=False,
            isolation_level=None,  # autocommit; we manage BEGIN explicitly if needed
        )
        self._conn.row_factory = sqlite3.Row
        # Custom aggregate: percentile(value, p). Registered once per
        # connection; used by sampled_blocks for rtp_p95.
        self._conn.create_aggregate("percentile", 2, _PercentileAgg)
        self._lock = Lock()
        self._init_schema()

    def _init_schema(self) -> None:
        with self._lock:
            self._conn.executescript("PRAGMA journal_mode=WAL;")
            self._conn.executescript("PRAGMA synchronous=NORMAL;")
            # Performance PRAGMAs (per-connection; re-applied on every
            # Storage() init so they survive process restarts).
            #   cache_size  — negative value = KB of in-memory page cache.
            #                 -65536 = ~64 MB. Rightsized after observing
            #                 RSS hit 1.3 GB with the original -200000:
            #                 64 MB is enough for hot index pages
            #                 (idx_blocks_ts, tx_contract_block PK) while
            #                 leaving the VFS to do the rest through mmap.
            #   mmap_size   — 256 MB memory-mapped window. Was 1 GB which
            #                 pulled most of state.db into RSS under load
            #                 — we don't need the whole file resident,
            #                 only the index pages + hot block tails.
            #   temp_store  — 2 = MEMORY, so ORDER BY / GROUP BY sort
            #                 buffers stay off /tmp entirely.
            self._conn.executescript("PRAGMA cache_size = -65536;")
            self._conn.executescript("PRAGMA mmap_size = 268435456;")
            self._conn.executescript("PRAGMA temp_store = MEMORY;")
            # busy_timeout — wait up to 5s on lock contention before raising
            # OperationalError("database is locked"). Without this, a brief
            # writer-vs-reader collision (collector commit overlapping with
            # an /api/window_summary scan) returns an immediate error to
            # the user. 5s is enough to ride out any single collector batch
            # without making slow queries hang noticeably longer than they
            # would have anyway.
            self._conn.executescript("PRAGMA busy_timeout = 5000;")
            self._conn.executescript(_SCHEMA)

    # -- writes ------------------------------------------------------------
    def write_block(self, b: ExecBlock) -> None:
        with self._lock:
            self._conn.execute(
                """INSERT OR IGNORE INTO blocks (
                    block_number, block_id, timestamp_ms, tx_count, retried,
                    retry_pct, state_reset_us, tx_exec_us, commit_us, total_us,
                    tps_effective, tps_avg, gas_used, gas_per_sec_effective,
                    gas_per_sec_avg, active_chunks, slow_chunks
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    b.block_number, b.block_id, b.timestamp_ms, b.tx_count, b.retried,
                    b.retry_pct, b.state_reset_us, b.tx_exec_us, b.commit_us, b.total_us,
                    b.tps_effective, b.tps_avg, b.gas_used, b.gas_per_sec_effective,
                    b.gas_per_sec_avg, b.active_chunks, b.slow_chunks,
                ),
            )

    def write_alert(self, a: AlertEvent, ts: float | None = None) -> None:
        with self._lock:
            self._conn.execute(
                """INSERT INTO alerts (ts, rule, severity, key, title, detail)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    ts if ts is not None else time.time(),
                    a.rule, a.severity.value, a.key, a.title, a.detail,
                ),
            )

    # -- reads -------------------------------------------------------------
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
            "peak_gps": int(row["peak_gps"] or 0),
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

    def load_recent_alerts(self, limit: int) -> list[StoredAlert]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT id, ts, rule, severity, key, title, detail "
                "FROM alerts ORDER BY id DESC LIMIT ?",
                (int(limit),),
            ).fetchall()
        rows.reverse()
        return [
            StoredAlert(
                id=r["id"], ts=r["ts"], rule=r["rule"],
                severity=Severity(r["severity"]), key=r["key"],
                title=r["title"], detail=r["detail"],
            )
            for r in rows
        ]

    def load_reorg_history(self) -> tuple[int, "StoredAlert | None"]:
        """Return (total reorg count, latest reorg alert) for ReorgRule bootstrap.

        Used at startup so counters survive process restarts — the live
        reorg #2 at 2026-04-19 23:30 UTC was displayed as "since start: 1"
        because the restart between reorgs wiped the in-memory counter.
        Reading from the append-only `alerts` table is the cleanest
        persistent source of truth.
        """
        with self._lock:
            count = self._conn.execute(
                "SELECT COUNT(*) AS n FROM alerts WHERE rule = 'reorg'"
            ).fetchone()["n"]
            last = self._conn.execute(
                "SELECT id, ts, rule, severity, key, title, detail "
                "FROM alerts WHERE rule = 'reorg' ORDER BY id DESC LIMIT 1"
            ).fetchone()
        if not last:
            return int(count), None
        return int(count), StoredAlert(
            id=last["id"], ts=last["ts"], rule=last["rule"],
            severity=Severity(last["severity"]), key=last["key"],
            title=last["title"], detail=last["detail"],
        )

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
                          active_chunks, slow_chunks
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
            "window": window,
            "blocks": [dict(r) for r in block_rows],
        }

    def load_alerts_range(
        self,
        *,
        from_ts: float | None = None,
        to_ts: float | None = None,
        severity: str | None = None,
        limit: int = 500,
    ) -> list[StoredAlert]:
        """Filtered historical alert query for the /alerts page.

        Complements ``load_recent_alerts`` (a simple tail read) with a
        time-window + severity filter. Returns newest-first so the UI
        can stream rows without a second reverse pass.
        """
        where = []
        params: list = []
        if from_ts is not None:
            where.append("ts >= ?"); params.append(float(from_ts))
        if to_ts is not None:
            where.append("ts <= ?"); params.append(float(to_ts))
        if severity:
            where.append("severity = ?"); params.append(severity)
        clause = (" WHERE " + " AND ".join(where)) if where else ""
        params.append(max(1, min(int(limit), 5000)))
        sql = (
            "SELECT id, ts, rule, severity, key, title, detail "
            f"FROM alerts{clause} ORDER BY id DESC LIMIT ?"
        )
        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()
        return [
            StoredAlert(
                id=r["id"], ts=r["ts"], rule=r["rule"],
                severity=Severity(r["severity"]), key=r["key"],
                title=r["title"], detail=r["detail"],
            )
            for r in rows
        ]

    def block_count(self) -> int:
        with self._lock:
            row = self._conn.execute("SELECT COUNT(*) AS n FROM blocks").fetchone()
        return int(row["n"])

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
                """
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
                    MAX(gas_per_sec_effective) AS gas_max,
                    AVG(state_reset_us)        AS state_reset_us,
                    AVG(tx_exec_us)            AS tx_exec_us,
                    AVG(commit_us)             AS commit_us,
                    COUNT(*)                   AS samples
                FROM blocks
                WHERE timestamp_ms BETWEEN ? AND ?
                GROUP BY (timestamp_ms - ?) / ?
                ORDER BY t ASC
                """,
                (int(from_ts_ms), int(to_ts_ms), int(from_ts_ms), int(bin_ms)),
            ).fetchall()
        return [dict(r) for r in rows]

    # -- tx enrichment -----------------------------------------------------
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

    def tx_enrichment_count(self) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) AS n FROM tx_enrichment"
            ).fetchone()
        return int(row["n"])

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

    def tx_contract_block_count(self) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) AS n FROM tx_contract_block"
            ).fetchone()
        return int(row["n"])

    # -- contract_hour rollup ---------------------------------------------

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

    def enrichment_has_block(self, block_number: int) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM tx_enrichment WHERE block_number = ? LIMIT 1",
                (int(block_number),),
            ).fetchone()
        return row is not None

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
        return deleted

    # -- popup detail queries ---------------------------------------------

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
            FROM tx_contract_block tcb
            JOIN blocks b ON b.block_number = tcb.block_number
            WHERE tcb.to_addr = ?
              AND b.timestamp_ms BETWEEN ? AND ?
        """
        with self._lock:
            stat_row = self._conn.execute(
                sql_stats, (addr, from_ts, to_ts)
            ).fetchone()

            peak_row = self._conn.execute(
                """SELECT tcb.block_number, tcb.tx_count, tcb.total_gas,
                          b.tx_count AS block_tx, b.retry_pct, b.timestamp_ms
                   FROM tx_contract_block tcb
                   JOIN blocks b ON b.block_number = tcb.block_number
                   WHERE tcb.to_addr = ?
                     AND b.timestamp_ms BETWEEN ? AND ?
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
                     FROM tx_contract_block tcb
                     JOIN blocks b ON b.block_number = tcb.block_number
                     WHERE tcb.to_addr = ?
                       AND b.timestamp_ms BETWEEN ? AND ?
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

    def close(self) -> None:
        with self._lock:
            self._conn.close()


# Pattern classifier used by get_contract_detail. Intentionally narrow:
# we only flag the one shape that's both unambiguous from aggregate stats
# AND useful to operators — "exactly one tx per block over many blocks",
# the airdrop-farmer / heartbeat signature that dominates testnet top-N.
# Intent is described, not inferred: we report the *pattern* ("uniform"),
# not the *motive* ("scam"), so the tag stays defensible. Contracts that
# don't match the pattern return None — better a missing tag than a
# wrong one, same policy as labels.
#
# Why 50 blocks as the floor: a contract touching fewer than 50 blocks
# in the window may coincidentally have a 1:1 ratio from too-small a
# sample. Measured on 24h top-N data, every genuine bot-pattern address
# had ≥1 000 blocks in the window, so 50 is a conservative minimum.
_PATTERN_UNIFORM_MIN_BLOCKS = 50
_PATTERN_UNIFORM_RATIO_LO = 0.98
_PATTERN_UNIFORM_RATIO_HI = 1.02


def _classify_pattern(
    blocks_appeared: int, tx_count: int
) -> dict[str, str] | None:
    if blocks_appeared < _PATTERN_UNIFORM_MIN_BLOCKS:
        return None
    ratio = tx_count / blocks_appeared
    if _PATTERN_UNIFORM_RATIO_LO <= ratio <= _PATTERN_UNIFORM_RATIO_HI:
        return {
            "label": "uniform",
            "detail": (
                f"{ratio:.2f} tx/block over {blocks_appeared:,} blocks "
                f"· likely bot pattern"
            ),
        }
    return None


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
        slow_chunks=r["slow_chunks"],
    )


def bulk_write_blocks(storage: Storage, blocks: Iterable[ExecBlock]) -> int:
    """Convenience for replay: insert many blocks, return count."""
    n = 0
    for b in blocks:
        storage.write_block(b)
        n += 1
    return n
