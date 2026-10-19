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
import time as time
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from threading import Lock

from monad_ops.parser import ExecBlock
from monad_ops.rules.events import AlertEvent as AlertEvent
from monad_ops.rules.events import Severity
from monad_ops.storage.alerts import _AlertEnvelopeMixin
from monad_ops.storage.base_fee import _BaseFeeQueryMixin
from monad_ops.storage.blocks import _GAS_PER_SEC_SCALE as _GAS_PER_SEC_SCALE
from monad_ops.storage.blocks import _BlockQueryMixin
from monad_ops.storage.blocks import _row_to_block as _row_to_block
from monad_ops.storage.consensus import _ConsensusQueryMixin
from monad_ops.storage.enrichment import _EnrichmentQueryMixin
from monad_ops.storage.maintenance import _MaintenanceMixin
from monad_ops.storage.metadata import _MetadataMixin
from monad_ops.storage.proposers import _ProposerQueryMixin
from monad_ops.storage.reorgs import _ReorgQueryMixin

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
    -- formerly `slow_chunks`; renamed 2026-05-19 after a source dive
    -- confirmed sc = storage LRU cache size, not chunks
    storage_cache_size    INTEGER NOT NULL
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

-- Per-minute aggregate of monad-bft consensus events. The bft journal
-- emits ~5500 lines/sec under load — at the two relevant signals
-- (round_advance + local_timeout, ~2.6/sec total) and 1440 minutes/day
-- the rollup table stays under 1500 rows/day, vs 224 K raw events.
-- Aggregating at write-time per `feedback_no_per_tx_rows_for_fast_chains`.
--
-- Columns:
--   ts_minute       — Unix-ms floored to minute boundary (PK).
--   rounds_total    — total ``advancing round`` events (QC + TC).
--   rounds_tc       — subset that closed via TimeoutCertificate;
--                     timeout_pct = rounds_tc / rounds_total * 100.
--   local_timeouts  — ``local timeout`` events from this node's own
--                     pacemaker (operator-side signal, separate from
--                     the chain-wide TC count).
CREATE TABLE IF NOT EXISTS bft_minute (
    ts_minute          INTEGER PRIMARY KEY,
    rounds_total       INTEGER NOT NULL,
    rounds_tc          INTEGER NOT NULL,
    local_timeouts     INTEGER NOT NULL,
    -- Network-layer event counts within the same minute bucket.
    -- DEFAULT 0 so existing rows pre-2026-05-03 read sensibly without
    -- a backfill — they predate the network_layer_signal rule and we
    -- never had counts for them. New rows write live values.
    decrypt_fails      INTEGER NOT NULL DEFAULT 0,
    session_timeouts   INTEGER NOT NULL DEFAULT 0,
    timestamp_invalids INTEGER NOT NULL DEFAULT 0
);
-- ADD-COLUMN migrations for hosts that ran pre-2026-05-03 schema.
-- IF NOT EXISTS isn't supported on ALTER TABLE in sqlite, but the
-- migration helper below catches "duplicate column" as a no-op.

-- Per-block base-fee samples from monad-bft proposal messages.
-- One row per block_seq via INSERT OR IGNORE — proposals are
-- rebroadcast 2-3× per round, the first observation wins. Used by
-- the F6 base-fee response curve, the analogue to the validator-
-- timeouts panel: chart the chain's base_fee over time, and replay
-- queries (epochs 532/533/534) overlay it against gas utilization
-- to demonstrate Monad's dynamic-fee behaviour under load.
--
-- Storing in wei (raw on-chain unit) so reads can downscale per
-- consumer — wei → gwei (÷ 1e9) for the operator chart, wei → gas-
-- price-equivalent for builder-side analysis.
-- `author` is the proposer's secp key, nullable because it is a later
-- addition and pre-existing rows have none.
CREATE TABLE IF NOT EXISTS bft_base_fee (
    block_seq    INTEGER PRIMARY KEY,
    ts_ms        INTEGER NOT NULL,
    base_fee_wei INTEGER NOT NULL,
    author       TEXT
);

CREATE INDEX IF NOT EXISTS idx_bft_base_fee_ts ON bft_base_fee (ts_ms);

-- Lightweight key/value table for small operator-state values that
-- would be wasteful to persist into a dedicated table. Currently used
-- to carry the empirically-learned epoch_typical_length across
-- restarts so the dashboard isn't blank for the ~minute it takes the
-- backfill task to seed `_epochs` after a service restart that
-- happened to land at an epoch boundary.
CREATE TABLE IF NOT EXISTS meta (
    key        TEXT PRIMARY KEY,
    value      TEXT NOT NULL,
    updated_ts INTEGER NOT NULL
);
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


@dataclass(frozen=True, slots=True)
class BftMinute:
    """One minute-bucket of consensus-event counters from bft_minute."""
    ts_minute: int          # Unix-ms floored to minute boundary
    rounds_total: int       # advancing round count (QC + TC)
    rounds_tc: int          # subset closed by TimeoutCertificate
    local_timeouts: int     # local pacemaker fires
    # Network-layer event counts (added 2026-05-03 alongside the
    # NetworkLayerSignalRule). All three are sparse at baseline (~0/h)
    # so the per-minute resolution is fine.
    decrypt_fails: int = 0      # RaptorCast UDP-auth `failed to decrypt`
    session_timeouts: int = 0   # wireauth `session timeout expired`
    timestamp_invalids: int = 0 # consensus_state `Timestamp validation failed`


@dataclass(frozen=True, slots=True)
class BftBaseFee:
    """One per-block base-fee sample from a monad-bft proposal message."""
    block_seq: int       # = block_number (matches blocks table)
    ts_ms: int           # parsed from the JSON envelope timestamp
    base_fee_wei: int    # raw on-chain unit; divide by 1e9 for gwei
    author: str | None = None   # proposer's secp key; None on pre-migration rows


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


class Storage(
    _MaintenanceMixin, _BaseFeeQueryMixin, _ConsensusQueryMixin, _ProposerQueryMixin,
    _ReorgQueryMixin, _MetadataMixin, _AlertEnvelopeMixin, _EnrichmentQueryMixin,
    _BlockQueryMixin,
):
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
            # Forward-migration: older DBs (pre-2026-05-03) have a
            # bft_minute table without the network-layer columns. Try
            # to add them and ignore the duplicate-column error if the
            # column is already present.
            for col in (
                "decrypt_fails",
                "session_timeouts",
                "timestamp_invalids",
            ):
                try:
                    self._conn.execute(
                        f"ALTER TABLE bft_minute ADD COLUMN {col} "
                        "INTEGER NOT NULL DEFAULT 0"
                    )
                except sqlite3.OperationalError as e:
                    if "duplicate column" not in str(e).lower():
                        raise
            # Forward-migration 2026-08-14: bft_base_fee gained `author`.
            # Nullable, so existing rows stay valid and only proposals
            # seen after this ships carry a proposer.
            try:
                self._conn.execute(
                    "ALTER TABLE bft_base_fee ADD COLUMN author TEXT"
                )
            except sqlite3.OperationalError as e:
                if "duplicate column" not in str(e).lower():
                    raise
            # Forward-migration 2026-05-19: rename `slow_chunks` column.
            # The field stores the size of monad-execution's in-memory
            # storage-slot LRU cache (sourced from __exec_block `sc=`
            # field → db.print_stats() → cache_->storage_stats() →
            # LruCache::size_.load()), not anything chunk-related. Old
            # name was a parser-side misnomer that propagated. For
            # fresh installs the CREATE TABLE above uses the new name
            # and this rename is a no-op (caught by the missing-column
            # exception). For existing DBs the rename preserves data.
            try:
                self._conn.execute(
                    "ALTER TABLE blocks "
                    "RENAME COLUMN slow_chunks TO storage_cache_size"
                )
            except sqlite3.OperationalError as e:
                msg = str(e).lower()
                # Fresh install: `slow_chunks` doesn't exist (table was
                # created with the new name). Anything else = real error.
                if "no such column" not in msg:
                    raise
            self._conn.commit()

    # -- writes ------------------------------------------------------------
    def write_block(self, b: ExecBlock) -> None:
        with self._lock:
            self._conn.execute(
                """INSERT OR IGNORE INTO blocks (
                    block_number, block_id, timestamp_ms, tx_count, retried,
                    retry_pct, state_reset_us, tx_exec_us, commit_us, total_us,
                    tps_effective, tps_avg, gas_used, gas_per_sec_effective,
                    gas_per_sec_avg, active_chunks, storage_cache_size
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    b.block_number, b.block_id, b.timestamp_ms, b.tx_count, b.retried,
                    b.retry_pct, b.state_reset_us, b.tx_exec_us, b.commit_us, b.total_us,
                    b.tps_effective, b.tps_avg, b.gas_used, b.gas_per_sec_effective,
                    b.gas_per_sec_avg, b.active_chunks, b.storage_cache_size,
                ),
            )

    # -- reads -------------------------------------------------------------
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

    def load_reorg_history(self) -> tuple[int, StoredAlert | None]:
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

    def load_alerts_range(
        self,
        *,
        from_ts: float | None = None,
        to_ts: float | None = None,
        severity: str | Sequence[str] | None = None,
        limit: int = 500,
    ) -> list[StoredAlert]:
        """Filtered historical alert query for the /alerts page.

        Complements ``load_recent_alerts`` (a simple tail read) with a
        time-window + severity filter. ``severity`` may be one value or
        several (a Foundation colour code maps onto more than one).
        Returns newest-first so the UI can stream rows without a second
        reverse pass.
        """
        where = []
        params: list = []
        if from_ts is not None:
            where.append("ts >= ?")
            params.append(float(from_ts))
        if to_ts is not None:
            where.append("ts <= ?")
            params.append(float(to_ts))
        if severity:
            sevs = [severity] if isinstance(severity, str) else list(severity)
            where.append(f"severity IN ({','.join('?' * len(sevs))})")
            params.extend(sevs)
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

    # -- contract_hour rollup ---------------------------------------------

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

    # -- bft consensus minute rollup --------------------------------------

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


    # -- popup detail queries ---------------------------------------------

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


def bulk_write_blocks(storage: Storage, blocks: Iterable[ExecBlock]) -> int:
    """Convenience for replay: insert many blocks, return count."""
    n = 0
    for b in blocks:
        storage.write_block(b)
        n += 1
    return n
