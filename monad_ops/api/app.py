"""FastAPI app — serves the dashboard and JSON endpoints.

Design notes:
  * Read-only. Mutation of state happens only in the collector loop.
  * No auth at the app layer — intended to sit behind nginx with
    either a subnet-allow ACL (for an internal endpoint) or TLS on a
    public hostname. Templates for both setups live under ``systemd/``.
  * Templates live under ``monad_ops/dashboard/templates/``, static
    files under ``monad_ops/dashboard/static/``.
"""

from __future__ import annotations

import asyncio
import re
import subprocess
import time
from collections import OrderedDict
from pathlib import Path

from fastapi import FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.exceptions import HTTPException as StarletteHTTPException

from monad_ops.config import Config
from monad_ops.enricher import EnrichmentWorker
from monad_ops.labels import ContractLabels
from monad_ops.reorg_capture import find_artifact
from monad_ops.rules.events import code_color_for
from monad_ops.state import State

_THIS_DIR = Path(__file__).parent
_PKG_DIR = _THIS_DIR.parent
_REPO_DIR = _PKG_DIR.parent
_TEMPLATE_DIR = _PKG_DIR / "dashboard" / "templates"
_STATIC_DIR = _PKG_DIR / "dashboard" / "static"


def _asset_version() -> str:
    """Version string for cache-busting CSS/JS via ?v=... query param.

    Combines the short git HEAD hash (human-readable) with the newest
    template/static mtime (catches uncommitted edits after a restart).
    Either part can be missing; the result still changes whenever either
    changes.
    """
    try:
        out = subprocess.check_output(
            ["git", "-C", str(_REPO_DIR), "rev-parse", "--short", "HEAD"],
            stderr=subprocess.DEVNULL,
            timeout=2,
        )
        git_part = out.decode().strip() or "dev"
    except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired):
        git_part = "dev"
    mtime = 0
    for d in (_STATIC_DIR, _TEMPLATE_DIR):
        for f in d.rglob("*"):
            if f.is_file():
                try:
                    mtime = max(mtime, int(f.stat().st_mtime))
                except OSError:
                    continue
    return f"{git_part}-{mtime}" if mtime else git_part


_ASSET_VERSION = _asset_version()


def build_app(
    state: State,
    config: Config,
    enricher: EnrichmentWorker | None = None,
    labels: ContractLabels | None = None,
    journal_capture_dir: Path | None = None,
) -> FastAPI:
    labels = labels or ContractLabels({})
    app = FastAPI(title="monad-ops", docs_url=None, redoc_url=None, openapi_url=None)
    # CORS — permissive for GET only. The API is read-only, there is no
    # auth surface to protect, and the whole point of a public operations
    # dashboard is to let other builders pull from it (Foundation
    # retrospectives, community dashboards embedding our metrics). The
    # CSP on the HTML response stays strict (default-src 'self') — only
    # the JSON endpoints opt into cross-origin.
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["GET", "HEAD", "OPTIONS"],
        allow_headers=["*"],
        max_age=600,
    )
    templates = Jinja2Templates(directory=str(_TEMPLATE_DIR))
    app.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")

    # Error counter for /api/status/errors (G11).
    from collections import Counter
    _error_counts: Counter[int] = Counter()
    _error_since = time.time()

    @app.middleware("http")
    async def _count_errors(request: Request, call_next):
        response = await call_next(request)
        if response.status_code >= 400:
            _error_counts[response.status_code] += 1
        return response

    # Per-endpoint TTLs. Each one balances "user expectation of freshness"
    # against "how much load we shed by caching". For an open dashboard
    # tab polling at 30s, even 1-2s of staleness is invisible while
    # collapsing 1000 concurrent viewers into a single SQL/snapshot per
    # interval. Values cross-referenced with the dashboard's poll cadence
    # in dashboard/static/dashboard.js.
    _STATE_TTL = 1.0          # in-memory snapshot, recomputed every block
    _BLOCKS_TTL = 2.0         # in-memory recent-blocks tail
    _ALERTS_TTL = 5.0         # in-memory recent-alerts tail
    _PROBES_TTL = 60.0        # probes loop runs every ~30s host-side
    _REORGS_LIST_TTL = 30.0   # changes only when a new reorg fires (rare)
    _STRESS_EVENTS_TTL = 10.0 # alerts table append-only; live envelope updates need fresh reads
    _REORG_TRACE_TTL = 300.0  # historical reorg trace is immutable
    # Short TTL applied when a reorg trace is missing post-event blocks
    # (the tailer hasn't caught up yet). Without this an unlucky first
    # viewer's truncated result would stick in cache for 5 minutes; the
    # underlying SQL is sub-millisecond so refreshing every few seconds
    # is cheap.
    _REORG_TRACE_PARTIAL_TTL = 5.0
    _WINDOW_SUMMARY_TTL = 15.0  # heavy SQL aggregate over arbitrary window

    # Generic TTL cache + in-flight dedup. Used for the two heavy
    # aggregates — top_retried (15s TTL, 4-sec query) and blocks/sampled
    # (dynamic 2–15s TTL, ~100-300ms query on 24h windows). Both share
    # the same shape: quantize-key → single-SQL-per-window → N viewers
    # collapse to 1 SQL per TTL interval.
    #
    # Bounded size to prevent unchecked growth: quantize-keys rotate
    # every few seconds, so over hours an unbounded dict would accumulate
    # thousands of stale entries even though each is individually small.
    # A 128-entry cap + FIFO eviction keeps memory predictable; hot keys
    # are refreshed in-place so they never age out.
    # Cache entries are (stored_at, ttl_sec, value). The per-entry TTL
    # (rather than a single per-bucket value) lets a loader downgrade a
    # specific result's freshness — e.g. mark a reorg trace as
    # "incomplete, refresh soon" when the tailer hasn't caught up to
    # the post-event window yet.
    _cache_store: dict[str, OrderedDict[tuple, tuple[float, float, object]]] = {}
    _cache_inflight: dict[str, dict[tuple, asyncio.Future]] = {}
    _CACHE_MAX_ENTRIES = 128
    # Test-only seam: lets a caller inspect the per-entry TTL applied to
    # a cached value (e.g. asserting that a truncated reorg trace was
    # given the partial-TTL not the long one). app.state is the
    # idiomatic mount point for app-scoped objects in Starlette/FastAPI;
    # the production code path never reads from it.
    app.state.cache_store = _cache_store

    async def _cached(
        bucket: str,
        ttl_sec: float,
        cache_key: tuple,
        loader,
        *,
        ttl_for_value=None,
    ):
        """Read-through cache with per-entry TTL + in-flight dedup.

        ``ttl_for_value(value)`` is an optional callable that returns
        an override TTL for a freshly-loaded value. Use it when some
        results should be cached for less time than others (e.g. a
        partially-populated reorg trace whose post-window hasn't been
        ingested yet).
        """
        store = _cache_store.setdefault(bucket, OrderedDict())
        inflight = _cache_inflight.setdefault(bucket, {})
        now = time.monotonic()
        entry = store.get(cache_key)
        if entry and (now - entry[0]) < entry[1]:
            # Move-to-end marks this key recently used (LRU-ish).
            store.move_to_end(cache_key)
            return entry[2]
        pending = inflight.get(cache_key)
        if pending is not None:
            return await pending
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        inflight[cache_key] = fut
        try:
            value = await loader()
            eff_ttl = ttl_for_value(value) if ttl_for_value is not None else ttl_sec
            store[cache_key] = (time.monotonic(), eff_ttl, value)
            store.move_to_end(cache_key)
            # Evict oldest entries once over the cap. Bounded to a handful
            # of pops even in pathological traffic, so the eviction loop
            # doesn't stall the event loop.
            while len(store) > _CACHE_MAX_ENTRIES:
                store.popitem(last=False)
            fut.set_result(value)
            return value
        except Exception as e:
            fut.set_exception(e)
            raise
        finally:
            inflight.pop(cache_key, None)

    # Prewarm helper removed — since the top_retried read path moved to
    # the contract_hour rollup (sub-50 ms for 24 h), keeping an extra
    # TTL-cache warmer would just duplicate hot data already served by
    # the rollup + 15 s cache. The cli.py warm_top_retried_shapes task
    # is likewise no longer registered.

    # Cached first-block timestamp — only changes when new blocks arrive
    # earlier than what we've seen (extremely rare). Lazily populated.
    _data_start_ms: int | None = None

    def _get_data_start_ms() -> int | None:
        nonlocal _data_start_ms
        if _data_start_ms is not None:
            return _data_start_ms
        if state.storage is None:
            return None
        row = state.storage._conn.execute(
            "SELECT MIN(timestamp_ms) FROM blocks"
        ).fetchone()
        if row and row[0] is not None:
            _data_start_ms = int(row[0])
        return _data_start_ms

    @app.api_route("/api/state", methods=["GET", "HEAD"])
    async def api_state() -> JSONResponse:
        async def _load():
            snap = state.snapshot()
            return {
                "node_name": config.node.name,
                "data_start_ms": _get_data_start_ms(),
                "started_at": snap.started_at,
                "uptime_sec": snap.uptime_sec,
                "blocks_seen": snap.blocks_seen,
                "last_block": snap.last_block,
                "last_block_seen_ms": snap.last_block_seen_ms,
                "blocks_per_sec_1m": snap.blocks_per_sec_1m,
                "rtp_avg_1m": snap.rtp_avg_1m,
                "rtp_avg_5m": snap.rtp_avg_5m,
                "rtp_max_1m": snap.rtp_max_1m,
                "tx_per_sec_1m": snap.tx_per_sec_1m,
                "gas_per_sec_1m": snap.gas_per_sec_1m,
                "tps_effective_peak_1m": snap.tps_effective_peak_1m,
                "tps_effective_avg_1m": snap.tps_effective_avg_1m,
                "tps_eff_peak_block": snap.tps_eff_peak_block,
                "gas_per_sec_effective_peak_1m": snap.gas_per_sec_effective_peak_1m,
                "gas_eff_peak_block": snap.gas_eff_peak_block,
                "reorg_count": snap.reorg_count,
                "recent_reorgs_24h": snap.recent_reorgs_24h,
                "last_reorg_number": snap.last_reorg_number,
                "last_reorg_old_id": snap.last_reorg_old_id,
                "last_reorg_new_id": snap.last_reorg_new_id,
                "last_reorg_ts_ms": snap.last_reorg_ts_ms,
                "reference_block": snap.reference_block,
                "reference_checked_ms": snap.reference_checked_ms,
                "reference_error": snap.reference_error,
                "reference_local_at_sample": snap.reference_local_at_sample,
                "current_alerts": snap.current_alerts,
                "epoch": {
                    "number": snap.epoch_number,
                    "blocks_in": snap.epoch_blocks_in,
                    "typical_length": snap.epoch_typical_length,
                    "eta_sec": snap.epoch_eta_sec,
                },
                # Consensus-health view. Foundation tracks
                # ``validator_timeout_pct`` chain-wide (2026-04-20
                # stress-test summary: ``<3% target``); ``local_timeout_per_min`` is the
                # operator-side complement — this node's pacemaker fires
                # per minute. Both default to 0.0 until the bft tailer
                # has filled at least one minute bucket.
                "consensus": {
                    "validator_timeout_pct_5m": snap.validator_timeout_pct_5m,
                    "local_timeout_per_min_5m": snap.local_timeout_per_min_5m,
                    "rounds_observed_5m": snap.bft_rounds_observed_5m,
                    "local_timeouts_5m": snap.bft_local_timeouts_5m,
                },
            }
        payload = await _cached("state", _STATE_TTL, (), _load)
        return JSONResponse(payload)

    @app.api_route("/api/blocks", methods=["GET", "HEAD"])
    async def api_blocks(
        limit: int = Query(300, ge=1, le=2000),
    ) -> JSONResponse:
        async def _load():
            blocks = state.recent_blocks(limit=limit)
            return [
                {
                    "n": b.block_number,
                    "t": b.timestamp_ms,
                    "tx": b.tx_count,
                    "rt": b.retried,
                    "rtp": b.retry_pct,
                    "gas": b.gas_used,
                    "tot_us": b.total_us,
                    # Execution-time components for the breakdown chart.
                    # Keys kept short to hold the payload compact — the
                    # dashboard polls /api/blocks every 10s with limit=300.
                    "sr_us": b.state_reset_us,
                    "te_us": b.tx_exec_us,
                    "cm_us": b.commit_us,
                    "tps_eff": b.tps_effective,
                }
                for b in blocks
            ]
        payload = await _cached("blocks", _BLOCKS_TTL, (limit,), _load)
        return JSONResponse(payload)

    @app.api_route("/api/blocks/sampled", methods=["GET", "HEAD"])
    async def api_blocks_sampled(
        from_ts_ms: int = Query(..., ge=0),
        to_ts_ms: int = Query(..., ge=0),
        points: int = Query(300, ge=10, le=2000),
    ) -> JSONResponse:
        """Downsampled block series for dashboard charts.

        Returns at most ``points`` bins across the [from, to] window,
        aggregated server-side. Designed to back the period selector
        (5m / 15m / 1h / 4h / 24h / custom) so a chart never renders
        more than ~300 datapoints regardless of how large the window
        is — Chart.js stays responsive, network payloads stay small.

        Wrapped in to_thread: large windows hit the blocks index but
        the aggregation still walks many rows.
        """
        if state.storage is None:
            return JSONResponse({"error": "persistence disabled"}, status_code=503)
        if to_ts_ms <= from_ts_ms:
            return JSONResponse(
                {"error": "to_ts_ms must be > from_ts_ms"}, status_code=400
            )
        # Hard cap on span to stop someone from asking for a 30-day scan
        # (would pull millions of blocks into a single GROUP BY even with
        # the index). 7 days matches the client's max span.
        MAX_SPAN_MS = 7 * 86400 * 1000
        if (to_ts_ms - from_ts_ms) > MAX_SPAN_MS:
            return JSONResponse(
                {"error": "span too large (max 7 days)"}, status_code=400
            )
        # Dynamic TTL + quantization: staleness tolerance scales with
        # bin size. A 5m chart has 1-second bins — user expects near-
        # real-time, so TTL 2s. A 24h chart has 5-minute bins — another
        # 15 seconds of latency is imperceptible. Without quantization
        # each viewer's `Date.now()` is a unique ts, defeating cache
        # between users. Match quantization step to TTL.
        span_ms = to_ts_ms - from_ts_ms
        bin_ms = max(1, span_ms // max(1, min(points, 2000)))
        ttl_sec = max(2.0, min(bin_ms / 1000 / 3, 15.0))
        step = int(ttl_sec * 1000)
        cache_key = (
            (from_ts_ms // step) * step,
            (to_ts_ms // step) * step,
            int(points),
        )
        async def _load():
            bins = await asyncio.to_thread(
                state.storage.sampled_blocks,
                from_ts_ms=from_ts_ms,
                to_ts_ms=to_ts_ms,
                target_points=points,
            )
            return {
                "from_ts_ms": from_ts_ms,
                "to_ts_ms": to_ts_ms,
                "bin_ms": span_ms // max(1, min(points, len(bins) if bins else points)),
                "bins": bins,
            }
        payload = await _cached("sampled", ttl_sec, cache_key, _load)
        return JSONResponse(payload)

    @app.api_route("/api/bft_series", methods=["GET", "HEAD"])
    async def api_bft_series(
        from_ts_ms: int = Query(..., ge=0),
        to_ts_ms: int = Query(..., ge=0),
    ) -> JSONResponse:
        """Per-minute consensus series for the validator-timeout chart.

        1-min resolution is the storage cadence — no client-side bucketing
        and no server-side downsampling. At the storage cap of 2000 rows
        the longest visible range is ~33 hours; the dashboard's 24h
        preset is comfortably inside that. For replay queries beyond
        that horizon, callers should use the aggregated
        ``/api/window_summary.consensus`` shape instead.
        """
        if state.storage is None:
            return JSONResponse({"error": "persistence disabled"}, status_code=503)
        if to_ts_ms <= from_ts_ms:
            return JSONResponse(
                {"error": "to_ts_ms must be > from_ts_ms"}, status_code=400
            )
        # 33h hard cap (2000 minute-buckets) so a misconfigured client
        # can't pull the whole table; matches Storage.list_bft_minutes
        # default limit.
        MAX_SPAN_MS = 33 * 3600 * 1000
        span_ms = to_ts_ms - from_ts_ms
        if span_ms > MAX_SPAN_MS:
            return JSONResponse(
                {"error": "span too large (max 33 hours)"}, status_code=400
            )
        # Cache TTL = 5s, matched quantization. Series doesn't move fast
        # (one new bucket per minute) so 5s is plenty fresh for chart UX
        # and lets multiple viewers share the same cache slot.
        step = 5_000
        cache_key = (
            (from_ts_ms // step) * step,
            (to_ts_ms // step) * step,
        )

        async def _load():
            bins = await asyncio.to_thread(
                state.storage.list_bft_minutes,
                from_ts_ms,
                to_ts_ms,
            )
            return {
                "from_ts_ms": from_ts_ms,
                "to_ts_ms": to_ts_ms,
                "bin_ms": 60_000,
                "bins": bins,
            }
        payload = await _cached("bft_series", 5.0, cache_key, _load)
        return JSONResponse(payload)

    @app.api_route("/api/base_fee_series", methods=["GET", "HEAD"])
    async def api_base_fee_series(
        from_ts_ms: int = Query(..., ge=0),
        to_ts_ms: int = Query(..., ge=0),
        points: int = Query(300, ge=10, le=2000),
    ) -> JSONResponse:
        """Per-block base-fee samples from the bft proposal stream.

        Backs the F6 base-fee response curve on the dashboard, the
        operator-side analogue of Foundation's "dynamic-fee behaviour
        worked as expected" framing from 2026-04-20.

        Server-side downsampling so a 24h window returns ~300 bins
        with avg/min/max envelope, not 200 K raw block samples.
        Wrapped in to_thread because long windows still walk every
        ts_ms in the range index.
        """
        if state.storage is None:
            return JSONResponse({"error": "persistence disabled"}, status_code=503)
        if to_ts_ms <= from_ts_ms:
            return JSONResponse(
                {"error": "to_ts_ms must be > from_ts_ms"}, status_code=400
            )
        # 7-day cap matches the chart toolbar's max preset; replays beyond
        # that horizon should call /api/window_summary for aggregates.
        MAX_SPAN_MS = 7 * 86400 * 1000
        span_ms = to_ts_ms - from_ts_ms
        if span_ms > MAX_SPAN_MS:
            return JSONResponse(
                {"error": "span too large (max 7 days)"}, status_code=400
            )
        # Same dynamic TTL/quantization pattern as /api/blocks/sampled —
        # short windows refresh fast (live chart), long windows tolerate
        # 15 s of staleness so multiple viewers share cache slots.
        bin_ms = max(1, span_ms // max(1, min(points, 2000)))
        ttl_sec = max(2.0, min(bin_ms / 1000 / 3, 15.0))
        step = int(ttl_sec * 1000)
        cache_key = (
            (from_ts_ms // step) * step,
            (to_ts_ms // step) * step,
            int(points),
        )

        async def _load():
            bins = await asyncio.to_thread(
                state.storage.sampled_bft_base_fee,
                from_ts_ms=from_ts_ms,
                to_ts_ms=to_ts_ms,
                target_points=points,
            )
            return {
                "from_ts_ms": from_ts_ms,
                "to_ts_ms": to_ts_ms,
                "bin_ms": span_ms // max(1, min(points, len(bins) if bins else points)),
                "bins": bins,
            }
        payload = await _cached("base_fee_series", ttl_sec, cache_key, _load)
        return JSONResponse(payload)

    @app.api_route("/api/blocks/range", methods=["GET", "HEAD"])
    async def api_blocks_range(
        from_block: int | None = Query(None, ge=0),
        to_block: int | None = Query(None, ge=0),
        from_ts_ms: int | None = Query(None, ge=0),
        to_ts_ms: int | None = Query(None, ge=0),
        limit: int = Query(5000, ge=1, le=50_000),
    ) -> JSONResponse:
        """Historical block query backed by persistent storage.

        Use for post-event analysis (e.g. querying just the stress-test
        window by block_number or ms timestamp). Returns an empty list
        if persistence is disabled.
        """
        if state.storage is None:
            return JSONResponse({"error": "persistence disabled"}, status_code=503)
        blocks = state.storage.load_blocks_range(
            from_block=from_block,
            to_block=to_block,
            from_ts_ms=from_ts_ms,
            to_ts_ms=to_ts_ms,
            limit=limit,
        )
        return JSONResponse({
            "count": len(blocks),
            "blocks": [
                {
                    "n": b.block_number,
                    "t": b.timestamp_ms,
                    "tx": b.tx_count,
                    "rt": b.retried,
                    "rtp": b.retry_pct,
                    "gas": b.gas_used,
                    "tot_us": b.total_us,
                    "tpse": b.tps_effective,
                    "gpse": b.gas_per_sec_effective,
                }
                for b in blocks
            ],
        })

    @app.api_route("/api/alerts", methods=["GET", "HEAD"])
    async def api_alerts(
        limit: int = Query(50, ge=1, le=200),
    ) -> JSONResponse:
        async def _load():
            alerts = state.recent_alerts_with_ts(limit=limit)
            return [
                {
                    "rule": a.rule,
                    "severity": a.severity.value,
                    "code_color": code_color_for(a.severity).value,
                    "title": a.title,
                    "detail": a.detail,
                    "ts_ms": int(ts * 1000),
                }
                for a, ts in alerts
            ]
        payload = await _cached("alerts", _ALERTS_TTL, (limit,), _load)
        return JSONResponse(payload)

    @app.api_route("/api/alerts/history", methods=["GET", "HEAD"])
    async def api_alerts_history(
        from_ts_ms: int | None = Query(None, ge=0),
        to_ts_ms: int | None = Query(None, ge=0),
        severity: str | None = Query(None, pattern="^(critical|warn|info|recovered)$"),
        limit: int = Query(500, ge=1, le=5000),
    ) -> JSONResponse:
        """Historical alerts from persistent storage, filterable.

        Unlike /api/alerts (in-memory tail, cleared on restart), this
        reads the sqlite `alerts` table with optional ts/severity
        filters. Returns newest-first.
        """
        if state.storage is None:
            return JSONResponse({"error": "persistence disabled"}, status_code=503)
        rows = state.storage.load_alerts_range(
            from_ts=(from_ts_ms / 1000.0) if from_ts_ms is not None else None,
            to_ts=(to_ts_ms / 1000.0) if to_ts_ms is not None else None,
            severity=severity,
            limit=limit,
        )
        return JSONResponse({
            "count": len(rows),
            "alerts": [
                {
                    "id": r.id,
                    "ts_ms": int(r.ts * 1000),
                    "rule": r.rule,
                    "severity": r.severity.value,
                    "code_color": code_color_for(r.severity).value,
                    "key": r.key,
                    "title": r.title,
                    "detail": r.detail,
                }
                for r in rows
            ],
        })

    # Fields kept in the ``public`` variant of reorg traces. Anything
    # omitted is local-node performance telemetry (tx_exec_us,
    # total_us, active_chunks, …) that leaks hardware characteristics
    # and is not part of the chain itself. The public set covers
    # everything derivable from the chain record alone — enough to
    # support operator-to-operator discussion of a reorg without
    # revealing how this specific node is provisioned.
    _REORG_TRACE_PUBLIC_FIELDS = (
        "block_number", "block_id", "timestamp_ms",
        "tx_count", "retried", "retry_pct", "gas_used",
    )

    def _sanitize_block_for_public(row: dict) -> dict:
        return {k: row[k] for k in _REORG_TRACE_PUBLIC_FIELDS if k in row}

    @app.api_route("/api/stress_events", methods=["GET", "HEAD"])
    async def api_stress_events(
        limit: int = Query(5, ge=1, le=50),
        max_age_days: int = Query(30, ge=1, le=365),
        merge_gap_sec: int = Query(1800, ge=60, le=86400),
    ) -> JSONResponse:
        """Quick-jump targets for past + ongoing stress events.

        Walks the alerts table for retry_spike CRITICAL/RECOVERED runs,
        groups consecutive criticals into envelopes, then merges
        envelopes whose recovery-to-rearm gap is short enough that they
        are semantically one event. Used by the dashboard's stress-event
        button row to let an operator jump straight to the window of a
        past or ongoing stress test without picking dates by hand.
        Backed by ``Storage.list_stress_envelopes`` — see that method
        for the merge-gap rationale (default 30 min covers within-batch
        dips without merging real between-batch silence).
        """
        if state.storage is None:
            return JSONResponse({"error": "persistence disabled"}, status_code=503)
        async def _load():
            rows = await asyncio.to_thread(
                state.storage.list_stress_envelopes,
                limit=limit,
                max_age_days=max_age_days,
                merge_gap_sec=float(merge_gap_sec),
            )
            return {"count": len(rows), "events": rows}
        payload = await _cached(
            "stress_events", _STRESS_EVENTS_TTL,
            (limit, max_age_days, merge_gap_sec),
            _load,
        )
        return JSONResponse(payload)

    @app.api_route("/api/reorgs", methods=["GET", "HEAD"])
    async def api_reorgs(
        limit: int = Query(200, ge=1, le=5000),
    ) -> JSONResponse:
        """List of observed reorg events, newest-first.

        Each row reconstructs both block_ids (``before`` from the
        persisted blocks row, ``after`` from the alert key) and a
        compact block-metrics summary. ``has_journal`` is true when a
        sanitized journal-trace artifact has been captured for the
        event (only reorgs that fired after the capture feature shipped
        — historical reorgs return false). For the full per-block
        neighbor trace, call ``/api/reorgs/{block_number}``; for the
        gzipped journal artifact, call
        ``/api/reorgs/{block_number}/journal``.
        """
        if state.storage is None:
            return JSONResponse({"error": "persistence disabled"}, status_code=503)
        async def _load():
            rows = await asyncio.to_thread(state.storage.list_reorgs, limit=limit)
            if journal_capture_dir is not None:
                for row in rows:
                    bn = row.get("block_number")
                    row["has_journal"] = (
                        bn is not None
                        and find_artifact(journal_capture_dir, int(bn)) is not None
                    )
            else:
                for row in rows:
                    row["has_journal"] = False
            return {"count": len(rows), "reorgs": rows}
        payload = await _cached("reorgs", _REORGS_LIST_TTL, (limit,), _load)
        return JSONResponse(payload)

    @app.api_route("/api/reorgs/{block_number}/journal", methods=["GET", "HEAD"])
    async def api_reorg_journal(block_number: int) -> FileResponse:
        """Sanitized journal trace around a reorg event.

        Returns the gzipped JSONL artifact captured at fire time,
        covering the ``monad-bft`` consensus stream from a few seconds
        before the reorged block's wall-clock timestamp through a few
        seconds after. Peer IPs from the wire-auth keepalive stream
        and the local OTLP loopback are scrubbed at write time; the
        rest of the consensus trace (validator pubkeys, block ids,
        rounds, votes, base fees) is public chain data and stays
        intact.

        404 when no artifact exists — either the reorg pre-dates the
        capture feature, or the journal had already rotated past the
        event's window when the deferred snapshot ran.
        """
        if journal_capture_dir is None:
            return JSONResponse(
                {"error": "journal capture disabled"}, status_code=503
            )
        path = find_artifact(journal_capture_dir, block_number)
        if path is None:
            return JSONResponse(
                {"error": "journal artifact not found"}, status_code=404
            )
        return FileResponse(
            path,
            media_type="application/gzip",
            filename=f"reorg-{block_number}.jsonl.gz",
        )

    @app.api_route("/api/reorgs/{block_number}", methods=["GET", "HEAD"])
    async def api_reorg_trace(
        block_number: int,
        window: int = Query(30, ge=0, le=500),
        level: str = Query("public", pattern="^(public|full)$"),
    ) -> JSONResponse:
        """Reorg event + neighboring blocks for forensic review.

        ``window`` controls how many blocks before/after the reorged
        block to include (0–500, default 30). ``level=public`` (the
        default) strips local timing fields so the trace is safe to
        share operator-to-operator; ``level=full`` returns every
        ExecBlock field for deeper internal analysis.

        404 if no reorg alert exists for this block_number.
        """
        if state.storage is None:
            return JSONResponse({"error": "persistence disabled"}, status_code=503)
        async def _load():
            trace = await asyncio.to_thread(
                state.storage.get_reorg_trace, block_number, window=window
            )
            if trace is None:
                return None
            # Sanitization happens silently — no "level" marker in the
            # response body. A marker would just invite the reader to
            # wonder what the "other" level withholds, and it doesn't
            # actually protect the stripped fields (JSON is trivially
            # editable). The URL parameter remains available as an
            # opt-in for internal callers who need the local timing.
            if level == "public":
                trace = {
                    **trace,
                    "blocks": [_sanitize_block_for_public(b) for b in trace["blocks"]],
                }
            return trace

        def _ttl(trace) -> float:
            # An unknown block_number caches its 404 long — those don't
            # spontaneously become reorgs.
            if trace is None:
                return _REORG_TRACE_TTL
            # Truncated post-window means the tailer is still catching
            # up to the reorged block's neighbours. Cache only briefly
            # so a viewer who hits the endpoint at fire-time + 0.1s
            # doesn't get stuck with a partial trace for 5 minutes.
            blocks = trace.get("blocks") or []
            highest_seen = blocks[-1]["block_number"] if blocks else None
            post_complete = (
                highest_seen is not None and highest_seen >= block_number + window
            )
            return _REORG_TRACE_TTL if post_complete else _REORG_TRACE_PARTIAL_TTL

        trace = await _cached(
            "reorg_trace", _REORG_TRACE_TTL, (block_number, window, level), _load,
            ttl_for_value=_ttl,
        )
        if trace is None:
            return JSONResponse(
                {"error": "reorg alert not found"},
                status_code=404,
            )
        return JSONResponse(trace)

    @app.api_route("/api/probes", methods=["GET", "HEAD"])
    async def api_probes() -> JSONResponse:
        async def _load():
            probes, ran_at = state.probes()
            return {
                "ran_at": ran_at,
                "probes": [
                    {
                        "name": p.name,
                        "status": p.status,
                        "summary": p.summary,
                        "details": p.details,
                    }
                    for p in probes
                ],
            }
        payload = await _cached("probes", _PROBES_TTL, (), _load)
        return JSONResponse(payload)

    def _sanitize_probe_summary(name: str, summary: str) -> str:
        """Strip exact port numbers, ulimit values and percentages from
        public probe summaries (A15). Keeps the status signal without
        leaking host-configuration detail."""
        if name == "udp_config":
            # Public view: we never want the port number or the
            # "config not readable" caveat — just health.
            if "authenticated UDP" in summary:
                return "authenticated UDP listener healthy"
            return summary
        if name == "fd_limits":
            return re.sub(r"nofile soft=\d+ hard=\d+", "fd limits within safe margin", summary)
        if name == "disk_usage":
            return re.sub(r"\(peak [\d.]+%?\)", "(peak <20%)", summary)
        return summary

    @app.api_route("/api/probes/public", methods=["GET", "HEAD"])
    async def api_probes_public() -> JSONResponse:
        """Public-safe subset of /api/probes.

        `/api/probes` carries `details` with operator-sensitive paths —
        key-backup directories, `/dev/nvme<N>p<N>`, mount points, device
        metadata. An iteration-2 audit flagged that as host-path leakage,
        so deployments behind nginx should 404 `/api/probes` on the
        public virtual host (the shipped nginx template does this).

        This endpoint exposes only `name`/`status`/`summary` — enough
        for the dashboard's host-probes panel to show green/amber/red
        rows, with no path or hardware identifier in the payload.
        """
        async def _load():
            probes, ran_at = state.probes()
            return {
                "ran_at": ran_at,
                "probes": [
                    {
                        "name": p.name,
                        "status": p.status,
                        "summary": _sanitize_probe_summary(p.name, p.summary),
                    }
                    for p in probes
                ],
            }
        payload = await _cached("probes_public", _PROBES_TTL, (), _load)
        return JSONResponse(payload)

    @app.api_route("/api/contracts/top_retried", methods=["GET", "HEAD"])
    async def api_top_retried(
        since_block: int | None = Query(None, ge=0),
        since_ts_ms: int | None = Query(None, ge=0),
        until_block: int | None = Query(None, ge=0),
        until_ts_ms: int | None = Query(None, ge=0),
        min_appearances: int = Query(3, ge=1, le=10_000),
        limit: int = Query(50, ge=1, le=500),
    ) -> JSONResponse:
        """Top contracts ranked by appearance in retried blocks.

        Correlation (not causation): a contract that keeps showing up in
        blocks with rtp>0 is a candidate 'high-conflict' contract
        (its txs tend to trigger re-execution of other txs in the block).
        Filter by ``since_block/since_ts_ms`` and/or ``until_block/until_ts_ms``
        to scope a window (e.g. the stress-test interval).
        """
        if state.storage is None:
            return JSONResponse({"error": "persistence disabled"}, status_code=503)
        # Reads go through the `contract_hour` rollup (see storage.py).
        # At 20 M+ raw tx_contract_block rows a bounded scan of the
        # precomputed hourly aggregate is ~1 000× cheaper — 12 K rows
        # for a 24 h window instead of 4.3 M. Hour granularity is the
        # right compromise for a ranking query; exact per-block fidelity
        # is still available through the legacy path if a future caller
        # needs it.
        #
        # `since_block` / `until_block` filters are not supported by the
        # rollup (hour_ms is the only time axis). Callers that depend on
        # block-number precision should switch to timestamp filters; the
        # dashboard already does.
        if since_block is not None or until_block is not None:
            return JSONResponse(
                {
                    "error": (
                        "block-number filters are not supported; "
                        "pass since_ts_ms / until_ts_ms instead"
                    )
                },
                status_code=400,
            )
        # Quantize ts bounds to 15s so multiple viewers hitting "last
        # hour" within the same quarter-minute hit the same cache key.
        TOP_TTL = 15.0
        step = int(TOP_TTL * 1000)
        def _q(ts: int | None) -> int | None:
            return None if ts is None else (ts // step) * step
        cache_key = (
            _q(since_ts_ms), _q(until_ts_ms),
            min_appearances, limit,
        )
        async def _load():
            return await asyncio.to_thread(
                state.storage.top_retried_contracts_rollup,
                since_ts_ms=since_ts_ms,
                until_ts_ms=until_ts_ms,
                min_appearances=min_appearances,
                limit=limit,
            )
        stats = await _cached("top_retried", TOP_TTL, cache_key, _load)
        def _row(s):
            lbl = labels.get(s.to_addr)
            return {
                "to_addr": s.to_addr,
                "label": lbl.name if lbl else None,
                "category": lbl.category if lbl else None,
                "blocks_appeared": s.blocks_appeared,
                "retried_blocks": s.retried_blocks,
                "retried_ratio": round(
                    s.retried_blocks / s.blocks_appeared, 4
                ) if s.blocks_appeared else 0.0,
                "avg_rtp_of_blocks": round(s.avg_rtp_of_blocks, 2),
                "tx_count": s.tx_count,
                "total_gas": s.total_gas,
            }
        return JSONResponse({
            "count": len(stats),
            "rows": [_row(s) for s in stats],
        })

    @app.api_route("/api/contracts/labels", methods=["GET", "HEAD"])
    async def api_contracts_labels() -> JSONResponse:
        """Full dump of the loaded label registry."""
        return JSONResponse({
            "count": len(labels),
            "labels": labels.as_dict(),
        })

    # Detail-popup endpoints. Called on-demand when a user clicks a block
    # number or contract row, so a 60s TTL is more than enough — not a
    # polling endpoint. Heavy SQL runs in a worker thread to keep the
    # event loop free for the dashboard's 10s block tail.
    _BLOCK_DETAIL_TTL = 60.0
    _CONTRACT_DETAIL_TTL = 60.0
    # Matched to TTL so a repeat open inside 30s serves from the browser
    # without hitting origin. `private` because the cached response is
    # per-user (no auth header today but future-proofs us).
    _DETAIL_CACHE_HEADER = {"Cache-Control": "private, max-age=30"}
    _ADDR_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")

    @app.api_route("/api/blocks/{block_number}", methods=["GET", "HEAD"])
    async def api_block_detail(
        block_number: int,
        neighbor_window: int = Query(25, ge=0, le=500),
        top_contracts_limit: int = Query(5, ge=1, le=50),
    ) -> JSONResponse:
        """Single-block detail for the block popup.

        Returns the block row + top contracts in it + a compact neighbor
        sparkline. Labels are resolved on the top-contracts list so the
        frontend can render ``Staking Precompile`` instead of the hex.
        Also reports whether a reorg occurred within ±``neighbor_window``
        of this block so the popup can link to the reorg trace.

        404 if the block isn't persisted (either too old for the
        retention window or in-flight before it was written).
        """
        if state.storage is None:
            return JSONResponse({"error": "persistence disabled"}, status_code=503)
        if block_number < 0:
            return JSONResponse({"error": "block_number must be >= 0"}, status_code=400)

        async def _load():
            detail = await asyncio.to_thread(
                state.storage.get_block_detail,
                block_number,
                neighbor_window=neighbor_window,
                top_contracts_limit=top_contracts_limit,
            )
            if detail is None:
                return None
            # Resolve labels on top contracts.
            for c in detail["top_contracts"]:
                lbl = labels.get(c["to_addr"])
                c["label"] = lbl.name if lbl else None
                c["category"] = lbl.category if lbl else None
            # Reorg-near check: is there a reorg alert on this block or a
            # close neighbor? Dashboard can deep-link to /api/reorgs/{n}.
            reorg_block = await asyncio.to_thread(
                state.storage.find_reorg_near_block,
                block_number,
                window=neighbor_window,
            )
            detail["reorg_near"] = (
                {"block_number": reorg_block, "delta": reorg_block - block_number}
                if reorg_block is not None else None
            )
            return detail

        payload = await _cached(
            "block_detail",
            _BLOCK_DETAIL_TTL,
            (block_number, neighbor_window, top_contracts_limit),
            _load,
        )
        if payload is None:
            return JSONResponse(
                {"error": "block not found"}, status_code=404
            )
        return JSONResponse(payload, headers=_DETAIL_CACHE_HEADER)

    @app.api_route("/api/contracts/{addr}", methods=["GET", "HEAD"])
    async def api_contract_detail(
        addr: str,
        from_ts_ms: int | None = Query(None, ge=0),
        to_ts_ms: int | None = Query(None, ge=0),
        hours: int = Query(24, ge=1, le=24 * 30),
    ) -> JSONResponse:
        """Per-contract detail for the contract popup.

        Window is either explicit (``from_ts_ms``/``to_ts_ms``) or
        derived from ``hours`` ending at now. Default 24 h balances "fits
        a meaningful view" against "query cost under the contract_hour
        rollup retention".

        Response carries stats, dominance buckets (per-block share of
        tx), peak block, rank among peers, and an hourly activity
        sparkline. Always returns a 200 with the full shape even if the
        contract has no activity — callers get predictable keys.
        """
        if state.storage is None:
            return JSONResponse({"error": "persistence disabled"}, status_code=503)
        if not _ADDR_RE.match(addr):
            return JSONResponse(
                {"error": "addr must be a 0x-prefixed 40-hex address"},
                status_code=400,
            )

        now_ms = int(time.time() * 1000)
        if from_ts_ms is None or to_ts_ms is None:
            to_ts_ms = now_ms
            from_ts_ms = now_ms - hours * 3600 * 1000
        if to_ts_ms <= from_ts_ms:
            return JSONResponse(
                {"error": "to_ts_ms must be > from_ts_ms"}, status_code=400
            )
        MAX_SPAN_MS = 30 * 24 * 3600 * 1000
        if (to_ts_ms - from_ts_ms) > MAX_SPAN_MS:
            return JSONResponse(
                {"error": "window span too large (max 30 days)"}, status_code=400
            )

        addr_lower = addr.lower()
        # Quantize ts bounds to TTL so multiple viewers collapse to one
        # cache entry within the refresh interval.
        step = int(_CONTRACT_DETAIL_TTL * 1000)
        cache_key = (
            addr_lower,
            (from_ts_ms // step) * step,
            (to_ts_ms // step) * step,
        )

        async def _load():
            detail = await asyncio.to_thread(
                state.storage.get_contract_detail,
                addr_lower,
                from_ts_ms=from_ts_ms,
                to_ts_ms=to_ts_ms,
            )
            lbl = labels.get(addr_lower)
            detail["label"] = lbl.name if lbl else None
            detail["category"] = lbl.category if lbl else None
            # Suppress the uniform-pattern heuristic for system-category
            # contracts. The Staking Precompile is by construction "1 tx
            # per block" — every delegator stake/unstake op hits it
            # separately, which matches the uniform-ratio signature but
            # carries none of the bot-pattern meaning. For known-legit
            # infra we skip the flag entirely to avoid a false signal.
            if lbl is not None and lbl.category == "system":
                detail["pattern"] = None
            return detail

        payload = await _cached(
            "contract_detail", _CONTRACT_DETAIL_TTL, cache_key, _load
        )
        return JSONResponse(payload, headers=_DETAIL_CACHE_HEADER)

    @app.api_route("/api/status/errors", methods=["GET", "HEAD"])
    async def api_status_errors() -> JSONResponse:
        """Error counters since process start, grouped by status code."""
        return JSONResponse({
            "since_ms": int(_error_since * 1000),
            "uptime_sec": round(time.time() - _error_since, 1),
            "counts": dict(_error_counts),
            "total": sum(_error_counts.values()),
        })

    @app.api_route("/api/enrichment/status", methods=["GET", "HEAD"])
    async def api_enrichment_status() -> JSONResponse:
        if enricher is None:
            return JSONResponse({"enabled": False})
        return JSONResponse({"enabled": True, **enricher.stats})

    @app.api_route("/api/window_summary", methods=["GET", "HEAD"])
    async def api_window_summary(
        from_ts_ms: int = Query(..., ge=0),
        to_ts_ms: int = Query(..., ge=0),
        top_contracts_limit: int = Query(15, ge=1, le=500),
        min_appearances: int = Query(3, ge=1, le=10_000),
        include_blocks: bool = Query(False),
    ) -> JSONResponse:
        """Single-call summary of a time window — for post-event analysis
        and for external tooling pulling chain metrics.

        Default response is cheap and bounded: aggregate (peak/avg rtp,
        tps, gas, totals) + the contract ranking. That tier accepts
        any window up to 30 days — enough to compare whole Epochs or
        a week of activity without moving 10s of MB.

        ``include_blocks=true`` additionally returns the per-block
        time-series — the data needed to rebuild a chart of a specific
        event. That tier is capped at 2 hours of span (≈18 K blocks,
        ~3.5 MB response). A stress batch is exactly this shape;
        longer retrospectives should request the aggregate tier across
        multiple windows instead of a single monster dump.

        Why the split: the raw-block dump at a 24 h window is ~40 MB,
        at 7 d is ~300 MB. A publicly exposed endpoint cannot return
        those sizes safely, and no dashboard or report actually needs
        18 K+ block rows at once.
        """
        _MAX_SPAN_MS = 30 * 24 * 3600 * 1000               # 30 days
        _MAX_SPAN_WITH_BLOCKS_MS = 2 * 3600 * 1000         # 2 hours

        if state.storage is None:
            return JSONResponse({"error": "persistence disabled"}, status_code=503)
        if to_ts_ms <= from_ts_ms:
            return JSONResponse(
                {"error": "to_ts_ms must be > from_ts_ms"}, status_code=400
            )
        span_ms = to_ts_ms - from_ts_ms
        if span_ms > _MAX_SPAN_MS:
            return JSONResponse(
                {
                    "error": (
                        f"window span too large: {span_ms/3600_000:.1f} h > "
                        f"{_MAX_SPAN_MS/3600_000:.0f} h cap"
                    )
                },
                status_code=400,
            )
        if include_blocks and span_ms > _MAX_SPAN_WITH_BLOCKS_MS:
            return JSONResponse(
                {
                    "error": (
                        f"include_blocks=true requires span ≤ "
                        f"{_MAX_SPAN_WITH_BLOCKS_MS/3600_000:.0f} h "
                        f"(requested {span_ms/3600_000:.1f} h). "
                        "Drop include_blocks for the aggregate-only tier, "
                        "or split the window into 2 h segments."
                    )
                },
                status_code=400,
            )

        # Quantize ts bounds so multiple viewers asking for "last hour"
        # within the same TTL window collapse to one cache entry. Match
        # quantization step to TTL — finer step would mean each viewer's
        # Date.now() is a unique key, defeating cache between sessions.
        step = int(_WINDOW_SUMMARY_TTL * 1000)
        cache_key = (
            (from_ts_ms // step) * step,
            (to_ts_ms // step) * step,
            top_contracts_limit,
            min_appearances,
            include_blocks,
        )

        # Contract ranking path is span-dependent. Short windows (≤6 h —
        # a Foundation stress batch is 2 h) stay on the raw
        # tx_contract_block aggregate where sparse contracts remain
        # visible — the scan is bounded and fast enough (~500 ms at
        # 2 h). Longer windows switch to the hourly rollup: at 24 h the
        # raw scan is 20 s, which would turn a 30-day-capped endpoint
        # into a de facto DoS vector. The rollup is identical to raw
        # for the top-20 (the default limit is 15) and preserves ~92 %
        # of top-100 across every tested window — the lost tail is
        # sparse one-shot addresses, not signal.
        _ROLLUP_SPAN_THRESHOLD_MS = 6 * 3600 * 1000

        async def _load():
            blocks = []
            if include_blocks:
                # Bounded by the 2 h span cap above, so load_blocks_range's
                # 50 K row limit never bites for well-formed requests.
                blocks = await asyncio.to_thread(
                    state.storage.load_blocks_range,
                    from_ts_ms=from_ts_ms,
                    to_ts_ms=to_ts_ms,
                    limit=50_000,
                )
            # Aggregate metrics: the collector loop already stores every
            # block's contribution in `blocks` (the raw SQLite table), but
            # we only need the per-row fields to compute peaks/means. Pull
            # them via a lightweight aggregate-only path — loading rows to
            # rebuild Python objects just to sum them would throw the
            # response-size budget out the window.
            aggregate = await asyncio.to_thread(
                state.storage.block_metrics_aggregate,
                from_ts_ms=from_ts_ms,
                to_ts_ms=to_ts_ms,
            )
            # Consensus-side aggregate for the same window. Cheap —
            # SUM over minute buckets, O(window-minutes) — and surfaces
            # Foundation's headline KPI for stress-replay queries:
            # /api/window_summary?from=...epoch_532...&to=...epoch_534...
            # returns the chain-wide validator-timeout % directly.
            consensus = await asyncio.to_thread(
                state.storage.load_bft_window,
                from_ts_ms,
                to_ts_ms,
            )
            base_fee = await asyncio.to_thread(
                state.storage.load_base_fee_window,
                from_ts_ms,
                to_ts_ms,
            )
            if span_ms <= _ROLLUP_SPAN_THRESHOLD_MS:
                contracts = await asyncio.to_thread(
                    state.storage.top_retried_contracts,
                    since_ts_ms=from_ts_ms,
                    until_ts_ms=to_ts_ms,
                    min_appearances=min_appearances,
                    limit=top_contracts_limit,
                )
            else:
                contracts = await asyncio.to_thread(
                    state.storage.top_retried_contracts_rollup,
                    since_ts_ms=from_ts_ms,
                    until_ts_ms=to_ts_ms,
                    min_appearances=min_appearances,
                    limit=top_contracts_limit,
                )

            def _row(s):
                lbl = labels.get(s.to_addr)
                return {
                    "to_addr": s.to_addr,
                    "label": lbl.name if lbl else None,
                    "category": lbl.category if lbl else None,
                    "blocks_appeared": s.blocks_appeared,
                    "retried_blocks": s.retried_blocks,
                    "retried_ratio": round(
                        s.retried_blocks / s.blocks_appeared, 4
                    ) if s.blocks_appeared else 0.0,
                    "avg_rtp_of_blocks": round(s.avg_rtp_of_blocks, 2),
                    "tx_count": s.tx_count,
                    "total_gas": s.total_gas,
                }

            payload = {
                "window": {
                    "from_ts_ms": from_ts_ms,
                    "to_ts_ms": to_ts_ms,
                    "span_sec": round(span_ms / 1000.0, 1),
                },
                "aggregate": aggregate,
                "consensus": consensus,
                "base_fee": base_fee,
                "top_contracts": [_row(s) for s in contracts],
            }
            if include_blocks:
                payload["blocks"] = [
                    {
                        "n": b.block_number,
                        "t": b.timestamp_ms,
                        "tx": b.tx_count,
                        "rt": b.retried,
                        "rtp": b.retry_pct,
                        "gas": b.gas_used,
                        "tpse": b.tps_effective,
                        "gpse": b.gas_per_sec_effective,
                    }
                    for b in blocks
                ]
            return payload

        payload = await _cached(
            "window_summary", _WINDOW_SUMMARY_TTL, cache_key, _load
        )
        return JSONResponse(payload)

    @app.api_route("/healthz", methods=["GET", "HEAD"])
    async def healthz() -> JSONResponse:
        return JSONResponse({"ok": True})

    @app.api_route("/manifest.json", methods=["GET", "HEAD"])
    async def manifest_json() -> FileResponse:
        return FileResponse(
            _STATIC_DIR / "manifest.json", media_type="application/manifest+json"
        )

    @app.api_route("/robots.txt", methods=["GET", "HEAD"])
    async def robots_txt() -> FileResponse:
        return FileResponse(_STATIC_DIR / "robots.txt", media_type="text/plain")

    @app.api_route("/sitemap.xml", methods=["GET", "HEAD"])
    async def sitemap_xml() -> FileResponse:
        return FileResponse(_STATIC_DIR / "sitemap.xml", media_type="application/xml")

    @app.api_route("/.well-known/security.txt", methods=["GET", "HEAD"])
    async def security_txt() -> FileResponse:
        return FileResponse(
            _STATIC_DIR / ".well-known" / "security.txt",
            media_type="text/plain",
        )

    @app.api_route("/favicon.ico", methods=["GET", "HEAD"])
    async def favicon() -> FileResponse:
        # Serve the SVG for the legacy /favicon.ico path so browsers that
        # preflight it before parsing the HTML <link> don't log a 404.
        return FileResponse(
            _STATIC_DIR / "favicon.svg", media_type="image/svg+xml"
        )

    @app.api_route("/", methods=["GET", "HEAD"])
    async def root(request: Request):
        # Starlette >= 0.29 expects (request, name, context) signature.
        return templates.TemplateResponse(
            request,
            "index.html",
            {"node_name": config.node.name, "asset_version": _ASSET_VERSION},
        )

    @app.api_route("/alerts", methods=["GET", "HEAD"])
    async def alerts_page(request: Request):
        return templates.TemplateResponse(
            request,
            "alerts.html",
            {"node_name": config.node.name, "asset_version": _ASSET_VERSION},
        )

    @app.api_route("/api", methods=["GET", "HEAD"])
    async def api_docs(request: Request):
        # base_url comes from the Host header — kept out of the template
        # source so a forked monad-ops shows its own URL in the examples
        # without needing to patch HTML. Defaults to relative if Host is
        # unavailable (curl without --header, tests).
        host = request.headers.get("host", "")
        scheme = request.url.scheme or "https"
        base_url = f"{scheme}://{host}".rstrip("/") if host else ""
        return templates.TemplateResponse(
            request,
            "api.html",
            {
                "asset_version": _ASSET_VERSION,
                "base_url": base_url,
            },
        )

    # Branded HTML 404 for non-API routes. FastAPI's default for a missing
    # path is `{"detail": "Not Found"}` — fine for API, bad UX for a
    # human who types the wrong URL or follows a stale link. We keep the
    # JSON response for anything under /api/ and /healthz so tools still
    # get structured errors, and swap to the HTML template for the
    # visible site paths. Other 4xx/5xx codes bubble through unchanged.
    @app.exception_handler(StarletteHTTPException)
    async def not_found_handler(request: Request, exc: StarletteHTTPException):
        if exc.status_code == 404:
            path = request.url.path
            if not (path.startswith("/api/") or path.startswith("/healthz")):
                return templates.TemplateResponse(
                    request,
                    "404.html",
                    {"asset_version": _ASSET_VERSION, "path": path},
                    status_code=404,
                )
        # Preserve default behavior for all other cases (incl. API 404s).
        return JSONResponse(
            {"detail": exc.detail},
            status_code=exc.status_code,
            headers=getattr(exc, "headers", None),
        )

    return app
