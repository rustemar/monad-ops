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
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.exceptions import HTTPException as StarletteHTTPException

from monad_ops.api.context import ApiContext
from monad_ops.api.ratelimit import TokenBucketLimiter, client_key
from monad_ops.api.routes import alerts as alerts_routes
from monad_ops.api.routes import blocks as blocks_routes
from monad_ops.api.routes import details as details_routes
from monad_ops.api.routes import meta as meta_routes
from monad_ops.api.routes import pages as pages_routes
from monad_ops.api.routes import reorgs as reorgs_routes
from monad_ops.api.routes import series as series_routes
from monad_ops.config import Config
from monad_ops.enricher import EnrichmentWorker
from monad_ops.labels import ContractLabels
from monad_ops.parser import drift
from monad_ops.state import State

_THIS_DIR = Path(__file__).parent
_PKG_DIR = _THIS_DIR.parent
_REPO_DIR = _PKG_DIR.parent
_TEMPLATE_DIR = _PKG_DIR / "dashboard" / "templates"
_STATIC_DIR = _PKG_DIR / "dashboard" / "static"


def _git(*args: str) -> str | None:
    """One read-only git command against the checkout; None when git cannot answer."""
    try:
        out = subprocess.check_output(
            ["git", "-C", str(_REPO_DIR), *args],
            stderr=subprocess.DEVNULL,
            timeout=2,
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError):
        return None
    return out.decode(errors="replace").strip()


def _git_head() -> str | None:
    return _git("rev-parse", "--short", "HEAD") or None


def _git_head_full() -> str | None:
    return _git("rev-parse", "HEAD") or None


def _git_recent_commits(limit: int = 8) -> list[dict] | None:
    """Newest-first ``{commit, committed_at, subject}`` rows, or None outside a checkout.

    Lists the pushed tip when the branch tracks one, so every row exists on
    the public repo; only subjects and short hashes reach the wire.
    """
    ref = "@{u}" if _git("rev-parse", "--verify", "-q", "@{u}") else "HEAD"
    out = _git("log", f"-n{limit}", "--format=%h%x1f%ct%x1f%s", ref)
    if out is None:
        return None
    rows: list[dict] = []
    for line in out.splitlines():
        parts = line.split("\x1f", 2)
        if len(parts) != 3 or not parts[1].isdigit():
            continue
        rows.append({"commit": parts[0], "committed_at": int(parts[1]), "subject": parts[2]})
    return rows


def _asset_version() -> str:
    """Version string for cache-busting CSS/JS via ?v=... query param.

    Combines the short git HEAD hash (human-readable) with the newest
    template/static mtime (catches uncommitted edits after a restart).
    Either part can be missing; the result still changes whenever either
    changes.
    """
    git_part = _git_head() or "dev"
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
# What this process is running: HEAD at import time. Compared against the live
# HEAD later so a commit made without a restart shows up as such.
_RUNNING_COMMIT = _git_head()
_RUNNING_COMMIT_FULL = _git_head_full()
_STARTED_AT = time.time()


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

    # Registered before the error counter so a 429 is still counted. CORS
    # sits inside this layer, hence the explicit allow-origin on the 429.
    _api_cfg = config.api
    _limiter = (
        TokenBucketLimiter(_api_cfg.rate_limit_requests, _api_cfg.rate_limit_window_sec)
        if _api_cfg.rate_limit_enabled else None
    )

    @app.middleware("http")
    async def _rate_limit(request: Request, call_next):
        if _limiter is None or not request.url.path.startswith("/api/"):
            return await call_next(request)
        verdict = _limiter.check(client_key(request))
        if not verdict.allowed:
            return JSONResponse(
                {"error": "rate_limited", "retry_after_sec": verdict.retry_after_sec},
                status_code=429,
                headers={
                    "Retry-After": str(verdict.retry_after_sec),
                    "X-RateLimit-Limit": str(_api_cfg.rate_limit_requests),
                    "X-RateLimit-Remaining": "0",
                    "Access-Control-Allow-Origin": "*",
                },
            )
        response = await call_next(request)
        response.headers["X-RateLimit-Limit"] = str(_api_cfg.rate_limit_requests)
        response.headers["X-RateLimit-Remaining"] = str(verdict.remaining)
        return response

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
    _PROBES_TTL = 60.0        # probes loop runs every ~30s host-side
    _VERSION_TTL = 30.0       # version_watch runs hourly; short cache surfaces an upgrade fast
    _CHANGES_TTL = 60.0       # git log of the checkout; changes once a day at most
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

    # One handle shared by every extracted route group (queue item R1).
    # Built here because it carries _cached, which the groups read through.
    ctx = ApiContext(
        state=state,
        config=config,
        cached=_cached,
        labels=labels,
        journal_capture_dir=journal_capture_dir,
    )

    # Prewarm helper removed — since the top_retried read path moved to
    # the contract_hour rollup (sub-50 ms for 24 h), keeping an extra
    # TTL-cache warmer would just duplicate hot data already served by
    # the rollup + 15 s cache. The cli.py warm_top_retried_shapes task
    # is likewise no longer registered.

    # State and block endpoints live in their own module (queue item R1).
    # `/api/blocks/range` now sits before the chart series; no route between
    # them takes a path parameter, and all the literal `/api/blocks/*` paths
    # still come before the `/api/blocks/{block_number}` detail route.
    app.include_router(blocks_routes.build_router(ctx))

    # Chart series live in their own module (queue item R1); mounted here so
    # the route order around them is unchanged.
    app.include_router(series_routes.build_router(ctx))

    # Alert endpoints live in their own module (queue item R1); mounted here
    # so the route order around them is unchanged.
    app.include_router(alerts_routes.build_router(ctx))

    # Reorg endpoints live in their own module (queue item R1); they are
    # mounted here, where they used to be declared, so route order is
    # unchanged.
    app.include_router(reorgs_routes.build_router(ctx))

    def _sanitize_probe_summary(name: str, summary: str) -> str:
        """Strip exact port numbers, ulimit values and percentages from
        probe summaries. Keeps the status signal without leaking
        host-configuration detail."""
        if name == "udp_config":
            # We never want the port number or the "config not readable"
            # caveat — just health.
            if "authenticated UDP" in summary:
                return "authenticated UDP listener healthy"
            return summary
        if name == "fd_limits":
            return re.sub(r"nofile soft=\d+ hard=\d+", "fd limits within safe margin", summary)
        if name == "disk_usage":
            return re.sub(r"\(peak [\d.]+%?\)", "(peak <20%)", summary)
        return summary

    async def _api_probes_payload() -> dict:
        """Sanitized host-probe payload — name, status, summary only.

        The earlier two-tier architecture (`/api/probes` with `details` +
        `/api/probes/public` sanitized) was retired 2026-05-03: the
        `/public` suffix implied a `/private` counterpart that no longer
        exists, and the operator-sensitive `details` field (key-backup
        paths, `/dev/nvme<N>p<N>`, ulimit values) is information the
        operator already has via shell access. The single endpoint here
        is what the dashboard renders and what external clients see.
        """
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

    @app.api_route("/api/probes", methods=["GET", "HEAD"])
    async def api_probes() -> JSONResponse:
        payload = await _cached("probes", _PROBES_TTL, (), _api_probes_payload)
        return JSONResponse(payload)

    @app.api_route("/api/probes/public", methods=["GET", "HEAD"])
    async def api_probes_public() -> JSONResponse:
        """Backwards-compat alias of /api/probes.

        Pre-2026-05-03 callers (README + bookmarks) hit /public. Returns
        the same payload as /api/probes so nothing breaks during the
        transition; can be removed once external references migrate.
        """
        payload = await _cached("probes", _PROBES_TTL, (), _api_probes_payload)
        return JSONResponse(payload)

    @app.api_route("/api/version", methods=["GET", "HEAD"])
    async def api_version() -> JSONResponse:
        """Locally-installed monad package vs. apt repo.

        Powers the "node version" tile on the dashboard. Returns the
        same shape whether the operator is up to date, has an upgrade
        pending, or the probe could not run — the UI special-cases
        each ``status`` value.

        Public-safe: contains only package name + version strings + the
        configured repo URL — no host paths, no PIDs, no metadata about
        the host OS.
        """
        async def _load():
            status, checked_at = state.version()
            if status is None:
                return {
                    "package": config.version_watch.package,
                    "installed": None,
                    "latest": None,
                    "extras_newer": [],
                    "status": "unknown",
                    "error": "version_watch has not run yet",
                    "checked_at": None,
                    "pending_since": None,
                    "packages_url": config.version_watch.packages_url,
                    "enabled": config.version_watch.enabled,
                }
            return {
                "package": status.package,
                "installed": status.installed,
                "latest": status.latest,
                "extras_newer": list(status.extras_newer),
                "status": status.status,
                "error": status.error,
                "checked_at": checked_at,
                "pending_since": (
                    state.version_pending_since(status.latest)
                    if status.status == "update_available" else None
                ),
                "packages_url": config.version_watch.packages_url,
                "enabled": config.version_watch.enabled,
            }
        payload = await _cached("version", _VERSION_TTL, (), _load)
        return JSONResponse(payload)

    @app.api_route("/api/changes", methods=["GET", "HEAD"])
    async def api_changes() -> JSONResponse:
        """Recent commits of this monad-ops checkout and the commit the process runs.

        Powers the "recent changes" card: a visitor sees the dashboard is
        maintained, the operator sees when the checkout has moved past what
        is running. Public-safe: short hashes, commit times and subjects.
        ``enabled`` is false when the code is not a git checkout.
        """
        def _read_git() -> tuple[list[dict] | None, str | None, str | None]:
            return _git_recent_commits(8), _git_head(), _git_head_full()

        async def _load():
            commits, head, head_full = await asyncio.to_thread(_read_git)
            repo_url = (config.operator.public_repo or None) if config.operator.enabled else None
            return {
                "enabled": commits is not None,
                "error": None if commits is not None else "not a git checkout",
                "repo_url": repo_url,
                "running": {"commit": _RUNNING_COMMIT, "started_at": _STARTED_AT},
                "head": head,
                # Full hashes: short ones can grow as the object count does.
                "restart_pending": bool(
                    head_full and _RUNNING_COMMIT_FULL and head_full != _RUNNING_COMMIT_FULL
                ),
                "commits": commits or [],
            }
        payload = await _cached("changes", _CHANGES_TTL, (), _load)
        return JSONResponse(payload)

    @app.api_route("/api/validator_set", methods=["GET", "HEAD"])
    async def api_validator_set() -> JSONResponse:
        """Active validator-set snapshot from the staking precompile.

        Powers the "active validator set" tile on the dashboard. The
        snapshot itself changes on epoch boundaries (~5.5h on testnet);
        polled every 5 min by ``validator_set_loop`` in cli.py.

        Public-safe: returns only protocol constants + on-chain counts
        + the cutoff stake (already public via the precompile). No host
        metadata or peer-level identity.
        """
        async def _load():
            snapshot, checked_at = state.validator_set()
            if snapshot is None:
                return {
                    "enabled": config.validator_set.enabled,
                    "status": "unknown",
                    "error": "validator_set probe has not run yet",
                    "checked_at": None,
                    "epoch": None,
                    "in_epoch_delay": False,
                    "consensus_count": None,
                    "execution_count": None,
                    "bench_count": None,
                    "lowest_active_stake_wei": None,
                    "active_valset_cap": 200,
                    "active_validator_stake_mon": 10_000_000,
                    "min_auth_address_stake_mon": 100_000,
                }
            return {
                "enabled": config.validator_set.enabled,
                "status": snapshot.status,
                "error": snapshot.error,
                "checked_at": checked_at,
                "epoch": snapshot.epoch,
                "in_epoch_delay": snapshot.in_epoch_delay,
                "consensus_count": snapshot.consensus_count,
                "execution_count": snapshot.execution_count,
                "bench_count": snapshot.bench_count,
                "lowest_active_stake_wei": (
                    str(snapshot.lowest_active_stake_wei)
                    if snapshot.lowest_active_stake_wei is not None else None
                ),
                "active_valset_cap": 200,
                "active_validator_stake_mon": 10_000_000,
                "min_auth_address_stake_mon": 100_000,
            }
        payload = await _cached("validator_set", 30.0, (), _load)
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

    # Block/contract detail popups live in their own module (queue item R1);
    # mounted here so they still come after the literal /api/blocks/* and
    # /api/contracts/* routes above.
    app.include_router(details_routes.build_router(ctx))

    @app.api_route("/api/status/errors", methods=["GET", "HEAD"])
    async def api_status_errors() -> JSONResponse:
        """Error counters since process start, grouped by status code.

        ``parse_drift`` covers the other silent failure: per log-line
        kind, how many lines the parser recognised but could not extract
        (``drift``, zero at steady state) against how many it did parse
        (``ok``, which goes flat if a marker disappears entirely).
        """
        return JSONResponse({
            "since_ms": int(_error_since * 1000),
            "uptime_sec": round(time.time() - _error_since, 1),
            "counts": dict(_error_counts),
            "total": sum(_error_counts.values()),
            "parse_drift": drift.snapshot(),
        })

    @app.api_route("/api/enrichment/status", methods=["GET", "HEAD"])
    async def api_enrichment_status() -> JSONResponse:
        """Receipts-enrichment worker: lifetime counters plus the rule's
        current verdict.

        The counters are cumulative since process start and answer "has
        this ever gone wrong"; ``health`` is the rolling-window view and
        answers "is it wrong now". Both are needed — an outage that
        recovered leaves a permanent gap in the contract tables, so the
        lifetime ``failed``/``dropped`` totals stay operationally
        interesting long after the alert cleared.

        Public-safe: counts and a status word, no host metadata.
        """
        if enricher is None:
            return JSONResponse({"enabled": False})
        health, checked_at = state.enrichment_health()
        return JSONResponse({
            "enabled": True,
            **enricher.stats,
            "checked_at": checked_at,
            "health": {
                "status": "unknown",
                "failing": False,
                "dropping": False,
                "window_attempts": 0,
                "window_failed": 0,
                "fail_pct": None,
                "samples": 0,
            } if health is None else {
                "status": health.status,
                "failing": health.failing,
                "dropping": health.dropping,
                "window_attempts": health.window_attempts,
                "window_failed": health.window_failed,
                "fail_pct": health.fail_pct,
                "samples": health.samples,
            },
        })

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

    # Site metadata lives in its own module (queue item R1); mounted here so
    # the route order around it is unchanged.
    app.include_router(meta_routes.build_router(_STATIC_DIR))

    # Pages live in their own module (queue item R1); mounted here so the
    # route order around them is unchanged.
    app.include_router(
        pages_routes.build_router(
            ctx,
            templates=templates,
            asset_version=_ASSET_VERSION,
            api_rate_limit=_api_cfg if _limiter is not None else None,
        )
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
