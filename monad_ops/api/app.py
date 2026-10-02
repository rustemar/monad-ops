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
import subprocess
import time
from collections import OrderedDict
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.exceptions import HTTPException as StarletteHTTPException

from monad_ops.api.context import ApiContext
from monad_ops.api.ratelimit import TokenBucketLimiter, client_key
from monad_ops.api.routes import alerts as alerts_routes
from monad_ops.api.routes import blocks as blocks_routes
from monad_ops.api.routes import contracts as contracts_routes
from monad_ops.api.routes import details as details_routes
from monad_ops.api.routes import meta as meta_routes
from monad_ops.api.routes import node as node_routes
from monad_ops.api.routes import pages as pages_routes
from monad_ops.api.routes import reorgs as reorgs_routes
from monad_ops.api.routes import series as series_routes
from monad_ops.api.routes import status as status_routes
from monad_ops.api.routes import window as window_routes
from monad_ops.config import Config
from monad_ops.enricher import EnrichmentWorker
from monad_ops.labels import ContractLabels
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
    _CHANGES_TTL = 60.0       # git log of the checkout; changes once a day at most

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

    # Node info (probes, version, validator set) lives in its own module
    # (queue item R1), mounted where the probes used to be declared. The
    # validator set now registers before /api/changes; neither takes a path
    # parameter.
    app.include_router(node_routes.build_router(ctx))

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

    # Contract ranking and labels live in their own module (queue item R1),
    # mounted where they were declared: still ahead of the details router's
    # /api/contracts/{addr}.
    app.include_router(contracts_routes.build_router(ctx))

    # Block/contract detail popups live in their own module (queue item R1);
    # mounted here so they still come after the literal /api/blocks/* and
    # /api/contracts/* routes above.
    app.include_router(details_routes.build_router(ctx))

    # Status endpoints live in their own module (queue item R1), mounted where
    # they were declared; they get the middleware's counter and the enricher.
    app.include_router(
        status_routes.build_router(
            ctx,
            error_counts=_error_counts,
            error_since=_error_since,
            enricher=enricher,
        )
    )

    # Window summaries live in their own module (queue item R1), using
    # the shared cache and labels through the same app context.
    app.include_router(window_routes.build_router(ctx))

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
