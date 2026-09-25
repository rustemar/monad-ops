"""Human-facing pages: dashboard, alerts, incident and replay pages, profile, API docs.

Moved out of ``build_app`` unchanged (queue item R1, slice 6). The curated
incident and replay records live here because only these routes read them. The
branded 404 handler stays in ``build_app``: it is app-wide, not a route.
"""

from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from fastapi.templating import Jinja2Templates
from starlette.exceptions import HTTPException as StarletteHTTPException

from monad_ops.api.context import ApiContext
from monad_ops.config import ApiConfig

# Curated counter for the recovery-path assertion incident class.
# Source-of-truth lives here rather than in a side table because the
# entries are operator-curated (peer observations confirmed in
# conversation, not auto-detected from this node's journal). When a
# third case lands, append to this list and bump the count + last_seen
# in one place; the page and the JSON endpoint stay consistent.
_RECOVERY_PATH_CASES = [
    {"release": "v0.14.1", "month": "2026-04"},
    {"release": "v0.14.3", "month": "2026-05"},
]
_RECOVERY_PATH_LAST_SEEN_LABEL = "2026-05-18"

# Stress-event replay archive — listing of past Foundation stress
# windows with stable URLs. New events append a row here. Same
# static-curated pattern as _RECOVERY_PATH_CASES; auto-detection
# would be nice but stress events are rare enough (few per year)
# that manual curation costs nothing and avoids false-positive
# noise from non-stress retry spikes.
_REPLAY_EVENTS = [
    {
        "id": "2026-04-20",
        "page_url": "/replay/2026-04-20",
        "title": "Foundation testnet stress test — 2026-04-20",
        "date_label": "2026-04-20",
        "summary": (
            "Three 2-hour batches across epochs 532 / 533 / 534. "
            "~3–5k TPS, ~400 M gas/s, validator-timeout < 3% per Foundation summary."
        ),
        "api_base": "/api/window_summary?from_ts_ms=",
        "batches": [
            {"label": "epoch 532 (00:00 → 05:37 UTC)",
             "from_ts": 1776643200000, "to_ts": 1776663420000},
            {"label": "epoch 533 (05:37 → 11:14 UTC)",
             "from_ts": 1776663420000, "to_ts": 1776683640000},
            {"label": "epoch 534 (11:14 → 16:50 UTC)",
             "from_ts": 1776683640000, "to_ts": 1776703800000},
        ],
    },
]


def build_router(
    ctx: ApiContext,
    *,
    templates: Jinja2Templates,
    asset_version: str,
    api_rate_limit: ApiConfig | None,
) -> APIRouter:
    """Page routes. ``api_rate_limit`` is the limit the API docs page shows, or
    None when rate limiting is off."""
    router = APIRouter()

    @router.api_route("/", methods=["GET", "HEAD"])
    async def root(request: Request):
        # Starlette >= 0.29 expects (request, name, context) signature.
        return templates.TemplateResponse(
            request,
            "index.html",
            {"node_name": ctx.config.node.name, "asset_version": asset_version},
        )

    @router.api_route("/alerts", methods=["GET", "HEAD"])
    async def alerts_page(request: Request):
        return templates.TemplateResponse(
            request,
            "alerts.html",
            {"node_name": ctx.config.node.name, "asset_version": asset_version},
        )

    @router.api_route("/api/incidents/recovery-path-assertion", methods=["GET", "HEAD"])
    async def api_incidents_recovery_path() -> JSONResponse:
        """Curated counter for the recovery-path assertion class.

        Static payload — incremented by hand when a new case is
        confirmed via GitHub issue. The API exists so the dashboard
        tile can display the current count without templating the
        value into the HTML at build time.
        """
        return JSONResponse({
            "slug": "recovery-path-assertion",
            "title": "Recovery-path assertion stall class",
            "count": len(_RECOVERY_PATH_CASES),
            "last_seen_label": _RECOVERY_PATH_LAST_SEEN_LABEL,
            "releases": sorted({c["release"] for c in _RECOVERY_PATH_CASES}),
            "page_url": "/incidents/recovery-path-assertion",
        })

    @router.api_route("/incidents/recovery-path-assertion", methods=["GET", "HEAD"])
    async def incidents_recovery_path_page(request: Request):
        return templates.TemplateResponse(
            request,
            "incidents/recovery_path_assertion.html",
            {
                "asset_version": asset_version,
                "incident_count": len(_RECOVERY_PATH_CASES),
                "last_seen_label": _RECOVERY_PATH_LAST_SEEN_LABEL,
            },
        )

    @router.api_route("/api/replay", methods=["GET", "HEAD"])
    async def api_replay_index() -> JSONResponse:
        """Stress-event replay archive index — JSON shape so the HTML
        page and any external integrator share the same source of
        truth."""
        return JSONResponse({"events": _REPLAY_EVENTS})

    @router.api_route("/replay/{event_id}", methods=["GET", "HEAD"])
    async def replay_event_page(request: Request, event_id: str):
        event = next((e for e in _REPLAY_EVENTS if e["id"] == event_id), None)
        if event is None:
            raise StarletteHTTPException(status_code=404)
        host = request.headers.get("host", "")
        scheme = request.url.scheme or "https"
        base_url = f"{scheme}://{host}".rstrip("/") if host else ""
        return templates.TemplateResponse(
            request,
            "replay/event.html",
            {
                "asset_version": asset_version,
                "base_url": base_url,
                "event": event,
            },
        )

    @router.api_route("/replay/", methods=["GET", "HEAD"])
    @router.api_route("/replay", methods=["GET", "HEAD"])
    async def replay_index_page(request: Request):
        host = request.headers.get("host", "")
        scheme = request.url.scheme or "https"
        base_url = f"{scheme}://{host}".rstrip("/") if host else ""
        return templates.TemplateResponse(
            request,
            "replay/index.html",
            {
                "asset_version": asset_version,
                "base_url": base_url,
                "events": _REPLAY_EVENTS,
            },
        )

    @router.api_route("/profile/{handle}", methods=["GET", "HEAD"])
    async def profile_page(request: Request, handle: str):
        op = ctx.config.operator
        if not op.enabled or not op.handle or handle != op.handle:
            raise StarletteHTTPException(status_code=404)
        return templates.TemplateResponse(
            request,
            "profile/operator.html",
            {
                "asset_version": asset_version,
                "operator": op,
            },
        )

    @router.api_route("/api", methods=["GET", "HEAD"])
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
                "asset_version": asset_version,
                "base_url": base_url,
                "rate_limit": api_rate_limit,
            },
        )

    return router
