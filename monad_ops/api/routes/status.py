"""Service status endpoints: HTTP error counters with parser drift, and the
receipts-enrichment worker.

Moved out of ``build_app`` unchanged (queue item R1). Both read state that
``build_app`` owns rather than ``ApiContext``: the error counter its middleware
fills (shared, mutated in place), the counter's start time, and the enricher.
"""

from __future__ import annotations

import time
from collections import Counter

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from monad_ops.api.context import ApiContext
from monad_ops.enricher import EnrichmentWorker
from monad_ops.parser import drift


def build_router(
    ctx: ApiContext,
    *,
    error_counts: Counter[int],
    error_since: float,
    enricher: EnrichmentWorker | None,
) -> APIRouter:
    """Status routes bound to ``ctx`` and the app-owned counters."""
    router = APIRouter()

    @router.api_route("/api/status/errors", methods=["GET", "HEAD"])
    async def api_status_errors() -> JSONResponse:
        """Error counters since process start, grouped by status code.

        ``parse_drift`` covers the other silent failure: per log-line
        kind, how many lines the parser recognised but could not extract
        (``drift``, zero at steady state) against how many it did parse
        (``ok``, which goes flat if a marker disappears entirely).
        """
        return JSONResponse({
            "since_ms": int(error_since * 1000),
            "uptime_sec": round(time.time() - error_since, 1),
            "counts": dict(error_counts),
            "total": sum(error_counts.values()),
            "parse_drift": drift.snapshot(),
        })

    @router.api_route("/api/enrichment/status", methods=["GET", "HEAD"])
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
        health, checked_at = ctx.state.enrichment_health()
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

    return router
