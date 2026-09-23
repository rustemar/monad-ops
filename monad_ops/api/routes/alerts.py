"""Alert endpoints: the in-memory tail, the stored history, stress events.

Moved out of ``build_app`` unchanged (queue item R1, slice 4). Stress events
live here because they are read from the alerts table, not from blocks.
"""

from __future__ import annotations

import asyncio
import time

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from monad_ops.api.context import ApiContext
from monad_ops.rules.events import CodeColor, code_color_for, severities_for

# In-memory recent-alerts tail.
_ALERTS_TTL = 5.0
# Alerts table is append-only; live envelope updates need fresh reads.
_STRESS_EVENTS_TTL = 10.0

# How many envelopes get a profile. Each one costs a bounded
# aggregate query, so the cap is what keeps a limit=50 request from
# turning a cheap list call into 50 scans.
_STRESS_PROFILE_MAX = 10

# Profile fields lifted from the window aggregate. The full
# aggregate carries gas-per-second and averages the button row has
# no use for; this is the subset that answers "how big was it".
_STRESS_PROFILE_FIELDS = (
    "blocks", "peak_rtp", "avg_rtp", "peak_tps", "avg_tps",
    "total_tx", "total_retried", "total_gas",
)


def build_router(ctx: ApiContext) -> APIRouter:
    """Alert routes bound to ``ctx``."""
    router = APIRouter()

    @router.api_route("/api/alerts", methods=["GET", "HEAD"])
    async def api_alerts(
        limit: int = Query(50, ge=1, le=200),
    ) -> JSONResponse:
        async def _load():
            alerts = ctx.state.recent_alerts_with_ts(limit=limit)
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
        payload = await ctx.cached("alerts", _ALERTS_TTL, (limit,), _load)
        return JSONResponse(payload)

    @router.api_route("/api/alerts/history", methods=["GET", "HEAD"])
    async def api_alerts_history(
        from_ts_ms: int | None = Query(None, ge=0),
        to_ts_ms: int | None = Query(None, ge=0),
        severity: str | None = Query(None, pattern="^(critical|warn|info|recovered)$"),
        code_color: str | None = Query(None, pattern="^(red|orange|green)$"),
        limit: int = Query(500, ge=1, le=5000),
    ) -> JSONResponse:
        """Historical alerts from persistent storage, filterable.

        Unlike /api/alerts (in-memory tail, cleared on restart), this
        reads the sqlite `alerts` table with optional ts/severity
        filters. ``code_color`` filters by the Foundation colour code
        instead (GREEN covers both info and recovered) and cannot be
        combined with ``severity``. Returns newest-first.
        """
        if ctx.state.storage is None:
            return JSONResponse({"error": "persistence disabled"}, status_code=503)
        if severity and code_color:
            raise StarletteHTTPException(
                status_code=422, detail="use either severity or code_color, not both",
            )
        sev_filter: str | list[str] | None = severity
        if code_color:
            sev_filter = [s.value for s in severities_for(CodeColor(code_color))]
        rows = ctx.state.storage.load_alerts_range(
            from_ts=(from_ts_ms / 1000.0) if from_ts_ms is not None else None,
            to_ts=(to_ts_ms / 1000.0) if to_ts_ms is not None else None,
            severity=sev_filter,
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

    @router.api_route("/api/stress_events", methods=["GET", "HEAD"])
    async def api_stress_events(
        limit: int = Query(5, ge=1, le=50),
        max_age_days: int = Query(30, ge=1, le=365),
        merge_gap_sec: int = Query(1800, ge=60, le=86400),
        include_profile: bool = Query(True),
    ) -> JSONResponse:
        """Past and ongoing stress events, with what each one did.

        Walks the alerts table for retry_spike CRITICAL/RECOVERED runs,
        groups consecutive criticals into envelopes, then merges
        envelopes whose recovery-to-rearm gap is short enough that they
        are semantically one event. Used by the dashboard's stress-event
        button row to let an operator jump straight to the window of a
        past or ongoing stress test without picking dates by hand.
        Backed by ``Storage.list_stress_envelopes`` — see that method
        for the merge-gap rationale (default 30 min covers within-batch
        dips without merging real between-batch silence).

        ``include_profile`` (default on) adds the shape of each event —
        block count, peak and average retry_pct, peak per-block
        effective TPS, realized average TPS across the window, and
        transaction and gas totals — from the same SQL aggregate
        ``/api/window_summary`` uses. The two TPS figures answer
        different questions and differ by an order of magnitude on a
        real event: ``peak_tps`` is the fastest single block, ``avg_tps``
        is total transactions over the whole span, gaps included. Without it the caller
        knows an event happened and has to run a second query to learn
        anything about it, which is what the 2026-09-10 load test made
        obvious: the envelope was in the list within a minute, and every
        number worth reporting still had to be assembled by hand.

        A live envelope has ``to_ts_ms: null``; its profile is computed
        to wall-clock now, so it grows while the event runs.
        """
        if ctx.state.storage is None:
            return JSONResponse({"error": "persistence disabled"}, status_code=503)

        async def _load():
            rows = await asyncio.to_thread(
                ctx.state.storage.list_stress_envelopes,
                limit=limit,
                max_age_days=max_age_days,
                merge_gap_sec=float(merge_gap_sec),
            )
            if include_profile:
                now_ms = int(time.time() * 1000)
                for row in rows[:_STRESS_PROFILE_MAX]:
                    aggregate = await asyncio.to_thread(
                        ctx.state.storage.block_metrics_aggregate,
                        from_ts_ms=int(row["from_ts_ms"]),
                        to_ts_ms=int(row["to_ts_ms"] or now_ms),
                    )
                    row["profile"] = {
                        k: aggregate[k] for k in _STRESS_PROFILE_FIELDS
                    }
            return {"count": len(rows), "events": rows}

        payload = await ctx.cached(
            "stress_events", _STRESS_EVENTS_TTL,
            (limit, max_age_days, merge_gap_sec, include_profile),
            _load,
        )
        return JSONResponse(payload)

    return router
