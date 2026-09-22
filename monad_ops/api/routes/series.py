"""Chart time-series endpoints: consensus, reorg counts, base fee.

Moved out of ``build_app`` unchanged (queue item R1, slice 3). All three answer
the same question shape — bucket a window into per-minute rows for a chart — and
each rejects an oversized span itself rather than trusting the caller.
"""

from __future__ import annotations

import asyncio

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from monad_ops.api.context import ApiContext


def build_router(ctx: ApiContext) -> APIRouter:
    """Series routes bound to ``ctx``."""
    router = APIRouter()

    @router.api_route("/api/bft_series", methods=["GET", "HEAD"])
    async def api_bft_series(
        from_ts_ms: int = Query(..., ge=0),
        to_ts_ms: int = Query(..., ge=0),
    ) -> JSONResponse:
        """Per-minute consensus series for the validator-timeout chart.

        1-min resolution is the storage cadence — no client-side bucketing
        and no server-side downsampling. 7d window is the chart toolbar's
        max preset; at 1-min resolution that's 10080 buckets (~2 MB JSON),
        small enough to ship raw without losing single-bucket spikes that
        downsampling would smooth away.
        """
        if ctx.state.storage is None:
            return JSONResponse({"error": "persistence disabled"}, status_code=503)
        if to_ts_ms <= from_ts_ms:
            return JSONResponse(
                {"error": "to_ts_ms must be > from_ts_ms"}, status_code=400
            )
        MAX_SPAN_MS = 7 * 86400 * 1000
        span_ms = to_ts_ms - from_ts_ms
        if span_ms > MAX_SPAN_MS:
            return JSONResponse(
                {"error": "span too large (max 7 days)"}, status_code=400
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
                ctx.state.storage.list_bft_minutes,
                from_ts_ms,
                to_ts_ms,
                limit=10_500,
            )
            return {
                "from_ts_ms": from_ts_ms,
                "to_ts_ms": to_ts_ms,
                "bin_ms": 60_000,
                "bins": bins,
            }
        payload = await ctx.cached("bft_series", 5.0, cache_key, _load)
        return JSONResponse(payload)

    @router.api_route("/api/reorg_series", methods=["GET", "HEAD"])
    async def api_reorg_series(
        from_ts_ms: int = Query(..., ge=0),
        to_ts_ms: int = Query(..., ge=0),
    ) -> JSONResponse:
        """Per-minute reorg event counts split into single (info) vs cluster
        (warn). Pre-2026-05-03 critical-tier alerts are bucketed as cluster
        for visualization consistency."""
        if ctx.state.storage is None:
            return JSONResponse({"error": "persistence disabled"}, status_code=503)
        if to_ts_ms <= from_ts_ms:
            return JSONResponse(
                {"error": "to_ts_ms must be > from_ts_ms"}, status_code=400
            )
        MAX_SPAN_MS = 7 * 86400 * 1000
        if to_ts_ms - from_ts_ms > MAX_SPAN_MS:
            return JSONResponse(
                {"error": "span too large (max 7 days)"}, status_code=400
            )
        step = 5_000
        cache_key = (
            (from_ts_ms // step) * step,
            (to_ts_ms // step) * step,
        )

        async def _load():
            bins = await asyncio.to_thread(
                ctx.state.storage.list_reorg_minutes,
                from_ts_ms,
                to_ts_ms,
            )
            return {
                "from_ts_ms": from_ts_ms,
                "to_ts_ms": to_ts_ms,
                "bin_ms": 60_000,
                "bins": bins,
            }
        payload = await ctx.cached("reorg_series", 5.0, cache_key, _load)
        return JSONResponse(payload)

    @router.api_route("/api/base_fee_series", methods=["GET", "HEAD"])
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
        if ctx.state.storage is None:
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
                ctx.state.storage.sampled_bft_base_fee,
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
        payload = await ctx.cached("base_fee_series", ttl_sec, cache_key, _load)
        return JSONResponse(payload)

    return router
