"""Contract endpoints: the retried-contract ranking and the label registry.

Moved out of ``build_app`` unchanged (queue item R1). Mounted ahead of the
details router so these literal ``/api/contracts/*`` paths win over
``/api/contracts/{addr}``.
"""

from __future__ import annotations

import asyncio

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from monad_ops.api.context import ApiContext


def build_router(ctx: ApiContext) -> APIRouter:
    """Contract routes bound to ``ctx``."""
    router = APIRouter()

    @router.api_route("/api/contracts/top_retried", methods=["GET", "HEAD"])
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
        if ctx.state.storage is None:
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
                ctx.state.storage.top_retried_contracts_rollup,
                since_ts_ms=since_ts_ms,
                until_ts_ms=until_ts_ms,
                min_appearances=min_appearances,
                limit=limit,
            )
        stats = await ctx.cached("top_retried", TOP_TTL, cache_key, _load)
        def _row(s):
            lbl = ctx.labels.get(s.to_addr)
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

    @router.api_route("/api/contracts/labels", methods=["GET", "HEAD"])
    async def api_contracts_labels() -> JSONResponse:
        """Full dump of the loaded label registry."""
        return JSONResponse({
            "count": len(ctx.labels),
            "labels": ctx.labels.as_dict(),
        })

    return router
