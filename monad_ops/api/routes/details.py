"""Block and contract detail endpoints — the dashboard's click-through popups.

Moved out of ``build_app`` unchanged (queue item R1, slice 2). Both are
on-demand reads with their own cache header, so their TTLs and the address
pattern live here rather than among the polling endpoints' constants.

Mounted after the literal ``/api/blocks/*`` and ``/api/contracts/*`` routes:
FastAPI matches in registration order, so ``/api/blocks/sampled`` and
``/api/contracts/labels`` have to be declared before these parameterised paths.
"""

from __future__ import annotations

import asyncio
import re
import time

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from monad_ops.api.context import ApiContext

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


def build_router(ctx: ApiContext) -> APIRouter:
    """Detail routes bound to ``ctx``."""
    router = APIRouter()

    @router.api_route("/api/blocks/{block_number}", methods=["GET", "HEAD"])
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
        if ctx.state.storage is None:
            return JSONResponse({"error": "persistence disabled"}, status_code=503)
        if block_number < 0:
            return JSONResponse({"error": "block_number must be >= 0"}, status_code=400)

        async def _load():
            detail = await asyncio.to_thread(
                ctx.state.storage.get_block_detail,
                block_number,
                neighbor_window=neighbor_window,
                top_contracts_limit=top_contracts_limit,
            )
            if detail is None:
                return None
            # Resolve labels on top contracts.
            for c in detail["top_contracts"]:
                lbl = ctx.labels.get(c["to_addr"])
                c["label"] = lbl.name if lbl else None
                c["category"] = lbl.category if lbl else None
            # Reorg-near check: is there a reorg alert on this block or a
            # close neighbor? Dashboard can deep-link to /api/reorgs/{n}.
            reorg_block = await asyncio.to_thread(
                ctx.state.storage.find_reorg_near_block,
                block_number,
                window=neighbor_window,
            )
            detail["reorg_near"] = (
                {"block_number": reorg_block, "delta": reorg_block - block_number}
                if reorg_block is not None else None
            )
            return detail

        payload = await ctx.cached(
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

    @router.api_route("/api/contracts/{addr}", methods=["GET", "HEAD"])
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
        if ctx.state.storage is None:
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
                ctx.state.storage.get_contract_detail,
                addr_lower,
                from_ts_ms=from_ts_ms,
                to_ts_ms=to_ts_ms,
            )
            lbl = ctx.labels.get(addr_lower)
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

        payload = await ctx.cached(
            "contract_detail", _CONTRACT_DETAIL_TTL, cache_key, _load
        )
        return JSONResponse(payload, headers=_DETAIL_CACHE_HEADER)

    return router
