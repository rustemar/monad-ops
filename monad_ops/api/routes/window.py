"""Time-window summary: aggregates, contract ranking and optional block rows.

Moved out of ``build_app`` unchanged (queue item R1). Uses the app-owned
read-through cache and labels through ``ApiContext``.
"""

from __future__ import annotations

import asyncio

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from monad_ops.api.context import ApiContext


def build_router(ctx: ApiContext) -> APIRouter:
    """Window-summary route bound to ``ctx``."""
    router = APIRouter()
    _WINDOW_SUMMARY_TTL = 15.0  # heavy SQL aggregate over arbitrary window

    @router.api_route("/api/window_summary", methods=["GET", "HEAD"])
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

        if ctx.state.storage is None:
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
                    ctx.state.storage.load_blocks_range,
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
                ctx.state.storage.block_metrics_aggregate,
                from_ts_ms=from_ts_ms,
                to_ts_ms=to_ts_ms,
            )
            # Consensus-side aggregate for the same window. Cheap —
            # SUM over minute buckets, O(window-minutes) — and surfaces
            # Foundation's headline KPI for stress-replay queries:
            # /api/window_summary?from=...epoch_532...&to=...epoch_534...
            # returns the chain-wide validator-timeout % directly.
            consensus = await asyncio.to_thread(
                ctx.state.storage.load_bft_window,
                from_ts_ms,
                to_ts_ms,
            )
            base_fee = await asyncio.to_thread(
                ctx.state.storage.load_base_fee_window,
                from_ts_ms,
                to_ts_ms,
            )
            if span_ms <= _ROLLUP_SPAN_THRESHOLD_MS:
                contracts = await asyncio.to_thread(
                    ctx.state.storage.top_retried_contracts,
                    since_ts_ms=from_ts_ms,
                    until_ts_ms=to_ts_ms,
                    min_appearances=min_appearances,
                    limit=top_contracts_limit,
                )
            else:
                contracts = await asyncio.to_thread(
                    ctx.state.storage.top_retried_contracts_rollup,
                    since_ts_ms=from_ts_ms,
                    until_ts_ms=to_ts_ms,
                    min_appearances=min_appearances,
                    limit=top_contracts_limit,
                )

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

        payload = await ctx.cached(
            "window_summary", _WINDOW_SUMMARY_TTL, cache_key, _load
        )
        return JSONResponse(payload)

    return router
