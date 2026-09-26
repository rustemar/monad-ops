"""Live node state and block endpoints: the state snapshot, the recent tail,
the downsampled chart series and the historical range query.

Moved out of ``build_app`` unchanged (queue item R1, slice 7).
"""

from __future__ import annotations

import asyncio

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from monad_ops.api.context import ApiContext

# In-memory snapshot, recomputed every block.
_STATE_TTL = 1.0
# In-memory recent-blocks tail.
_BLOCKS_TTL = 2.0


def build_router(ctx: ApiContext) -> APIRouter:
    """Block and state routes bound to ``ctx``."""
    router = APIRouter()

    # Cached first-block timestamp — only changes when new blocks arrive
    # earlier than what we've seen (extremely rare). Lazily populated.
    _data_start_ms: int | None = None

    def _get_data_start_ms() -> int | None:
        nonlocal _data_start_ms
        if _data_start_ms is not None:
            return _data_start_ms
        if ctx.state.storage is None:
            return None
        row = ctx.state.storage._conn.execute(
            "SELECT MIN(timestamp_ms) FROM blocks"
        ).fetchone()
        if row and row[0] is not None:
            _data_start_ms = int(row[0])
        return _data_start_ms

    @router.api_route("/api/state", methods=["GET", "HEAD"])
    async def api_state() -> JSONResponse:
        async def _load():
            snap = ctx.state.snapshot()
            return {
                "node_name": ctx.config.node.name,
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
                "cluster_reorgs_24h": snap.cluster_reorgs_24h,
                "last_reorg_number": snap.last_reorg_number,
                "last_reorg_old_id": snap.last_reorg_old_id,
                "last_reorg_new_id": snap.last_reorg_new_id,
                "last_reorg_ts_ms": snap.last_reorg_ts_ms,
                "reference_block": snap.reference_block,
                "reference_checked_ms": snap.reference_checked_ms,
                "reference_error": snap.reference_error,
                "reference_local_at_sample": snap.reference_local_at_sample,
                "maintenance_until": ctx.state.maintenance_until(),
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
        payload = await ctx.cached("state", _STATE_TTL, (), _load)
        return JSONResponse(payload)

    @router.api_route("/api/blocks", methods=["GET", "HEAD"])
    async def api_blocks(
        limit: int = Query(300, ge=1, le=2000),
    ) -> JSONResponse:
        async def _load():
            blocks = ctx.state.recent_blocks(limit=limit)
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
        payload = await ctx.cached("blocks", _BLOCKS_TTL, (limit,), _load)
        return JSONResponse(payload)

    @router.api_route("/api/blocks/sampled", methods=["GET", "HEAD"])
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
        if ctx.state.storage is None:
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
                ctx.state.storage.sampled_blocks,
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
        payload = await ctx.cached("sampled", ttl_sec, cache_key, _load)
        return JSONResponse(payload)

    @router.api_route("/api/blocks/range", methods=["GET", "HEAD"])
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
        if ctx.state.storage is None:
            return JSONResponse({"error": "persistence disabled"}, status_code=503)
        blocks = ctx.state.storage.load_blocks_range(
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

    return router
