"""Reorg endpoints: the event list, the per-event trace, the journal artifact.

Moved out of ``build_app`` unchanged (queue item R1). The public/full split and
the two trace TTLs live here because nothing else reads them.
"""

from __future__ import annotations

import asyncio

from fastapi import APIRouter, Query
from fastapi.responses import FileResponse, JSONResponse

from monad_ops.api.context import ApiContext
from monad_ops.reorg_capture import find_artifact

# Changes only when a new reorg fires, which is rare.
_REORGS_LIST_TTL = 30.0
# A historical reorg trace is immutable.
_REORG_TRACE_TTL = 300.0
# Applied instead when a trace is missing post-event blocks: the tailer has not
# caught up yet, so the answer is expected to change shortly.
_REORG_TRACE_PARTIAL_TTL = 5.0

# Fields kept in the ``public`` variant of reorg traces. Anything
# omitted is local-node performance telemetry (tx_exec_us,
# total_us, active_chunks, …) that leaks hardware characteristics
# and is not part of the chain itself. The public set covers
# everything derivable from the chain record alone — enough to
# support operator-to-operator discussion of a reorg without
# revealing how this specific node is provisioned.
_REORG_TRACE_PUBLIC_FIELDS = (
    "block_number", "block_id", "timestamp_ms",
    "tx_count", "retried", "retry_pct", "gas_used",
)


def _sanitize_block_for_public(row: dict) -> dict:
    return {k: row[k] for k in _REORG_TRACE_PUBLIC_FIELDS if k in row}


def build_router(ctx: ApiContext) -> APIRouter:
    """Reorg routes bound to ``ctx``.

    ``/api/reorgs/{block_number}/journal`` is declared before
    ``/api/reorgs/{block_number}`` on purpose: FastAPI matches in registration
    order and the literal suffix has to win.
    """
    router = APIRouter()

    @router.api_route("/api/reorgs", methods=["GET", "HEAD"])
    async def api_reorgs(
        limit: int = Query(200, ge=1, le=5000),
    ) -> JSONResponse:
        """List of observed reorg events, newest-first.

        Each row reconstructs both block_ids (``before`` from the
        persisted blocks row, ``after`` from the alert key) and a
        compact block-metrics summary. ``has_journal`` is true when a
        sanitized journal-trace artifact has been captured for the
        event (only reorgs that fired after the capture feature shipped
        — historical reorgs return false). For the full per-block
        neighbor trace, call ``/api/reorgs/{block_number}``; for the
        gzipped journal artifact, call
        ``/api/reorgs/{block_number}/journal``.
        """
        if ctx.state.storage is None:
            return JSONResponse({"error": "persistence disabled"}, status_code=503)
        async def _load():
            rows = await asyncio.to_thread(ctx.state.storage.list_reorgs, limit=limit)
            if ctx.journal_capture_dir is not None:
                for row in rows:
                    bn = row.get("block_number")
                    row["has_journal"] = (
                        bn is not None
                        and find_artifact(ctx.journal_capture_dir, int(bn)) is not None
                    )
            else:
                for row in rows:
                    row["has_journal"] = False
            return {"count": len(rows), "reorgs": rows}
        payload = await ctx.cached("reorgs", _REORGS_LIST_TTL, (limit,), _load)
        return JSONResponse(payload)

    @router.api_route("/api/reorgs/{block_number}/journal", methods=["GET", "HEAD"])
    async def api_reorg_journal(block_number: int) -> FileResponse:
        """Sanitized journal trace around a reorg event.

        Returns the gzipped JSONL artifact captured at fire time,
        covering the ``monad-bft`` consensus stream from a few seconds
        before the reorged block's wall-clock timestamp through a few
        seconds after. Peer IPs from the wire-auth keepalive stream
        and the local OTLP loopback are scrubbed at write time; the
        rest of the consensus trace (validator pubkeys, block ids,
        rounds, votes, base fees) is public chain data and stays
        intact.

        404 when no artifact exists — either the reorg pre-dates the
        capture feature, or the journal had already rotated past the
        event's window when the deferred snapshot ran.
        """
        if ctx.journal_capture_dir is None:
            return JSONResponse(
                {"error": "journal capture disabled"}, status_code=503
            )
        path = find_artifact(ctx.journal_capture_dir, block_number)
        if path is None:
            return JSONResponse(
                {"error": "journal artifact not found"}, status_code=404
            )
        return FileResponse(
            path,
            media_type="application/gzip",
            filename=f"reorg-{block_number}.jsonl.gz",
        )

    @router.api_route("/api/reorgs/{block_number}", methods=["GET", "HEAD"])
    async def api_reorg_trace(
        block_number: int,
        window: int = Query(30, ge=0, le=500),
        level: str = Query("public", pattern="^(public|full)$"),
    ) -> JSONResponse:
        """Reorg event + neighboring blocks for forensic review.

        ``window`` controls how many blocks before/after the reorged
        block to include (0–500, default 30). ``level=public`` (the
        default) strips local timing fields so the trace is safe to
        share operator-to-operator; ``level=full`` returns every
        ExecBlock field for deeper internal analysis.

        404 if no reorg alert exists for this block_number.
        """
        if ctx.state.storage is None:
            return JSONResponse({"error": "persistence disabled"}, status_code=503)
        async def _load():
            trace = await asyncio.to_thread(
                ctx.state.storage.get_reorg_trace, block_number, window=window
            )
            if trace is None:
                return None
            # Sanitization happens silently — no "level" marker in the
            # response body. A marker would just invite the reader to
            # wonder what the "other" level withholds, and it doesn't
            # actually protect the stripped fields (JSON is trivially
            # editable). The URL parameter remains available as an
            # opt-in for internal callers who need the local timing.
            if level == "public":
                trace = {
                    **trace,
                    "blocks": [_sanitize_block_for_public(b) for b in trace["blocks"]],
                }
                # Proposer identity is public on-chain, but a shareable
                # trace that names the validator behind a reorg reads as
                # an accusation. Kept to level=full, which is local
                # analysis. The per-block copies are already gone —
                # _REORG_TRACE_PUBLIC_FIELDS is an allowlist — so this
                # only has to drop the top-level one.
                trace.pop("proposer", None)
            return trace

        def _ttl(trace) -> float:
            # An unknown block_number caches its 404 long — those don't
            # spontaneously become reorgs.
            if trace is None:
                return _REORG_TRACE_TTL
            # Truncated post-window means the tailer is still catching
            # up to the reorged block's neighbours. Cache only briefly
            # so a viewer who hits the endpoint at fire-time + 0.1s
            # doesn't get stuck with a partial trace for 5 minutes.
            blocks = trace.get("blocks") or []
            highest_seen = blocks[-1]["block_number"] if blocks else None
            post_complete = (
                highest_seen is not None and highest_seen >= block_number + window
            )
            return _REORG_TRACE_TTL if post_complete else _REORG_TRACE_PARTIAL_TTL

        trace = await ctx.cached(
            "reorg_trace", _REORG_TRACE_TTL, (block_number, window, level), _load,
            ttl_for_value=_ttl,
        )
        if trace is None:
            return JSONResponse(
                {"error": "reorg alert not found"},
                status_code=404,
            )
        return JSONResponse(trace)

    return router
