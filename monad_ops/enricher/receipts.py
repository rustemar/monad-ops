"""Fetch block receipts via JSON-RPC and map them to ``TxEnrichment`` rows.

Uses ``eth_getBlockReceipts`` — one RPC per block returning every tx
receipt at once. On nodes that don't expose it, this module will raise
``EnrichmentError`` and the caller can fall back to per-tx lookups in
a future iteration (not needed right now: the local testnet RPC
supports it).
"""

from __future__ import annotations

from typing import Any

import httpx
import structlog

from monad_ops.storage import ContractBlockAgg, Storage, TxEnrichment


log = structlog.stdlib.get_logger()


class EnrichmentError(Exception):
    """Raised when an RPC call fails or returns unexpected data."""


class ReceiptsClient:
    """Thin async JSON-RPC client scoped to receipt queries."""

    def __init__(
        self,
        rpc_url: str,
        timeout_sec: float = 5.0,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._rpc_url = rpc_url
        # If the caller injects a client, we don't own it (don't close it).
        self._owned_client = client is None
        self._client = client or httpx.AsyncClient(timeout=timeout_sec)

    async def close(self) -> None:
        if self._owned_client:
            await self._client.aclose()

    async def get_block_receipts(self, block_number: int) -> list[dict[str, Any]]:
        hex_n = hex(int(block_number))
        payload = {
            "jsonrpc": "2.0",
            "method": "eth_getBlockReceipts",
            "params": [hex_n],
            "id": 1,
        }
        try:
            resp = await self._client.post(self._rpc_url, json=payload)
        except httpx.HTTPError as e:
            raise EnrichmentError(f"RPC transport error: {e}") from e

        if resp.status_code != 200:
            raise EnrichmentError(f"RPC HTTP {resp.status_code}: {resp.text[:200]}")

        data = resp.json()
        if "error" in data:
            raise EnrichmentError(f"RPC error: {data['error']}")
        result = data.get("result")
        if result is None:
            raise EnrichmentError(f"block {block_number} receipts unavailable (null result)")
        if not isinstance(result, list):
            raise EnrichmentError(f"unexpected result shape: {type(result).__name__}")
        return result


async def fetch_block_receipts(
    client: ReceiptsClient, block_number: int
) -> list[dict[str, Any]]:
    """Convenience wrapper; separate so tests can monkey-patch easily."""
    return await client.get_block_receipts(block_number)


def receipts_to_enrichment_rows(
    block_number: int, receipts: list[dict[str, Any]]
) -> list[TxEnrichment]:
    """Map raw receipt dicts to ``TxEnrichment`` rows.

    Skips malformed entries rather than raising; a single broken receipt
    shouldn't poison an otherwise-valid block.
    """
    rows: list[TxEnrichment] = []
    for r in receipts:
        try:
            rows.append(TxEnrichment(
                block_number=block_number,
                tx_index=int(r["transactionIndex"], 16),
                tx_hash=r["transactionHash"],
                from_addr=(r.get("from") or "").lower() or "",
                to_addr=_norm_addr(r.get("to")),
                contract_created=_norm_addr(r.get("contractAddress")),
                status=int(r.get("status", "0x0"), 16),
                gas_used=int(r.get("gasUsed", "0x0"), 16),
            ))
        except (KeyError, ValueError, TypeError) as e:
            log.warning("enricher.bad_receipt", block=block_number, err=str(e))
            continue
    return rows


def receipts_to_contract_block_rows(
    block_number: int, receipts: list[dict[str, Any]]
) -> list[ContractBlockAgg]:
    """Aggregate receipts into one row per (block, to_addr).

    This is the hot path now written to disk — collapses N tx-rows for
    the same contract inside a block into a single ``ContractBlockAgg``.
    Contract-creation txs (``to`` missing/null) are dropped here because
    no dashboard query looks at them.
    """
    buckets: dict[str, tuple[int, int]] = {}
    for r in receipts:
        to = _norm_addr(r.get("to"))
        if to is None:
            continue
        try:
            gas = int(r.get("gasUsed", "0x0"), 16)
        except (ValueError, TypeError):
            log.warning("enricher.bad_receipt_gas", block=block_number, to=to)
            continue
        cur_tx, cur_gas = buckets.get(to, (0, 0))
        buckets[to] = (cur_tx + 1, cur_gas + gas)
    return [
        ContractBlockAgg(
            block_number=block_number,
            to_addr=addr,
            tx_count=tx,
            total_gas=gas,
        )
        for addr, (tx, gas) in buckets.items()
    ]


async def enrich_block(
    client: ReceiptsClient, storage: Storage, block_number: int
) -> int:
    """Fetch receipts for ``block_number`` and persist aggregate rows.

    Writes one row per (block, to_addr) destination to ``tx_contract_block``.
    The per-tx raw table (``tx_enrichment``) is intentionally NOT written
    to here — it's preserved in the schema for a future short-retention
    hot-tier but is not needed by any current dashboard query. See
    2026-04-20 outage notes for background.

    Returns the number of aggregate rows persisted. Zero-tx blocks or
    blocks whose receipts all create contracts (no to_addr) write zero.
    """
    import asyncio as _asyncio
    receipts = await fetch_block_receipts(client, block_number)
    agg_rows = receipts_to_contract_block_rows(block_number, receipts)
    # Sqlite write off the event loop — at 2.5 blk/s with contention
    # from the dashboard's JOIN queries, inline writes were occasionally
    # stalling /api/state response by hundreds of ms.
    await _asyncio.to_thread(storage.write_tx_contract_block, agg_rows)
    return len(agg_rows)


def _norm_addr(a: str | None) -> str | None:
    if a is None:
        return None
    a = a.strip().lower()
    return a or None
