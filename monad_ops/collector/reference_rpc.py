"""Periodic probe of an external reference RPC.

During a stress test the operator-critical question is "is MY node
lagging, or is the whole network halted?". Comparing local block
height against a public reference RPC answers that distinction.

The sibling ``monad_monitor.py`` running off-box already uses the same
reference for alerting; this module is the *on-dashboard* read, so an
operator watching the page sees the lag immediately without cross-
referencing a Telegram channel.

Lives under ``collector/`` because it is another poll-and-summarize
background task, same shape as ``probes.py``.
"""

from __future__ import annotations

import time
from dataclasses import dataclass

import httpx


_DEFAULT_TIMEOUT_SEC = 4.0


@dataclass(frozen=True, slots=True)
class ReferenceSample:
    """One observation of the external reference chain tip."""
    block_number: int | None
    checked_ms: int           # wall-clock time of the observation
    error: str | None         # human-readable failure mode if the probe couldn't complete
    # Local block at the exact moment the reference was polled. Stored
    # alongside so the dashboard can compute a point-in-time delta
    # rather than comparing a live local tip to a 0–15s stale reference
    # (staleness bias up to ~37 blocks at 2.5 blk/s × 15s).
    local_at_sample: int | None = None


async def fetch_reference_block(
    rpc_url: str,
    *,
    timeout_sec: float = _DEFAULT_TIMEOUT_SEC,
    client: httpx.AsyncClient | None = None,
) -> ReferenceSample:
    """POST eth_blockNumber to ``rpc_url`` and return a ReferenceSample.

    Never raises — any failure mode (transport, HTTP, JSON, schema) is
    captured in ``error`` on the returned sample. Callers render "lag
    unreachable" as a soft state rather than treating the probe failure
    as a node incident (we have the local node's ingestion for that).
    """
    own_client = client is None
    if client is None:
        client = httpx.AsyncClient(timeout=timeout_sec)
    try:
        try:
            resp = await client.post(
                rpc_url,
                json={"jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": 1},
            )
        except httpx.HTTPError as e:
            return ReferenceSample(None, int(time.time() * 1000), f"transport: {e}")

        if resp.status_code != 200:
            return ReferenceSample(None, int(time.time() * 1000),
                                   f"HTTP {resp.status_code}")
        try:
            data = resp.json()
        except ValueError as e:
            return ReferenceSample(None, int(time.time() * 1000), f"JSON: {e}")

        result = data.get("result")
        if not isinstance(result, str):
            err = data.get("error") or result
            return ReferenceSample(None, int(time.time() * 1000),
                                   f"RPC: {err!r}")
        try:
            n = int(result, 16)
        except ValueError:
            return ReferenceSample(None, int(time.time() * 1000),
                                   f"non-hex result: {result!r}")
        return ReferenceSample(n, int(time.time() * 1000), None)
    finally:
        if own_client:
            await client.aclose()
