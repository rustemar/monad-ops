"""Pure data fetcher for the active validator set.

Queries the Monad staking precompile (``0x...1000``) via the configured
node's RPC, returns a snapshot dataclass — no alert emission, no state,
no I/O beyond the eth_call round-trips. The collector mirrors the
existing operator-side validator block already rendered in
``monitoring/monad_monitor.py`` Telegram heartbeats; surfacing the same
data on the public dashboard makes the tile VDP-aware without adding a
second oracle.

Protocol constants and selectors sourced from docs.monad.xyz/reference/
staking/api; same values on mainnet and testnet. Selectors hard-coded
because they are public ABI surface and unlikely to change without a
hard fork.
"""

from __future__ import annotations

import asyncio
import http.client
import json
import re
import urllib.request
from dataclasses import dataclass

# Staking precompile address
_STAKING_PRECOMPILE = "0x0000000000000000000000000000000000001000"
_SEL_GET_EPOCH = "0x757991a8"
_SEL_GET_VALIDATOR = "0x2b6d639a"
_SEL_CONSENSUS_SET = "0xfb29b729"
_SEL_SNAPSHOT_SET = "0xde66a368"
_SEL_EXECUTION_SET = "0x7cb074df"
_MAX_SET_PAGES = 10

# Protocol constants from the staking API reference. Static — change
# only requires touching one file when the protocol bumps them.
ACTIVE_VALSET_SIZE = 200
ACTIVE_VALIDATOR_STAKE_MON = 10_000_000
MIN_AUTH_ADDRESS_STAKE_MON = 100_000
WEI_PER_MON = 10**18


@dataclass(frozen=True, slots=True)
class ValidatorSetSnapshot:
    """Active validator set at a point in time.

    ``status`` is one of:
      * ``"ok"`` — both membership lists complete; the stake lookup is optional
      * ``"unknown"`` — probe failed (RPC down, malformed reply, etc.);
        ``error`` carries the reason
    """
    epoch: int | None
    in_epoch_delay: bool
    consensus_count: int | None
    execution_count: int | None
    bench_count: int | None
    lowest_active_stake_wei: int | None
    status: str
    error: str | None = None

    @classmethod
    def unknown(
        cls, error: str, *, epoch: int | None = None, in_epoch_delay: bool = False,
    ) -> ValidatorSetSnapshot:
        return cls(epoch=epoch, in_epoch_delay=in_epoch_delay, consensus_count=None,
                   execution_count=None, bench_count=None, lowest_active_stake_wei=None,
                   status="unknown", error=error)


def _rpc_call_sync(url: str, method: str, params: list, timeout: float) -> object:
    payload = json.dumps({
        "jsonrpc": "2.0",
        "method": method,
        "params": params,
        "id": 1,
    }).encode()
    req = urllib.request.Request(
        url,
        data=payload,
        headers={"Content-Type": "application/json", "User-Agent": "monad-ops"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310
        data = json.loads(resp.read())
    if (not isinstance(data, dict) or data.get("jsonrpc") != "2.0"
            or type(data.get("id")) is not int or data["id"] != 1 or "error" in data):
        return None
    return data.get("result")


async def _rpc(url: str, method: str, params: list, timeout: float) -> object:
    try:
        return await asyncio.to_thread(_rpc_call_sync, url, method, params, timeout)
    except (OSError, ValueError, http.client.HTTPException):
        return None


async def _staking_words(
    url: str, calldata: str, block: str, timeout: float,
) -> list[int]:
    params = [{"to": _STAKING_PRECOMPILE, "data": calldata}, block]
    result = await _rpc(url, "eth_call", params, timeout)
    if (not isinstance(result, str) or (len(result) - 2) % 64
            or not re.fullmatch(r"0x[0-9a-fA-F]+", result)):
        return []
    return [int(result[i:i + 64], 16) for i in range(2, len(result), 64)]


async def _fetch_set(
    url: str, selector: str, block: str, timeout: float,
) -> list[int] | None:
    # Keep the protocol order for the cutoff lookup; a repeated ID invalidates the snapshot.
    ids: list[int] = []
    seen: set[int] = set()
    for _ in range(_MAX_SET_PAGES):
        start = len(ids)
        words = await _staking_words(url, selector + f"{start:064x}", block, timeout)
        if len(words) < 4:
            return None
        done, cursor, offset, count = words[:4]
        page = words[4:]
        if (done not in (0, 1) or offset != 96 or count != len(page)
                or cursor >= 2**32 or cursor != start + count or (not done and not page)):
            return None
        for vid in page:
            if vid >= 2**64 or vid in seen:
                return None
            seen.add(vid)
        ids.extend(page)
        if done:
            return ids
    return None


def _active_stake(words: list[int], *, in_epoch_delay: bool) -> int | None:
    # getValidator has twelve head words followed by two ABI-encoded byte strings.
    # Check both tails before trusting the stake field in a possibly truncated reply.
    tail = 12
    for offset_index in (10, 11):
        if tail >= len(words) or words[offset_index] != tail * 32:
            return None
        tail += 1 + (words[tail] + 31) // 32
    if tail != len(words):
        return None
    return words[8] if in_epoch_delay else words[6]  # snapshotStake or consensusStake


async def fetch_validator_set(
    *,
    rpc_url: str,
    timeout_sec: float = 10.0,
) -> ValidatorSetSnapshot:
    """Read both sets and the cutoff at the same height, including during epoch handover."""
    block = await _rpc(rpc_url, "eth_blockNumber", [], timeout_sec)
    if not isinstance(block, str) or not re.fullmatch(r"0x(?:0|[1-9a-fA-F][0-9a-fA-F]*)", block):
        return ValidatorSetSnapshot.unknown("block number probe failed")
    words = await _staking_words(rpc_url, _SEL_GET_EPOCH, block, timeout_sec)
    if len(words) != 2 or words[0] >= 2**64 or words[1] not in (0, 1):
        return ValidatorSetSnapshot.unknown("getEpoch probe failed")
    epoch, delay = words[0], bool(words[1])
    # At the boundary the contract prepares the next consensus set. Until the
    # epoch switches, this_epoch_valset() and this_epoch_view() use the snapshot.
    selector = _SEL_SNAPSHOT_SET if delay else _SEL_CONSENSUS_SET
    active = await _fetch_set(rpc_url, selector, block, timeout_sec)
    execution = await _fetch_set(rpc_url, _SEL_EXECUTION_SET, block, timeout_sec)
    if active is None or execution is None:
        return ValidatorSetSnapshot.unknown("validator-set paging failed",
                                            epoch=epoch, in_epoch_delay=delay)

    lowest_wei = None
    if active:
        # The contract stores both epoch sets in descending stake order.
        words = await _staking_words(
            rpc_url, _SEL_GET_VALIDATOR + f"{active[-1]:064x}", block, timeout_sec,
        )
        lowest_wei = _active_stake(words, in_epoch_delay=delay)
    return ValidatorSetSnapshot(
        epoch=epoch,
        in_epoch_delay=delay,
        consensus_count=len(active),
        execution_count=len(execution),
        bench_count=len(set(execution).difference(active)),
        lowest_active_stake_wei=lowest_wei,
        status="ok",
    )
