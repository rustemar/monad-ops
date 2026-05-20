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
import json
import urllib.error
import urllib.request
from dataclasses import dataclass


# Staking precompile address
_STAKING_PRECOMPILE = "0x0000000000000000000000000000000000001000"
_SEL_GET_EPOCH = "0x757991a8"
_SEL_GET_VALIDATOR = "0x2b6d639a"
_SEL_CONSENSUS_SET = "0xfb29b729"  # current epoch leaders
_SEL_EXECUTION_SET = "0x7cb074df"  # all eligible (>= ACTIVE_VALIDATOR_STAKE)

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
      * ``"ok"`` — full snapshot returned
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
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310
        data = json.loads(resp.read())
    if "result" in data:
        return data["result"]
    raise RuntimeError(f"rpc reply lacked 'result': {data!r}")


async def _staking_call(rpc_url: str, selector_and_args: str, timeout_sec: float) -> str | None:
    """eth_call against the staking precompile, returns hex result body (no ``0x``) or None."""
    def _do() -> str | None:
        try:
            result = _rpc_call_sync(
                rpc_url,
                "eth_call",
                [{"to": _STAKING_PRECOMPILE, "data": selector_and_args}, "latest"],
                timeout_sec,
            )
        except (urllib.error.URLError, TimeoutError, OSError, RuntimeError, ValueError):
            return None
        if not isinstance(result, str) or not result.startswith("0x"):
            return None
        return result[2:]
    return await asyncio.to_thread(_do)


async def _fetch_set_count(rpc_url: str, selector: str, timeout_sec: float) -> tuple[int | None, int | None]:
    """Page through ``get*ValidatorSet`` until ``is_done``.

    Returns ``(total_count, last_id)`` where ``last_id`` is the lowest-
    stake entry in a stake-sorted-descending set (cheap floor probe).
    Both are None on probe failure.
    """
    total = 0
    last_id: int | None = None
    start = 0
    # Hard cap at 10 pages (≈1000 validators) to avoid runaway on a
    # misbehaving RPC.
    for _ in range(10):
        body = await _staking_call(rpc_url, selector + f"{start:064x}", timeout_sec)
        if not body or len(body) < 256:
            return None, None
        is_done = int(body[0:64], 16)
        next_index = int(body[64:128], 16)
        length = int(body[192:256], 16)
        if length:
            last_word_off = 256 + (length - 1) * 64
            last_id = int(body[last_word_off:last_word_off + 64], 16)
        total += length
        if is_done:
            return total, last_id
        start = next_index
    return total, last_id


async def _fetch_consensus_stake(rpc_url: str, vid: int, timeout_sec: float) -> int | None:
    """Read ``consensusStake`` (wei) from ``getValidator(vid)``. None on probe failure."""
    body = await _staking_call(rpc_url, _SEL_GET_VALIDATOR + f"{vid:064x}", timeout_sec)
    if not body or len(body) < 448:
        return None
    # Return layout per the staking ABI: authAddress, flags, stake,
    # accRewardPerToken, commission, unclaimedRewards, consensusStake, …
    # consensusStake is the 7th word → offset 6*64 = 384.
    return int(body[384:448], 16)


async def fetch_validator_set(
    *,
    rpc_url: str,
    timeout_sec: float = 10.0,
) -> ValidatorSetSnapshot:
    """One-shot snapshot fetch. Never raises — failures land in ``status="unknown"``."""
    epoch_body = await _staking_call(rpc_url, _SEL_GET_EPOCH, timeout_sec)
    if not epoch_body or len(epoch_body) < 128:
        return ValidatorSetSnapshot(
            epoch=None, in_epoch_delay=False,
            consensus_count=None, execution_count=None, bench_count=None,
            lowest_active_stake_wei=None,
            status="unknown", error="getEpoch probe failed",
        )

    try:
        epoch = int(epoch_body[0:64], 16)
        in_epoch_delay = bool(int(epoch_body[64:128], 16))
    except ValueError:
        return ValidatorSetSnapshot(
            epoch=None, in_epoch_delay=False,
            consensus_count=None, execution_count=None, bench_count=None,
            lowest_active_stake_wei=None,
            status="unknown", error="getEpoch decode failed",
        )

    cons_count, cons_last_id = await _fetch_set_count(rpc_url, _SEL_CONSENSUS_SET, timeout_sec)
    exec_count, _ = await _fetch_set_count(rpc_url, _SEL_EXECUTION_SET, timeout_sec)
    if cons_count is None or exec_count is None:
        return ValidatorSetSnapshot(
            epoch=epoch, in_epoch_delay=in_epoch_delay,
            consensus_count=None, execution_count=None, bench_count=None,
            lowest_active_stake_wei=None,
            status="unknown", error="validator-set paging failed",
        )

    lowest_wei: int | None = None
    if cons_last_id is not None:
        lowest_wei = await _fetch_consensus_stake(rpc_url, cons_last_id, timeout_sec)

    return ValidatorSetSnapshot(
        epoch=epoch,
        in_epoch_delay=in_epoch_delay,
        consensus_count=cons_count,
        execution_count=exec_count,
        bench_count=max(0, exec_count - cons_count),
        lowest_active_stake_wei=lowest_wei,
        status="ok",
        error=None,
    )
