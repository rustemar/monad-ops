"""Unit tests for collector/validator_set.py — staking precompile decoding.

The eth_call wire shape is mocked at the ``_staking_call`` boundary;
all tests exercise the paging logic and ABI offset parsing the
collector relies on, plus the soft-fail paths.
"""

from __future__ import annotations

import pytest

from monad_ops.collector import validator_set as vs


def _pack_set_page(*, is_done: int, next_index: int, length: int, ids: list[int]) -> str:
    """Build the hex body returned by ``getConsensusValidatorSet`` /
    ``getExecutionValidatorSet`` paging shape.

    Layout: is_done (1 word) || next_index (1 word) || pad (1 word) ||
    length (1 word) || id_0 (1 word) || id_1 (1 word) || …
    """
    assert len(ids) == length
    body = f"{is_done:064x}{next_index:064x}{0:064x}{length:064x}"
    for vid in ids:
        body += f"{vid:064x}"
    return body


def _pack_get_validator(stake_wei: int) -> str:
    """Build the hex body returned by ``getValidator`` with the
    ``consensusStake`` slot at byte offset 384 (the 7th word)."""
    # Six prefix words of arbitrary zeroes + consensusStake.
    return "00" * 192 + f"{stake_wei:064x}"


def _pack_epoch(*, epoch: int, in_delay: bool) -> str:
    return f"{epoch:064x}{(1 if in_delay else 0):064x}"


@pytest.fixture
def patch_staking_call(monkeypatch):
    """Patch ``_staking_call`` with a dict-driven stub.

    The stub keys off the *selector* (first 8 hex chars of the call
    data) and returns the configured body. Set
    ``responses["return"] = None`` to simulate a probe failure.
    """
    responses: dict[str, list[str | None]] = {}

    async def _stub(_rpc_url: str, call_data: str, _timeout: float) -> str | None:
        # call_data is "0x<selector><args>" — the first 10 chars are
        # the selector (incl. the 0x prefix) which keys the response map.
        selector = call_data[:10]
        queue = responses.get(selector)
        if not queue:
            return None
        head = queue[0]
        # Keep the last response sticky so paging tests don't need to
        # specify ``getValidator`` once per id; only set-paging tests
        # explicitly enqueue multiple bodies and pop them in order.
        if len(queue) > 1:
            queue.pop(0)
        return head

    monkeypatch.setattr(vs, "_staking_call", _stub)
    return responses


@pytest.mark.asyncio
async def test_happy_path_single_page(patch_staking_call):
    """Full 200-id consensus set returned in one page; floor stake
    decoded from the last id."""
    patch_staking_call["0x757991a8"] = [_pack_epoch(epoch=545, in_delay=False)]
    cons_ids = list(range(200, 0, -1))  # stake-sorted descending
    patch_staking_call["0xfb29b729"] = [
        _pack_set_page(is_done=1, next_index=200, length=200, ids=cons_ids),
    ]
    patch_staking_call["0x7cb074df"] = [
        _pack_set_page(is_done=1, next_index=208, length=208, ids=list(range(208))),
    ]
    # getValidator for the cons_last_id (= cons_ids[-1] = 1) returns
    # 11M MON in wei.
    patch_staking_call["0x2b6d639a"] = [_pack_get_validator(11_000_000 * 10**18)]

    snap = await vs.fetch_validator_set(rpc_url="http://test")
    assert snap.status == "ok"
    assert snap.epoch == 545
    assert snap.in_epoch_delay is False
    assert snap.consensus_count == 200
    assert snap.execution_count == 208
    assert snap.bench_count == 8
    assert snap.lowest_active_stake_wei == 11_000_000 * 10**18
    assert snap.error is None


@pytest.mark.asyncio
async def test_unknown_status_on_epoch_failure(patch_staking_call):
    """If getEpoch returns nothing the snapshot is unknown — neither
    the set pages nor the stake probe is attempted, so partial data
    can't leak into the dashboard."""
    snap = await vs.fetch_validator_set(rpc_url="http://test")
    assert snap.status == "unknown"
    assert snap.epoch is None
    assert snap.consensus_count is None
    assert snap.lowest_active_stake_wei is None


@pytest.mark.asyncio
async def test_unknown_status_on_set_paging_failure(patch_staking_call):
    """Epoch known but paging fails → snapshot carries the epoch but
    counts remain None and status is unknown."""
    patch_staking_call["0x757991a8"] = [_pack_epoch(epoch=545, in_delay=False)]
    # consensus paging returns nothing
    snap = await vs.fetch_validator_set(rpc_url="http://test")
    assert snap.status == "unknown"
    assert snap.epoch == 545
    assert snap.consensus_count is None


@pytest.mark.asyncio
async def test_multi_page_paging_returns_total_and_last_id(patch_staking_call):
    """Two-page response — the collector must accumulate ``length`` and
    keep the last id of the final page as the floor probe target."""
    patch_staking_call["0x757991a8"] = [_pack_epoch(epoch=600, in_delay=True)]
    patch_staking_call["0xfb29b729"] = [
        _pack_set_page(is_done=0, next_index=128, length=128, ids=list(range(200, 72, -1))),
        _pack_set_page(is_done=1, next_index=200, length=72, ids=list(range(72, 0, -1))),
    ]
    patch_staking_call["0x7cb074df"] = [
        _pack_set_page(is_done=1, next_index=200, length=200, ids=list(range(200))),
    ]
    patch_staking_call["0x2b6d639a"] = [_pack_get_validator(10_000_000 * 10**18)]

    snap = await vs.fetch_validator_set(rpc_url="http://test")
    assert snap.status == "ok"
    assert snap.consensus_count == 200
    assert snap.in_epoch_delay is True
    # bench = exec - cons = 0 here; OK for this fixture
    assert snap.bench_count == 0
    assert snap.lowest_active_stake_wei == 10_000_000 * 10**18
