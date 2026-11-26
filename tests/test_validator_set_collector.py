"""Exercise the staking ABI and snapshot consistency at the JSON-RPC boundary."""

from __future__ import annotations

import pytest

from monad_ops.collector import validator_set as vs

EPOCH = "0x757991a8"
CONSENSUS = "0xfb29b729"
SNAPSHOT = "0xde66a368"
EXECUTION = "0x7cb074df"
VALIDATOR = "0x2b6d639a"
BLOCK = "0x5a7f"
URL = "http://test"


def _words(*values: int) -> str:
    return "".join(f"{value:064x}" for value in values)


def _page(done: bool, next_index: int, *ids: int) -> str:
    # bool, uint32, offset to uint64[], array length, then its elements.
    return "0x" + _words(done, next_index, 96, len(ids), *ids)


def _validator(stake: int, snapshot_stake: int = 0) -> str:
    # Full getValidator tuple, including 33-byte SECP and 48-byte BLS public keys.
    head = _words(1, 0, stake, 0, 0, 0, stake, 0, snapshot_stake, 0, 384, 480)
    return "0x" + head + _words(33, 1, 0, 48, 1, 0)


@pytest.fixture
def rpc(monkeypatch):
    # Exact calldata keys exercise each request's cursor and the validator ID used for stake.
    responses = {"eth_blockNumber": BLOCK, EPOCH: "0x" + _words(545, 0)}
    calls = []

    def respond(url, method, params, timeout):
        assert url == URL and timeout == 10.0
        calls.append((method, params))
        if method == "eth_blockNumber":
            assert params == []
            return responses[method]
        assert method == "eth_call"
        request, _ = params
        assert request["to"] == "0x0000000000000000000000000000000000001000"
        return responses.get(request["data"])

    monkeypatch.setattr(vs, "_rpc_call_sync", respond)
    return responses, calls


@pytest.mark.asyncio
async def test_snapshot_pins_every_page_and_stake_read_to_one_block(rpc):
    responses, calls = rpc
    stake = 11_000_000 * 10**18
    responses.update({
        EPOCH: "0x" + _words(600, 0),
        CONSENSUS + _words(0): _page(False, 2, 9, 4),
        CONSENSUS + _words(2): _page(True, 3, 8),
        EXECUTION + _words(0): _page(False, 1, 8),
        EXECUTION + _words(1): _page(True, 4, 4, 9, 5),
        VALIDATOR + _words(8): _validator(stake),
    })
    snap = await vs.fetch_validator_set(rpc_url=URL)
    assert snap.status == "ok" and snap.error is None
    assert snap.epoch == 600 and snap.in_epoch_delay is False
    assert (snap.consensus_count, snap.execution_count, snap.bench_count) == (3, 4, 1)
    assert snap.lowest_active_stake_wei == stake
    assert [method for method, _ in calls].count("eth_blockNumber") == 1
    assert calls[0] == ("eth_blockNumber", [])
    assert {params[1] for method, params in calls if method == "eth_call"} == {BLOCK}


@pytest.mark.asyncio
async def test_delay_uses_snapshot_membership_and_snapshot_stake(rpc):
    responses, calls = rpc
    responses.update({
        EPOCH: "0x" + _words(600, 1),
        SNAPSHOT + _words(0): _page(False, 1, 4),
        SNAPSHOT + _words(1): _page(True, 2, 3),
        EXECUTION + _words(0): _page(True, 2, 4, 5),
        VALIDATOR + _words(3): _validator(99 * 10**18, snapshot_stake=17 * 10**18),
    })
    snap = await vs.fetch_validator_set(rpc_url=URL)
    assert snap.status == "ok" and snap.epoch == 600 and snap.in_epoch_delay is True
    assert (snap.consensus_count, snap.execution_count, snap.bench_count) == (2, 2, 1)
    assert snap.lowest_active_stake_wei == 17 * 10**18
    eth_calls = [params for method, params in calls if method == "eth_call"]
    assert all(not request["data"].startswith(CONSENSUS) for request, _ in eth_calls)
    assert {block for _, block in eth_calls} == {BLOCK}


@pytest.mark.asyncio
async def test_broken_snapshot_during_delay_cannot_fall_back_to_consensus(rpc):
    responses, _ = rpc
    responses.update({
        EPOCH: "0x" + _words(600, 1),
        SNAPSHOT + _words(0): _page(True, 1, 3)[:-64],
        CONSENSUS + _words(0): _page(True, 1, 5),
        EXECUTION + _words(0): _page(True, 1, 5),
    })
    snap = await vs.fetch_validator_set(rpc_url=URL)
    assert snap.status == "unknown" and snap.epoch == 600 and snap.in_epoch_delay is True
    assert snap.consensus_count is None and snap.lowest_active_stake_wei is None


@pytest.mark.asyncio
async def test_equal_sized_sets_with_different_ids_have_a_bench(rpc):
    responses, _ = rpc
    responses[CONSENSUS + _words(0)] = _page(True, 3, 10, 15, 12)
    responses[EXECUTION + _words(0)] = _page(True, 3, 15, 12, 14)
    snap = await vs.fetch_validator_set(rpc_url=URL)
    assert (snap.consensus_count, snap.execution_count, snap.bench_count) == (3, 3, 1)


@pytest.mark.asyncio
async def test_empty_sets_are_known_without_a_floor_request(rpc):
    responses, calls = rpc
    responses[CONSENSUS + _words(0)] = responses[EXECUTION + _words(0)] = _page(True, 0)
    snap = await vs.fetch_validator_set(rpc_url=URL)
    assert snap.status == "ok" and snap.in_epoch_delay is False
    assert (snap.consensus_count, snap.execution_count, snap.bench_count) == (0, 0, 0)
    assert snap.lowest_active_stake_wei is None
    data = [params[0]["data"] for method, params in calls if method == "eth_call"]
    assert all(not calldata.startswith(VALIDATOR) for calldata in data)


@pytest.mark.parametrize("height", [None, 17, "latest", "0x", "0xgg", "0x01"])
@pytest.mark.asyncio
async def test_invalid_height_short_circuits_staking_calls(rpc, height):
    responses, calls = rpc
    responses["eth_blockNumber"] = height
    snap = await vs.fetch_validator_set(rpc_url=URL)
    assert snap.status == "unknown" and snap.epoch is None and snap.consensus_count is None
    assert calls == [("eth_blockNumber", [])]


@pytest.mark.parametrize("body", [
    pytest.param(None, id="failed-rpc"),
    pytest.param("0x00", id="truncated"),
    pytest.param("0x" + "z" * 64 + _words(0), id="non-hex"),
    pytest.param("0x" + _words(545, 2), id="invalid-delay"),
    pytest.param("0x" + _words(2**64, 0), id="epoch-overflow"),
])
@pytest.mark.asyncio
async def test_epoch_probe_failure_produces_unknown_snapshot(rpc, body):
    responses, calls = rpc
    responses[EPOCH] = body
    snap = await vs.fetch_validator_set(rpc_url=URL)
    assert snap.status == "unknown" and snap.epoch is None and snap.lowest_active_stake_wei is None
    assert [params[0]["data"] for m, params in calls if m == "eth_call"] == [EPOCH]


@pytest.mark.parametrize("body", [
    pytest.param(None, id="failed-rpc"),
    pytest.param(_page(True, 2, 4, 6)[:-64], id="truncated-id"),
    pytest.param(_page(True, 1, 4)[:-1] + "z", id="invalid-id"),
    pytest.param(_page(True, 1, 4) + _words(5), id="extra-id"),
    pytest.param("0x" + _words(2, 1, 96, 1, 4), id="invalid-flag"),
    pytest.param("0x" + _words(1, 1, 64, 1, 4), id="incorrect-array-offset"),
    pytest.param(_page(True, 1, 2**64), id="id-above-uint64"),
    pytest.param(_page(True, 2, 4, 4), id="repeated-id-inside-page"),
    pytest.param(_page(False, 0, 4), id="nonadvancing-cursor"),
    pytest.param(_page(True, 3, 4), id="skipped-cursor"),
    pytest.param(_page(False, 0), id="empty-unfinished-page"),
])
@pytest.mark.asyncio
async def test_invalid_set_returns_unknown_instead_of_partial_counts(rpc, body):
    responses, _ = rpc
    responses[CONSENSUS + _words(0)] = _page(True, 1, 10)
    responses[EXECUTION + _words(0)] = body
    snap = await vs.fetch_validator_set(rpc_url=URL)
    assert snap.status == "unknown" and snap.epoch == 545
    assert (snap.consensus_count, snap.execution_count, snap.bench_count) == (None, None, None)
    assert snap.lowest_active_stake_wei is None


@pytest.mark.asyncio
async def test_duplicate_across_pages_rejects_the_snapshot(rpc):
    responses, _ = rpc
    responses[CONSENSUS + _words(0)] = _page(False, 1, 10)
    responses[CONSENSUS + _words(1)] = _page(True, 2, 10)
    responses[EXECUTION + _words(0)] = _page(True, 1, 10)
    snap = await vs.fetch_validator_set(rpc_url=URL)
    assert snap.status == "unknown" and snap.consensus_count is None


@pytest.mark.parametrize("complete", [False, True], ids=["truncated", "complete"])
@pytest.mark.asyncio
async def test_set_at_page_limit_requires_an_explicit_final_page(rpc, complete):
    responses, calls = rpc
    for index in range(10):
        last = complete and index == 9
        responses[CONSENSUS + _words(index)] = _page(last, index + 1, 100 + index)
    responses[EXECUTION + _words(0)] = _page(True, 0)
    snap = await vs.fetch_validator_set(rpc_url=URL)
    if complete:
        assert snap.status == "ok" and snap.consensus_count == 10
    else:
        assert snap.status == "unknown" and snap.consensus_count is None
    pages = [params[0]["data"] for m, params in calls if m == "eth_call"]
    assert sum(data.startswith(CONSENSUS) for data in pages) == 10


@pytest.mark.parametrize("body", [
    pytest.param(None, id="failed-rpc"),
    pytest.param("0x00", id="missing-word"),
    pytest.param("0x" + _words(0) * 6 + "x" * 64, id="non-hex"),
    pytest.param(_validator(100)[:2 + 7 * 64], id="truncated-after-consensus-stake"),
    pytest.param(_validator(100)[:2 + 12 * 64], id="missing-dynamic-tails"),
])
@pytest.mark.asyncio
async def test_failed_floor_probe_leaves_valid_counts_visible(rpc, body):
    responses, _ = rpc
    responses[CONSENSUS + _words(0)] = responses[EXECUTION + _words(0)] = _page(True, 1, 10)
    responses[VALIDATOR + _words(10)] = body
    snap = await vs.fetch_validator_set(rpc_url=URL)
    assert snap.status == "ok" and snap.epoch == 545
    assert (snap.consensus_count, snap.execution_count, snap.bench_count) == (1, 1, 0)
    assert snap.lowest_active_stake_wei is None
