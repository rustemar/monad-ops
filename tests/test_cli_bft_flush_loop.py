"""Exercise the BFT flush worker across minute rollovers in both storage modes."""

import asyncio
from unittest.mock import Mock, call

import pytest

from monad_ops import cli
from monad_ops.parser import ConsensusEvent, ConsensusEventKind
from monad_ops.state import State
from monad_ops.storage import BftBaseFee, BftMinute, Storage


@pytest.mark.asyncio
@pytest.mark.parametrize("persist", [False, True], ids=["memory-only", "storage"])
async def test_bft_flush_drains_each_tick_and_keeps_current_minute(monkeypatch, persist):
    storage = Mock(spec=Storage) if persist else None
    state = State(storage=storage)
    state.add_consensus_event(ConsensusEvent(ConsensusEventKind.ROUND_ADVANCE_QC, 1, 1, 60_000))
    next_minutes = iter([120_000, 180_000])

    async def tick(delay):
        assert delay == 1
        minute = next(next_minutes, None)
        if minute is None:
            raise asyncio.CancelledError
        if minute == 180_000:
            # The previous tick consumed the closed bucket and its proposal.
            assert state.drain_bft_pending() == ([], [], BftMinute(120_000, 1, 0, 0))
        state.add_consensus_event(ConsensusEvent(ConsensusEventKind.ROUND_ADVANCE_QC, 2, 1, minute))
        state.add_consensus_event(ConsensusEvent(
            ConsensusEventKind.PROPOSAL, 2, None, minute, block_seq=minute, base_fee=100,
        ))

    monkeypatch.setattr(cli.asyncio, "sleep", tick)
    with pytest.raises(asyncio.CancelledError):
        await cli._bft_flush_loop(state, storage)

    assert state.drain_bft_pending() == ([], [], BftMinute(180_000, 1, 0, 0))
    if storage is not None:
        assert storage.bulk_write_bft.call_args_list == [
            call([BftMinute(60_000, 1, 0, 0), BftMinute(120_000, 1, 0, 0)],
                 [BftBaseFee(120_000, 120_000, 100)]),
            call([BftMinute(120_000, 1, 0, 0), BftMinute(180_000, 1, 0, 0)],
                 [BftBaseFee(180_000, 180_000, 100)]),
        ]
