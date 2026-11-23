"""The API's shared cache must isolate request cancellation and failures."""

from __future__ import annotations

import asyncio

import pytest

from monad_ops.api.app import build_app
from monad_ops.api.routes import blocks
from monad_ops.config import Config, NodeConfig
from monad_ops.state import State


@pytest.fixture
def cached(monkeypatch):
    """Capture the actual app-scoped cache passed to the route groups."""
    captured = []
    build_router = blocks.build_router

    def capture(ctx):
        captured.append(ctx.cached)
        return build_router(ctx)

    monkeypatch.setattr(blocks, "build_router", capture)
    build_app(State(), Config(node=NodeConfig(name="test", rpc_url="http://localhost")))
    return captured[0]


@pytest.mark.asyncio
async def test_cancelled_waiter_does_not_cancel_shared_load(cached):
    entered = asyncio.Event()
    release = asyncio.Event()
    calls = 0

    async def load():
        nonlocal calls
        calls += 1
        entered.set()
        await release.wait()
        return "value"

    leader = asyncio.create_task(cached("test", 10, (), load))
    await entered.wait()
    cancelled_waiter = asyncio.create_task(cached("test", 10, (), load))
    surviving_waiter = asyncio.create_task(cached("test", 10, (), load))
    await asyncio.sleep(0)  # Both waiters enter the already-running load.
    cancelled_waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await cancelled_waiter
    release.set()
    assert await asyncio.wait_for(leader, 1) == "value"
    assert await asyncio.wait_for(surviving_waiter, 1) == "value"
    assert await cached("test", 10, (), load) == "value"
    assert calls == 1


@pytest.mark.asyncio
async def test_cancelled_first_request_keeps_one_load_for_surviving_waiters(cached):
    entered = asyncio.Event()
    release = asyncio.Event()
    calls = 0

    async def load():
        nonlocal calls
        calls += 1
        entered.set()
        await release.wait()
        return "recovered"

    leader = asyncio.create_task(cached("test", 10, (), load))
    await entered.wait()
    first = asyncio.create_task(cached("test", 10, (), load))
    second = asyncio.create_task(cached("test", 10, (), load))
    await asyncio.sleep(0)
    leader.cancel()
    with pytest.raises(asyncio.CancelledError):
        await leader
    release.set()
    assert await asyncio.wait_for(first, 1) == "recovered"
    assert await asyncio.wait_for(second, 1) == "recovered"
    assert await cached("test", 10, (), load) == "recovered"
    assert calls == 1


@pytest.mark.asyncio
async def test_request_can_join_load_after_all_requesters_leave(cached):
    entered = asyncio.Event()
    release = asyncio.Event()
    calls = 0

    async def load():
        nonlocal calls
        calls += 1
        entered.set()
        await release.wait()
        return "value"

    first = asyncio.create_task(cached("test", 10, (), load))
    await entered.wait()
    second = asyncio.create_task(cached("test", 10, (), load))
    await asyncio.sleep(0)
    first.cancel()
    second.cancel()
    for task in (first, second):
        with pytest.raises(asyncio.CancelledError):
            await task
    # A new request still joins the original load rather than duplicating it.
    returning = asyncio.create_task(cached("test", 10, (), load))
    release.set()
    assert await asyncio.wait_for(returning, 1) == "value"
    assert await cached("test", 10, (), load) == "value"
    assert calls == 1


@pytest.mark.asyncio
async def test_abandoned_successful_load_populates_cache(cached):
    entered = asyncio.Event()
    release = asyncio.Event()
    finished = asyncio.Event()
    calls = 0

    async def load():
        nonlocal calls
        calls += 1
        entered.set()
        await release.wait()
        finished.set()
        return "value"

    first = asyncio.create_task(cached("test", 10, (), load))
    await entered.wait()
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    release.set()
    await asyncio.wait_for(finished.wait(), 1)
    assert await cached("test", 10, (), load) == "value"
    assert calls == 1


@pytest.mark.asyncio
async def test_load_failure_reaches_waiters_and_next_call_can_retry(cached):
    entered = asyncio.Event()
    release = asyncio.Event()
    calls = 0

    async def load():
        nonlocal calls
        calls += 1
        if calls == 1:
            entered.set()
            await release.wait()
            raise ValueError("load failed")
        return "recovered"

    leader = asyncio.create_task(cached("test", 10, (), load))
    await entered.wait()
    waiter = asyncio.create_task(cached("test", 10, (), load))
    await asyncio.sleep(0)
    release.set()
    for task in (leader, waiter):
        with pytest.raises(ValueError, match="load failed"):
            await asyncio.wait_for(task, 1)
    assert await cached("test", 10, (), load) == "recovered"
    assert calls == 2


@pytest.mark.asyncio
async def test_abandoned_load_failure_is_observed_and_can_retry(cached):
    entered = asyncio.Event()
    release = asyncio.Event()
    failed = asyncio.Event()
    errors = []
    loop = asyncio.get_running_loop()
    old_handler = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, context: errors.append(context))
    calls = 0

    async def load():
        nonlocal calls
        calls += 1
        if calls == 1:
            entered.set()
            await release.wait()
            failed.set()
            raise ValueError("load failed")
        return "recovered"

    try:
        first = asyncio.create_task(cached("test", 10, (), load))
        await entered.wait()
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        release.set()
        await asyncio.wait_for(failed.wait(), 1)
        await asyncio.sleep(0)  # Run the task's failure observer.
        assert await cached("test", 10, (), load) == "recovered"
        await asyncio.sleep(0)
        assert calls == 2
        assert errors == []
    finally:
        loop.set_exception_handler(old_handler)
