"""Shutdown ordering in cli.py — the API server is asked to exit, not cancelled.

Cancelling ``uvicorn.Server.serve()`` while it waits on the lifespan queue
made every stop log a CancelledError traceback (seen on each restart
until 2026-09-13). The helper flips ``should_exit`` and waits, cancelling
only when the server overstays its grace period.
"""

from __future__ import annotations

import asyncio

import pytest

from monad_ops.cli import stop_server


class _FakeServer:
    def __init__(self, *, exits: bool):
        self.should_exit = False
        self._exits = exits

    async def serve(self) -> None:
        while not (self.should_exit and self._exits):
            await asyncio.sleep(0.001)


@pytest.mark.asyncio
async def test_server_is_asked_to_exit_and_allowed_to_finish():
    server = _FakeServer(exits=True)
    task = asyncio.create_task(server.serve())
    await asyncio.sleep(0)

    await stop_server(server, task, grace_sec=1.0)

    assert server.should_exit is True
    assert task.done() and not task.cancelled()


@pytest.mark.asyncio
async def test_server_that_overstays_the_grace_period_is_cancelled():
    server = _FakeServer(exits=False)
    task = asyncio.create_task(server.serve())
    await asyncio.sleep(0)

    await stop_server(server, task, grace_sec=0.02)
    await asyncio.sleep(0)

    assert server.should_exit is True
    assert task.cancelled()


@pytest.mark.asyncio
async def test_server_failure_at_stop_does_not_propagate():
    async def boom():
        raise RuntimeError("bind lost")

    server = _FakeServer(exits=True)
    task = asyncio.create_task(boom())

    await stop_server(server, task, grace_sec=1.0)

    assert task.done() and task.exception() is not None
