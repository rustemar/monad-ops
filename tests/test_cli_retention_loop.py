"""Offline checks of retention startup, retry, and cancellation."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from monad_ops import cli


@pytest.mark.asyncio
@pytest.mark.parametrize(("interval_hours", "interval_sec"), [(0, 3600), (24, 86400)])
async def test_retention_delays_then_retries_with_configured_interval(
    monkeypatch, interval_hours, interval_sec,
):
    trace = []
    config = SimpleNamespace(
        retention=SimpleNamespace(keep_days=17, interval_hours=interval_hours),
    )

    class FakeStorage:
        def prune_older_than(self, *, keep_days):
            trace.append(("prune", keep_days))
            if sum(item[0] == "prune" for item in trace) == 1:
                raise RuntimeError("prune failed")
            return {"blocks": 3}

    class FakeLog:
        def info(self, name, **fields):
            trace.append((name, fields))

        def error(self, name, **fields):
            trace.append((name, fields))

    async def sleep(delay):
        trace.append(("sleep", delay))
        if sum(item[0] == "sleep" for item in trace) == 3:
            raise asyncio.CancelledError

    async def to_thread(fn, **kwargs):
        trace.append(("worker",))
        return fn(**kwargs)

    monkeypatch.setattr(cli.asyncio, "sleep", sleep)
    monkeypatch.setattr(cli.asyncio, "to_thread", to_thread)
    monkeypatch.setattr(cli, "log", FakeLog())
    with pytest.raises(asyncio.CancelledError):
        await cli._retention_loop(config, FakeStorage())
    assert trace == [
        ("retention.ready", {"keep_days": 17, "interval_sec": interval_sec}),
        ("sleep", 60),
        ("worker",), ("prune", 17), ("retention.error", {"exc": "prune failed"}),
        ("sleep", interval_sec),
        ("worker",), ("prune", 17), ("retention.prune", {"blocks": 3}),
        ("sleep", interval_sec),
    ]


@pytest.mark.asyncio
async def test_retention_cancelled_during_startup_does_not_prune(monkeypatch):
    config = SimpleNamespace(retention=SimpleNamespace(keep_days=14, interval_hours=24))

    class FakeStorage:
        def prune_older_than(self, *, keep_days):
            pytest.fail("retention should not start when startup delay is cancelled")

    async def sleep(delay):
        assert delay == 60
        raise asyncio.CancelledError

    monkeypatch.setattr(cli.asyncio, "sleep", sleep)
    with pytest.raises(asyncio.CancelledError):
        await cli._retention_loop(config, FakeStorage())
