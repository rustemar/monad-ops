"""Probe loop startup, retry, and shutdown behavior with offline probes."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from monad_ops import cli
from monad_ops.collector.probes import ProbeResult
from monad_ops.rules.events import AlertEvent, Severity


@pytest.mark.asyncio
async def test_probe_loop_restores_envelope_and_reports_recovery(monkeypatch):
    history = [AlertEvent(rule="probe:services", key="probe:services",
                          severity=Severity.WARN, title="t", detail="d")]
    trace = []
    samples = iter([
        [ProbeResult("services", "warn", "warm", {})],
        [ProbeResult("services", "ok", "recovered", {})],
    ])
    services = ["node.service"]
    config = SimpleNamespace(node=SimpleNamespace(services=services))

    class FakeState:
        def recent_alerts(self, *, limit):
            trace.append(("history", limit))
            return history

        def set_probes(self, results):
            trace.append(("state", results[0].status))

    class FakeSink:
        async def deliver(self, event):
            trace.append(("alert", event.key, event.severity))

    async def run(probe_services):
        assert probe_services is services
        result = next(samples)
        trace.append(("probe", result[0].status))
        return result

    async def sleep(delay):
        trace.append(("sleep", delay))
        if sum(item[0] == "sleep" for item in trace) == 2:
            raise asyncio.CancelledError

    monkeypatch.setattr(cli, "run_all_probes", run)
    monkeypatch.setattr(cli.asyncio, "sleep", sleep)
    coroutine = cli._probe_loop(config, FakeState(), FakeSink())
    assert trace == []
    with pytest.raises(asyncio.CancelledError):
        await coroutine
    assert trace == [
        ("history", 1000),
        ("probe", "warn"), ("state", "warn"), ("sleep", 60),
        ("probe", "ok"), ("state", "ok"),
        ("alert", "probe:services", Severity.RECOVERED), ("sleep", 60),
    ]


@pytest.mark.asyncio
async def test_probe_loop_logs_failure_and_retries(monkeypatch):
    trace = []
    config = SimpleNamespace(node=SimpleNamespace(services=[]))

    class FakeState:
        def recent_alerts(self, *, limit):
            return []

        def set_probes(self, results):
            trace.append(("state", results[0].status))

    class FakeSink:
        async def deliver(self, event):
            trace.append(("alert", event.severity))

    class FakeLog:
        def error(self, name, **fields):
            trace.append(("error", name, fields["exc"]))

    async def run(services):
        trace.append(("probe",))
        if sum(item[0] == "probe" for item in trace) == 1:
            raise RuntimeError("probe failed")
        return [ProbeResult("services", "warn", "warm", {})]

    async def sleep(delay):
        trace.append(("sleep", delay))
        if sum(item[0] == "sleep" for item in trace) == 2:
            raise asyncio.CancelledError

    monkeypatch.setattr(cli, "run_all_probes", run)
    monkeypatch.setattr(cli.asyncio, "sleep", sleep)
    monkeypatch.setattr(cli, "log", FakeLog())
    with pytest.raises(asyncio.CancelledError):
        await cli._probe_loop(config, FakeState(), FakeSink())
    assert trace == [
        ("probe",), ("error", "probe_loop.error", "probe failed"), ("sleep", 60),
        ("probe",), ("state", "warn"), ("alert", Severity.WARN), ("sleep", 60),
    ]
