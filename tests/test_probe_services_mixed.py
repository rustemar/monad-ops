"""Service probe decisions when some checks cannot be observed."""

from __future__ import annotations

import pytest

from monad_ops.collector import probes
from monad_ops.rules.events import Severity
from monad_ops.rules.probe_alerts import ProbeAlertRule


@pytest.mark.asyncio
@pytest.mark.parametrize("down", ["inactive", "failed"])
@pytest.mark.parametrize("reverse", [False, True])
async def test_confirmed_down_is_not_masked_by_timeout(monkeypatch, down, reverse):
    outputs = {
        "down.service": (3, down + "\n", ""),
        "unknown.service": (127, "", "timeout"),
        "active.service": (0, "active\n", ""),
    }

    async def fake_run(cmd):
        return outputs[cmd[-1]]

    monkeypatch.setattr(probes, "_run", fake_run)
    names = list(outputs)
    result = await probes.probe_services(list(reversed(names)) if reverse else names)
    assert result.status == "critical"
    assert "down.service" in result.summary
    assert "unknown.service" not in result.summary
    assert "unavailable" in result.summary
    assert result.details["unknown.service"] == "probe_error"
    events = ProbeAlertRule().evaluate([result])
    assert len(events) == 1 and events[0].severity is Severity.CRITICAL


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("rc", "stdout"),
    [
        (127, ""), (127, "inactive"), (127, "failed"),
        (0, "inactive"), (0, "failed"), (0, ""),
        (3, ""), (4, "inactive"), (4, "unknown"),
        (3, "activating"), (3, "deactivating"), (3, "garbage"),
    ],
)
async def test_timeout_and_ambiguous_results_do_not_prove_outage(monkeypatch, rc, stdout):
    outputs = {
        "ambiguous.service": (rc, stdout, ""),
        "unknown.service": (127, "", "timeout"),
    }

    async def fake_run(cmd):
        return outputs[cmd[-1]]

    monkeypatch.setattr(probes, "_run", fake_run)
    result = await probes.probe_services(list(outputs))
    assert result.status == "unknown"
    assert ProbeAlertRule().evaluate([result]) == []
    rule = ProbeAlertRule({"services": "critical"})
    assert rule.evaluate([result]) == []
    assert rule._prev["services"] == "critical"


@pytest.mark.asyncio
async def test_mixed_timeout_no_false_recovery_then_real_recovery(monkeypatch):
    outputs = {"a": (0, "active", ""), "b": (127, "", "timeout")}

    async def fake_run(cmd):
        return outputs[cmd[-1]]

    monkeypatch.setattr(probes, "_run", fake_run)
    rule = ProbeAlertRule({"services": "critical"})
    assert rule.evaluate([await probes.probe_services(["a", "b"])]) == []
    outputs["b"] = (0, "active", "")
    recovered = rule.evaluate([await probes.probe_services(["a", "b"])])
    assert len(recovered) == 1 and recovered[0].severity is Severity.RECOVERED


@pytest.mark.asyncio
async def test_duplicate_service_observations_use_latest_value(monkeypatch):
    outputs = iter([(3, "inactive", ""), (0, "active", ""), (127, "", "timeout")])

    async def fake_run(cmd):
        return next(outputs)

    monkeypatch.setattr(probes, "_run", fake_run)
    result = await probes.probe_services(["same", "same", "unknown"])
    assert result.status == "unknown"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("responses", "expected"),
    [
        ([(0, "active", ""), (0, "active", "")], "ok"),
        ([(0, "active", ""), (3, "inactive", "")], "critical"),
        ([(0, "active", ""), (3, "activating", "")], "critical"),
        ([(0, "active", ""), (1, "", "")], "critical"),
    ],
)
async def test_no_probe_errors_retains_existing_semantics(monkeypatch, responses, expected):
    outputs = iter(responses)

    async def fake_run(cmd):
        return next(outputs)

    monkeypatch.setattr(probes, "_run", fake_run)
    result = await probes.probe_services(["a", "b"])
    assert result.status == expected
