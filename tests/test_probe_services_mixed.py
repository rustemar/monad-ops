"""Service probe decisions from systemctl status and exit code."""

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
        (3, ""), (4, "unknown"),
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
    assert result.summary == "service state unavailable for 1/2 check(s)"


@pytest.mark.asyncio
async def test_service_counts_unique_units_after_the_last_observations(monkeypatch):
    outputs = iter([(3, "inactive", ""), (0, "active", ""), (0, "reloading", "")])

    async def fake_run(cmd):
        return next(outputs)

    monkeypatch.setattr(probes, "_run", fake_run)
    result = await probes.probe_services(["same", "same", "other"])
    assert result.status == "ok"
    assert result.summary == "2/2 active"
    assert result.details == {"same": "active", "other": "reloading"}


@pytest.mark.asyncio
async def test_no_configured_services_cannot_recover_an_open_alert(monkeypatch):
    async def fake_run(cmd):
        pytest.fail("no service check should run for an empty list")

    monkeypatch.setattr(probes, "_run", fake_run)
    result = await probes.probe_services([])
    assert result.status == "unknown"
    assert result.summary == "no services configured"
    rule = ProbeAlertRule({"services": "critical"})
    assert rule.evaluate([result]) == []
    assert rule._prev["services"] == "critical"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("response", "expected"),
    [
        ((0, "active", ""), "ok"),
        ((0, "reloading", ""), "ok"),
        ((0, "refreshing", ""), "ok"),
        ((3, "inactive", ""), "critical"),
        ((3, "failed", ""), "critical"),
        ((3, "activating", ""), "unknown"),
        ((3, "deactivating", ""), "unknown"),
        ((3, "maintenance", ""), "unknown"),
        ((1, "", "Failed to connect to bus"), "unknown"),
        ((1, "active", ""), "unknown"),
        ((0, "inactive", ""), "unknown"),
        ((0, "active\ninactive", ""), "unknown"),
    ],
)
async def test_service_status_uses_the_return_code_and_state(monkeypatch, response, expected):
    outputs = iter([(0, "active", ""), response])

    async def fake_run(cmd):
        return next(outputs)

    monkeypatch.setattr(probes, "_run", fake_run)
    result = await probes.probe_services(["a", "b"])
    assert result.status == expected
    assert "Failed to connect to bus" not in result.summary
    if expected == "unknown":
        rule = ProbeAlertRule({"services": "critical"})
        assert rule.evaluate([result]) == []
        assert rule._prev["services"] == "critical"
        assert ProbeAlertRule().evaluate([result]) == []


@pytest.mark.asyncio
async def test_confirmed_missing_unit_is_reported_separately_from_down_unit(monkeypatch):
    outputs = {
        "miss.service": (4, "inactive\n", ""),
        "down.service": (3, "failed\n", ""),
        "ambiguous.service": (4, "unknown\n", ""),
        "timeout.service": (127, "", "timeout"),
    }

    async def fake_run(cmd):
        return outputs[cmd[-1]]

    monkeypatch.setattr(probes, "_run", fake_run)
    result = await probes.probe_services(list(outputs))
    assert result.status == "critical"
    assert "configured unit(s) not found: miss.service" in result.summary
    assert "service(s) not active: down.service" in result.summary
    assert "unavailable" in result.summary
    assert "ambiguous.service" not in result.summary


@pytest.mark.asyncio
async def test_service_spawn_error_cannot_erase_a_confirmed_failure(monkeypatch):
    async def fake_run(cmd):
        if cmd[-1] == "other.service":
            raise PermissionError("fixture command error")
        return 3, "inactive", ""

    monkeypatch.setattr(probes, "_run", fake_run)
    result = await probes.probe_services(["down.service", "other.service"])
    assert result.status == "critical"
    assert result.details["other.service"] == "probe_error"
    assert "fixture command error" not in result.summary
