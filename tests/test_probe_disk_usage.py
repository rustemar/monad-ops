"""Disk probe behavior with isolated fake filesystem observations."""

from __future__ import annotations

from collections import namedtuple

import pytest

from monad_ops.collector import probes
from monad_ops.rules.probe_alerts import ProbeAlertRule

Usage = namedtuple("Usage", "total used free")


class FakePath:
    def __init__(self, name, exists=True):
        self.name = name
        self._exists = exists

    def exists(self):
        if isinstance(self._exists, Exception):
            raise self._exists
        return self._exists

    def __str__(self):
        return self.name


def fake_usage(monkeypatch, values):
    def read(path):
        value = values[path.name]
        if isinstance(value, Exception):
            raise value
        return value

    monkeypatch.setattr(probes.shutil, "disk_usage", read)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [
    None, OSError("private detail"), Usage(0, 0, 0), Usage(100, -1, 100),
    Usage(100, 101, 0), Usage(100, float("nan"), 10), Usage(100, 10, float("inf")),
])
async def test_no_valid_disk_observations_are_unknown(monkeypatch, failure):
    path = FakePath("missing", exists=failure is not None)
    fake_usage(monkeypatch, {"missing": failure})
    result = await probes.probe_disk_usage([path])
    assert result.status == "unknown"


@pytest.mark.asyncio
async def test_inaccessible_path_does_not_become_ok(monkeypatch):
    fake_usage(monkeypatch, {})
    result = await probes.probe_disk_usage([FakePath("denied", OSError("private"))])
    assert result.status == "unknown"
    assert result.details["denied"]["error"] == "private"


@pytest.mark.asyncio
async def test_missing_optional_path_does_not_mask_a_good_observation(monkeypatch):
    fake_usage(monkeypatch, {"root": Usage(100, 40, 55)})
    result = await probes.probe_disk_usage([FakePath("root"), FakePath("optional", False)])
    assert result.status == "ok"
    assert result.details["optional"] == {"exists": False}
    assert result.details["root"]["used_ratio"] == 0.4


@pytest.mark.asyncio
@pytest.mark.parametrize("used, expected", [(40, "unknown"), (85, "warn"), (95, "critical")])
async def test_incomplete_coverage_retains_observed_alerts(monkeypatch, used, expected):
    fake_usage(monkeypatch, {"root": Usage(100, used, 100 - used), "other": OSError("private")})
    result = await probes.probe_disk_usage([FakePath("root"), FakePath("other")])
    assert result.status == expected
    assert result.details["other"]["error"] == "private"


@pytest.mark.asyncio
async def test_unreadable_disk_does_not_close_existing_alert(monkeypatch):
    path = FakePath("root")
    rule = ProbeAlertRule()
    fake_usage(monkeypatch, {"root": Usage(100, 90, 10)})
    assert [e.severity.value for e in rule.evaluate([await probes.probe_disk_usage([path])])] == [
        "warn"
    ]
    fake_usage(monkeypatch, {"root": OSError("temporarily unreadable")})
    assert rule.evaluate([await probes.probe_disk_usage([path])]) == []
    fake_usage(monkeypatch, {"root": Usage(100, 40, 60)})
    assert [e.severity.value for e in rule.evaluate([await probes.probe_disk_usage([path])])] == [
        "recovered"
    ]
