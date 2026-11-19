"""Classifier tests for a single TrieDB device and its alert lifecycle."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, call

import pytest

from monad_ops.collector import probes
from monad_ops.rules.events import Severity
from monad_ops.rules.probe_alerts import ProbeAlertRule


def _limits(granularity: int, maximum: int) -> str:
    return json.dumps({"blockdevices": [{"disc-gran": granularity, "disc-max": maximum}]})


@pytest.fixture
def command_runner(monkeypatch, tmp_path):
    target = tmp_path / "device-fixture"
    target.touch()
    link = tmp_path / "triedb"
    link.symlink_to(target)
    monkeypatch.setattr(probes, "_TRIEDB_DEVICE", link)
    runner = AsyncMock()
    monkeypatch.setattr(probes, "_run", runner)
    return runner


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "outcome",
    [
        (1, "", "unavailable at /dev/test-triedb-device"),
        (127, _limits(0, 0), "command timeout"),
        PermissionError("cannot execute lsblk"),
        (0, "not-json", ""),
        (0, '{"blockdevices":[]}', ""),
        (0, '{"blockdevices":[{"disc-gran":null,"disc-max":0}]}', ""),
        (0, '{"blockdevices":[{"disc-gran":false,"disc-max":0}]}', ""),
        (0, '{"blockdevices":[{"disc-gran":0}]}', ""),
        (0, '{"blockdevices":[{"disc-gran":-1,"disc-max":0}]}', ""),
        (
            0,
            '{"blockdevices":[{"disc-gran":0,"disc-max":0},{"disc-gran":4096,"disc-max":8192}]}',
            "",
        ),
    ],
)
async def test_unconfirmed_discard_never_opens_or_recovers_alert(command_runner, outcome):
    command_runner.side_effect = [outcome, (127, "", "")]

    result = await probes.probe_triedb_device()

    assert result.status == "unknown"
    assert result.summary == "TrieDB discard capability could not be verified"
    assert ProbeAlertRule().evaluate([result]) == []
    open_alert = ProbeAlertRule({"triedb_device": "critical"})
    assert open_alert.evaluate([result]) == []
    assert open_alert._prev["triedb_device"] == "critical"
    target = str(probes._TRIEDB_DEVICE.resolve())
    assert command_runner.await_args_list == [
        call([
            "lsblk", "--bytes", "--nodeps", "--json",
            "--output", "DISC-GRAN,DISC-MAX", "--", target,
        ]),
        call(["nvme", "smart-log", target, "-o", "json"]),
    ]


@pytest.mark.asyncio
async def test_unreadable_device_path_is_unknown(monkeypatch, command_runner):
    class UnreadableDevice:
        def exists(self):
            raise PermissionError("permission denied for private path")

    monkeypatch.setattr(probes, "_TRIEDB_DEVICE", UnreadableDevice())

    result = await probes.probe_triedb_device()

    assert result.status == "unknown"
    assert result.summary == "TrieDB device path could not be checked"
    command_runner.assert_not_awaited()


@pytest.mark.asyncio
async def test_explicit_unsupported_discard_remains_critical_until_remeasured(command_runner):
    command_runner.side_effect = [
        (0, _limits(4096, 0), ""),
        PermissionError("nvme unavailable"),
        (0, _limits(4096, 2199023255040), ""),
        (0, '{"critical_warning":0,"percentage_used":40,"available_spare":70}', ""),
    ]
    rule = ProbeAlertRule()

    unsupported = await probes.probe_triedb_device()
    working = await probes.probe_triedb_device()

    assert unsupported.status == "critical"
    assert unsupported.summary == "DISCARD unavailable on TrieDB device"
    assert unsupported.details["disc_max"] == 0
    assert [event.severity for event in rule.evaluate([unsupported])] == [Severity.CRITICAL]
    assert working.status == "ok"
    assert working.summary == "DISCARD supported + SMART clean"
    assert working.details["disc_gran"] == 4096
    assert [event.severity for event in rule.evaluate([working])] == [Severity.RECOVERED]


@pytest.mark.asyncio
async def test_positive_discard_and_observed_smart_warning(command_runner):
    command_runner.side_effect = [
        (0, _limits(4096, 8192), ""),
        (0, '{"critical_warning":0,"percentage_used":80,"available_spare":70}', ""),
    ]

    result = await probes.probe_triedb_device()

    assert result.status == "warn"
    assert result.summary == "DISCARD ok; NVMe SMART shows wear or warning"
    assert result.details["smart_percentage_used"] == 80
