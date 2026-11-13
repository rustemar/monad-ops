"""Public alert APIs hide disk-specific details without modifying source data."""

from __future__ import annotations

import time

import httpx
import pytest

from monad_ops.api.app import build_app
from monad_ops.config import Config, NodeConfig
from monad_ops.rules.events import AlertEvent, Severity
from monad_ops.state import State
from monad_ops.storage import Storage


@pytest.mark.asyncio
async def test_disk_alerts_are_redacted_in_live_restored_and_history_responses(tmp_path):
    storage = Storage(tmp_path / "state.db")
    old = AlertEvent(
        rule="probe:disk_usage", severity=Severity.WARN, key="probe:disk_usage",
        title="Probe disk_usage WARN", detail="/synthetic/private-mount 91.2% used",
    )
    storage.write_alert(old, ts=time.time() - 30)
    state = State(storage=storage)
    assert state.bootstrap_alerts_from_storage(limit=10) == 1
    events = [
        AlertEvent(rule="probe:disk_usage", severity=Severity.CRITICAL,
                   key="probe:disk_usage", title="Probe disk_usage CRITICAL",
                   detail="/synthetic/private-mount 98.3% used (1.0 GB free)"),
        AlertEvent(rule="probe:disk_usage", severity=Severity.RECOVERED,
                   key="probe:disk_usage", title="Probe disk_usage recovered",
                   detail="all mounts < 85% (peak 42%)"),
        AlertEvent(rule="probe:disk_usage", severity=Severity.INFO,
                   key="probe:disk_usage", title="Probe disk_usage INFO",
                   detail="unexpected /synthetic/private-mount 42%"),
        AlertEvent(rule="other", severity=Severity.WARN, key="other",
                   title="Other warning", detail="Other alert detail unchanged"),
    ]
    for event in events:
        state.add_alert(event)

    config = Config(node=NodeConfig(name="test-node", rpc_url="http://127.0.0.1:9999"))
    app = build_app(state, config, enricher=None, labels=None)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        responses = [
            (await client.get("/api/state")).json()["current_alerts"],
            (await client.get("/api/alerts")).json(),
            (await client.get("/api/alerts/history")).json()["alerts"],
        ]
    expected = {
        "warn": "Disk usage warning.",
        "critical": "Disk usage critical.",
        "recovered": "Disk usage recovered.",
        "info": "Disk usage status update.",
    }
    for records in responses:
        assert len(records) == 5
        disk = [item for item in records if item["rule"] == "probe:disk_usage"]
        assert len(disk) == 4
        for item in disk:
            assert item["detail"] == expected[item["severity"]]
            assert "/synthetic/" not in str(item)
            assert "91.2%" not in str(item) and "98.3%" not in str(item)
        other = next(item for item in records if item["rule"] == "other")
        assert other["detail"] == "Other alert detail unchanged"
        assert other["title"] == "Other warning" and other["severity"] == "warn"

    # APIs must not mutate in-memory records or persisted diagnostic detail.
    assert state.recent_alerts(limit=10)[0].detail == old.detail
    assert state.recent_alerts(limit=10)[1].detail == events[0].detail
    persisted = storage.load_alerts_range(limit=10)
    assert {item.detail for item in persisted} == {old.detail, *(item.detail for item in events)}
    storage.close()
