"""Public probes and alerts must not expose sensitive host diagnostics."""

from __future__ import annotations

import time
from pathlib import Path
from unittest import mock

import httpx
import pytest

from monad_ops.api.app import build_app
from monad_ops.api.public_alerts import public_alert
from monad_ops.api.routes.node import _sanitize_probe_summary
from monad_ops.collector.probes import ProbeResult, probe_fd_limits, probe_udp_config
from monad_ops.config import Config, NodeConfig
from monad_ops.rules.events import AlertEvent, Severity
from monad_ops.state import State
from monad_ops.storage import Storage


@pytest.mark.parametrize(
    ("name", "status", "raw", "expected"),
    [
        (
            "fd_limits",
            "critical",
            "synthetic nofile soft=512 < 4096",
            "file descriptor limits critical",
        ),
        (
            "fd_limits",
            "warn",
            "synthetic nofile soft=8192 < 16384",
            "file descriptor limits warning",
        ),
        (
            "fd_limits",
            "unknown",
            "cannot read /proc/4242/limits: synthetic error",
            "file descriptor limits status unavailable",
        ),
        ("fd_limits", "ok", "soft=1000000 hard=1000000", "file descriptor limits normal"),
        ("fd_limits", "future", "synthetic secret", "file descriptor limits status unavailable"),
        ("fd_limits", ["bad"], None, "file descriptor limits status unavailable"),
        (
            "udp_config",
            "ok",
            "authenticated UDP configured on :19999",
            "authenticated UDP status OK",
        ),
        (
            "udp_config",
            "warn",
            "node.toml missing authenticated_bind_address_port",
            "authenticated UDP status warning",
        ),
        (
            "udp_config",
            "critical",
            "authenticated UDP is not listening :19999",
            "authenticated UDP status critical",
        ),
        (
            "udp_config",
            "unknown",
            "node.toml not readable and :19999 not detected",
            "authenticated UDP status unavailable",
        ),
        ("udp_config", ["bad"], None, "authenticated UDP status unavailable"),
    ],
)
@pytest.mark.asyncio
async def test_public_probe_preserves_status_and_hides_raw_summary(name, status, raw, expected):
    state = State()
    target = ProbeResult(name, status, raw, {"secret": "synthetic private data"})
    other = ProbeResult("services", "ok", "3/3 active", {})
    state.set_probes([target, other])
    config = Config(node=NodeConfig(name="test", rpc_url="http://127.0.0.1:9999"))
    app = build_app(state, config, enricher=None, labels=None)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as c:
        for endpoint in ("/api/probes", "/api/probes/public"):
            response = await c.get(endpoint)
            assert response.status_code == 200
            probes = {p["name"]: p for p in response.json()["probes"]}
            assert probes[name] == {"name": name, "status": status, "summary": expected}
            assert probes["services"]["summary"] == "3/3 active"
            assert "synthetic" not in response.text and "19999" not in response.text
            assert "/proc/4242" not in response.text and "8192" not in response.text
    private, _ = state.probes()
    assert private[0] is target and target.summary == raw
    assert target.details == {"secret": "synthetic private data"}


@pytest.mark.asyncio
async def test_real_producer_failure_summaries_are_neutralized(monkeypatch):
    async def fake_systemctl(*args, **kwargs):
        return (0, "4242", "")

    monkeypatch.setattr("monad_ops.collector.probes._run", fake_systemctl)
    with (
        mock.patch.object(Path, "exists", return_value=True),
        mock.patch.object(Path, "read_text", return_value="Max open files 512 4096 files"),
    ):
        fd = await probe_fd_limits()
    assert fd.status == "critical" and "512" in fd.summary
    assert (
        _sanitize_probe_summary(fd.name, fd.summary, fd.status) == "file descriptor limits critical"
    )

    async def fake_ss(*args, **kwargs):
        return (0, "", "")

    monkeypatch.setattr("monad_ops.collector.probes._run", fake_ss)
    with mock.patch.object(Path, "exists", return_value=False):
        udp = await probe_udp_config()
    assert udp.status == "unknown" and ":8001" in udp.summary
    assert (
        _sanitize_probe_summary(udp.name, udp.summary, udp.status)
        == "authenticated UDP status unavailable"
    )


@pytest.mark.asyncio
async def test_sensitive_alerts_redacted_in_live_restored_history_without_mutation(tmp_path):
    storage = Storage(tmp_path / "state.db")
    old = AlertEvent(
        rule="probe:fd_limits",
        severity=Severity.WARN,
        key="probe:fd_limits",
        title="Probe fd_limits WARN",
        detail="soft=8192 < 16384",
    )
    storage.write_alert(old, ts=time.time() - 30)
    state = State(storage=storage)
    assert state.bootstrap_alerts_from_storage(limit=10) == 1
    events = [
        AlertEvent(
            rule="probe:fd_limits",
            severity=Severity.CRITICAL,
            key="probe:fd_limits",
            title="Probe fd_limits CRITICAL",
            detail="/proc/4242 nofile soft=512",
        ),
        AlertEvent(
            rule="probe:udp_config",
            severity=Severity.WARN,
            key="probe:udp_config",
            title="Probe udp_config WARN",
            detail="synthetic :19999 path /private/node.toml",
        ),
        AlertEvent(
            rule="probe:udp_config",
            severity=Severity.RECOVERED,
            key="probe:udp_config",
            title="Probe udp_config recovered",
            detail="authenticated UDP on :19999",
        ),
        AlertEvent(
            rule="other",
            severity=Severity.WARN,
            key="other",
            title="Other",
            detail="Other detail unchanged",
        ),
    ]
    for event in events:
        state.add_alert(event)
    config = Config(node=NodeConfig(name="test", rpc_url="http://127.0.0.1:9999"))
    app = build_app(state, config, enricher=None, labels=None)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as c:
        responses = [
            (await c.get("/api/state")).json()["current_alerts"],
            (await c.get("/api/alerts")).json(),
            (await c.get("/api/alerts/history")).json()["alerts"],
        ]
    expected = {
        ("probe:fd_limits", "warn"): "File descriptor limits warning.",
        ("probe:fd_limits", "critical"): "File descriptor limits critical.",
        ("probe:udp_config", "warn"): "Authenticated UDP status warning.",
        ("probe:udp_config", "recovered"): "Authenticated UDP status recovered.",
    }
    for rows in responses:
        assert len(rows) == 5
        sensitive = [row for row in rows if row["rule"] != "other"]
        for row in sensitive:
            assert row["detail"] == expected[(row["rule"], row["severity"])]
            assert "19999" not in str(row) and "/proc/4242" not in str(row)
            assert "8192" not in str(row) and "/private" not in str(row)
        other = next(row for row in rows if row["rule"] == "other")
        assert other["detail"] == "Other detail unchanged"
    assert state.recent_alerts(limit=10)[0].detail == old.detail
    assert state.recent_alerts(limit=10)[1].detail == events[0].detail
    assert {row.detail for row in storage.load_alerts_range(limit=10)} == {
        old.detail,
        *(item.detail for item in events),
    }
    storage.close()


def test_unknown_severity_is_neutral_and_original_remains_intact():
    original = {"rule": "probe:fd_limits", "severity": ["invalid"], "detail": "/private"}
    public = public_alert(original)
    assert public["detail"] == "File descriptor limits status update."
    assert original["detail"] == "/private"
