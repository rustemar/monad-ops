import pytest

from monad_ops.collector import probes
from monad_ops.rules.probe_alerts import ProbeAlertRule


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "rc, output",
    [
        (1, "UNCONN 0 0 127.0.0.1:8001 0.0.0.0:*"),
        (127, "UNCONN 0 0 127.0.0.1:8001 0.0.0.0:*"),
        (0, "UNCONN 0 0 127.0.0.1:80010 0.0.0.0:*"),
        (0, "UNCONN 0 0 127.0.0.1:1234 192.0.2.1:8001"),
        (0, "ESTAB 0 0 127.0.0.1:8001 192.0.2.1:1234"),
        (0, "error while examining UDP port :8001"),
        (0, ""),
    ],
)
async def test_udp_fallback_requires_observed_local_socket(tmp_path, monkeypatch, rc, output):
    monkeypatch.setattr(probes, "_MONAD_CONFIG", tmp_path / "missing.toml")

    async def run(command):
        return rc, output, ""

    monkeypatch.setattr(probes, "_run", run)
    result = await probes.probe_udp_config()
    assert result.status == "unknown"
    alerts = ProbeAlertRule({"udp_config": "warn"})
    assert alerts.evaluate([result]) == []
    assert alerts._prev["udp_config"] == "warn"


@pytest.mark.asyncio
@pytest.mark.parametrize("address", ["0.0.0.0", "[::]", "[fe80::1]%eth0", "*"])
async def test_udp_fallback_reads_exact_local_port(tmp_path, monkeypatch, address):
    monkeypatch.setattr(probes, "_MONAD_CONFIG", tmp_path / "missing.toml")

    async def run(command):
        assert command == ["ss", "-H", "-uln"]
        return 0, f"UNCONN 0 0 {address}:8001 *:*\nUNCONN 0 0 *:8000 *:*", ""

    monkeypatch.setattr(probes, "_run", run)
    result = await probes.probe_udp_config()
    assert result.status == "ok"
    assert result.details == {"source": "socket-fallback", "auth_port": 8001, "clear_port": 8000}


@pytest.mark.asyncio
async def test_udp_fallback_reports_clear_port_only_when_locally_bound(tmp_path, monkeypatch):
    monkeypatch.setattr(probes, "_MONAD_CONFIG", tmp_path / "missing.toml")

    async def run(command):
        return 0, "UNCONN 0 0 *:8001 *:*\nUNCONN 0 0 *:80001 192.0.2.1:8000", ""

    monkeypatch.setattr(probes, "_run", run)
    result = await probes.probe_udp_config()
    assert result.status == "ok"
    assert result.details["clear_port"] is None
