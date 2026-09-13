"""rules/probe_alerts.py — one alert per envelope, closed by one RECOVERED."""

from __future__ import annotations

from monad_ops.collector.probes import ProbeResult
from monad_ops.rules.events import AlertEvent, Severity
from monad_ops.rules.probe_alerts import ProbeAlertRule, open_probe_envelopes


def _r(status: str, name: str = "stale_deploy") -> ProbeResult:
    return ProbeResult(name=name, status=status, summary=f"{name} is {status}", details={})


def _sev(events: list[AlertEvent]) -> list[str]:
    return [e.severity.value for e in events]


def test_warn_fires_once_and_recovers_once():
    rule = ProbeAlertRule()
    assert _sev(rule.evaluate([_r("ok")])) == []
    assert _sev(rule.evaluate([_r("warn")])) == ["warn"]
    assert _sev(rule.evaluate([_r("warn")])) == []
    assert _sev(rule.evaluate([_r("warn")])) == []
    assert _sev(rule.evaluate([_r("ok")])) == ["recovered"]
    assert _sev(rule.evaluate([_r("ok")])) == []


def test_escalation_and_deescalation_are_reported():
    rule = ProbeAlertRule()
    assert _sev(rule.evaluate([_r("warn")])) == ["warn"]
    assert _sev(rule.evaluate([_r("critical")])) == ["critical"]
    assert _sev(rule.evaluate([_r("critical")])) == []
    assert _sev(rule.evaluate([_r("warn")])) == ["warn"]
    assert _sev(rule.evaluate([_r("ok")])) == ["recovered"]


def test_unknown_neither_fires_nor_closes():
    rule = ProbeAlertRule()
    rule.evaluate([_r("warn")])
    assert _sev(rule.evaluate([_r("unknown")])) == []
    assert _sev(rule.evaluate([_r("ok")])) == ["recovered"]


def test_seeded_open_envelope_is_closed_after_a_restart():
    rule = ProbeAlertRule({"stale_deploy": "warn"})
    events = rule.evaluate([_r("ok")])
    assert _sev(events) == ["recovered"]
    assert events[0].key == "probe:stale_deploy"


def test_seeded_open_envelope_is_not_reopened_while_still_warm():
    rule = ProbeAlertRule({"stale_deploy": "warn"})
    assert _sev(rule.evaluate([_r("warn")])) == []


def test_key_backups_never_alerts():
    rule = ProbeAlertRule()
    assert rule.evaluate([_r("critical", name="key_backups")]) == []


def test_event_shape_matches_the_old_loop():
    rule = ProbeAlertRule()
    (ev,) = rule.evaluate([_r("warn")])
    assert (ev.rule, ev.key, ev.title) == (
        "probe:stale_deploy", "probe:stale_deploy", "Probe stale_deploy WARN",
    )
    assert ev.detail == "stale_deploy is warn"


def _ev(sev: Severity, key: str = "probe:stale_deploy") -> AlertEvent:
    return AlertEvent(rule=key, severity=sev, key=key, title="t", detail="d")


def test_open_envelopes_reads_the_last_event_per_probe():
    history = [
        _ev(Severity.WARN),
        _ev(Severity.WARN),
        _ev(Severity.CRITICAL, key="probe:services"),
        _ev(Severity.RECOVERED, key="probe:services"),
        _ev(Severity.WARN, key="stall"),
    ]
    assert open_probe_envelopes(history) == {"stale_deploy": "warn"}
