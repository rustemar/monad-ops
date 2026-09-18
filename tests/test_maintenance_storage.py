"""The window's home in state.db: one transaction per side, so the CLI and
the service's flush never interleave half-way."""

from __future__ import annotations

from monad_ops.rules.events import AlertEvent, Severity
from monad_ops.storage import Storage


def _alert(rule: str, sev: Severity, key: str) -> AlertEvent:
    return AlertEvent(rule=rule, severity=sev, key=key, title="t", detail="d")


def test_open_keeps_since_while_open_and_restarts_it_after_expiry(tmp_path) -> None:
    st = Storage(tmp_path / "s.db")
    st.open_maintenance(2000.0, now_sec=1000.0)
    st.open_maintenance(3000.0, now_sec=1500.0)  # extend while open
    assert st.maintenance_window() == (1000.0, 3000.0)
    st.open_maintenance(5000.0, now_sec=4000.0)  # previous window has passed
    assert st.maintenance_window() == (4000.0, 5000.0)
    st.close()


def test_take_closed_window_is_claimed_once(tmp_path) -> None:
    st = Storage(tmp_path / "s.db")
    st.open_maintenance(2000.0, now_sec=1000.0)
    assert st.take_closed_window(1999.0) is None  # still open
    assert st.take_closed_window(2000.0) == (1000.0, 2000.0)
    assert st.take_closed_window(2001.0) is None  # already taken
    assert st.maintenance_window() == (None, 2000.0)
    # Re-opening after a take starts a fresh window, never a headless one.
    st.open_maintenance(4000.0, now_sec=3000.0)
    assert st.maintenance_window() == (3000.0, 4000.0)
    st.close()


def test_alerts_between_recovered_envelopes_and_last_severity(tmp_path) -> None:
    st = Storage(tmp_path / "s.db")
    st.write_alert(_alert("stall", Severity.CRITICAL, "stall:critical"), ts=100.0)
    st.write_alert(_alert("stall", Severity.RECOVERED, "stall"), ts=200.0)
    st.write_alert(
        _alert("service_failure", Severity.CRITICAL, "service_failure:monad-rpc"), ts=300.0
    )
    st.write_alert(_alert("stall", Severity.WARN, "stall:warn"), ts=400.0)

    assert [r[3] for r in st.alerts_between(150.0, 400.0)] == [
        "stall", "service_failure:monad-rpc", "stall:warn"]
    assert [r[3] for r in st.alerts_between(150.0, 400.0, before=400.0)] == [
        "stall", "service_failure:monad-rpc"]
    assert st.recovered_envelopes() == {"stall"}
    assert st.last_severity_before("stall", 250.0) == "recovered"
    assert st.last_severity_before("stall", 450.0) == "warn"
    assert st.last_severity_before("service_failure:monad-rpc", 350.0) == "critical"
    assert st.last_severity_before("reference_lag", 450.0) is None
    st.close()
