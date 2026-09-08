"""Unit tests for rules/service_failure.py — the systemd-level detector.

Snapshots are built by hand rather than shelled out, same as the rest of
the rule suite: what is under test is the state machine, not systemctl.
"""

from __future__ import annotations

from monad_ops.collector.process_restart import InvocationSnapshot
from monad_ops.rules.events import Severity
from monad_ops.rules.service_failure import ServiceFailureRule


def _snap(
    *,
    service: str = "monad-bft",
    active_state: str | None = "active",
    sub_state: str | None = "running",
    result: str | None = "success",
    n_restarts: int | None = 0,
    exec_main_status: int | None = 0,
    exec_main_code: str | None = None,
    error: str | None = None,
) -> InvocationSnapshot:
    return InvocationSnapshot(
        service=service,
        invocation_id="f60018c6ddcb4c64bc703e8ecde3b7f6",
        sub_state=sub_state,
        active_state=active_state,
        error=error,
        result=result,
        n_restarts=n_restarts,
        exec_main_status=exec_main_status,
        exec_main_code=exec_main_code,
    )


def test_healthy_service_is_silent():
    rule = ServiceFailureRule()
    assert rule.on_snapshot(_snap()) is None
    assert rule.on_snapshot(_snap()) is None


def test_first_sight_of_a_healthy_service_does_not_page():
    """Our own startup must not alert. The counter is only baselined."""
    rule = ServiceFailureRule()
    assert rule.on_snapshot(_snap(n_restarts=7)) is None


def test_first_sight_of_a_down_service_fires_immediately():
    """A unit already failed when monad-ops starts is a node that is
    down right now — silence would be the wrong trade."""
    rule = ServiceFailureRule()
    ev = rule.on_snapshot(
        _snap(
            active_state="failed",
            sub_state="failed",
            result="core-dump",
            exec_main_code="dumped",
            exec_main_status=6,
        )
    )
    assert ev is not None
    assert ev.severity is Severity.CRITICAL
    assert ev.rule == "service_failure"
    assert ev.title == "Service down: monad-bft"
    assert "core-dump" in ev.detail
    assert "dumped" in ev.detail


def test_crash_restart_fires_even_though_the_unit_reads_active():
    """NRestarts only moves on an auto-restart after failure, so a bump
    means the process died on its own between polls."""
    rule = ServiceFailureRule()
    rule.on_snapshot(_snap(n_restarts=0))
    ev = rule.on_snapshot(_snap(n_restarts=1, result="signal"))
    assert ev is not None
    assert ev.severity is Severity.CRITICAL
    assert ev.title == "Service crashed and restarted: monad-bft"
    assert "not planned" in ev.detail


def test_a_single_crash_fires_once_and_has_no_recovered():
    """The counter stays elevated after the restart, so the crash must
    not re-fire; and since the unit is already back there is nothing to
    recover from — a paired RECOVERED would just be noise."""
    rule = ServiceFailureRule()
    rule.on_snapshot(_snap(n_restarts=0))
    assert rule.on_snapshot(_snap(n_restarts=1)) is not None
    assert rule.on_snapshot(_snap(n_restarts=1)) is None
    assert rule.on_snapshot(_snap(n_restarts=1)) is None


def test_down_service_does_not_re_fire_while_it_stays_down():
    rule = ServiceFailureRule()
    down = _snap(active_state="failed", sub_state="failed", result="exit-code")
    assert rule.on_snapshot(down) is not None
    assert rule.on_snapshot(down) is None
    assert rule.on_snapshot(down) is None


def test_recovery_emits_exactly_one_recovered():
    rule = ServiceFailureRule()
    rule.on_snapshot(_snap(active_state="failed", sub_state="failed"))
    ev = rule.on_snapshot(_snap())
    assert ev is not None
    assert ev.severity is Severity.RECOVERED
    assert ev.title == "Service healthy again: monad-bft"
    assert rule.on_snapshot(_snap()) is None


def test_activating_is_not_yet_a_recovery():
    """A unit in auto-restart backoff is on its way somewhere; the
    envelope stays open until it is actually active."""
    rule = ServiceFailureRule()
    rule.on_snapshot(_snap(active_state="failed", sub_state="failed"))
    assert rule.on_snapshot(
        _snap(active_state="activating", sub_state="auto-restart")
    ) is None
    assert rule.on_snapshot(_snap()) is not None


def test_crash_during_a_down_envelope_is_part_of_the_same_incident():
    """A unit flapping in auto-restart before systemd gives up should
    produce one alert, not one per bounce."""
    rule = ServiceFailureRule()
    rule.on_snapshot(_snap(n_restarts=0))
    ev = rule.on_snapshot(
        _snap(active_state="failed", sub_state="failed", n_restarts=3)
    )
    assert ev is not None
    assert ev.title == "Service down: monad-bft"
    assert rule.on_snapshot(
        _snap(active_state="failed", sub_state="failed", n_restarts=4)
    ) is None
    ev = rule.on_snapshot(_snap(n_restarts=4))
    assert ev is not None
    assert ev.severity is Severity.RECOVERED


def test_probe_error_leaves_state_untouched():
    """A systemctl timeout is no information. It must not arm, must not
    disarm, and must not shift the crash baseline."""
    rule = ServiceFailureRule()
    rule.on_snapshot(_snap(n_restarts=3))
    assert rule.on_snapshot(
        _snap(active_state=None, n_restarts=None, error="rc=1: timeout")
    ) is None
    # The baseline survived, so the next real sample still sees the jump.
    ev = rule.on_snapshot(_snap(n_restarts=4))
    assert ev is not None
    assert ev.severity is Severity.CRITICAL


def test_missing_counter_property_never_reads_as_a_crash():
    """None means "no information", not zero — a unit whose NRestarts we
    cannot read must not manufacture a delta."""
    rule = ServiceFailureRule()
    assert rule.on_snapshot(_snap(n_restarts=None)) is None
    assert rule.on_snapshot(_snap(n_restarts=None)) is None
    assert rule.on_snapshot(_snap(n_restarts=0)) is None


def test_each_service_keeps_its_own_envelope():
    rule = ServiceFailureRule()
    rule.on_snapshot(_snap(service="monad-bft"))
    rule.on_snapshot(_snap(service="monad-execution"))
    ev = rule.on_snapshot(_snap(service="monad-bft", n_restarts=1))
    assert ev is not None
    assert ev.key == "service_failure:monad-bft"
    assert rule.on_snapshot(_snap(service="monad-execution")) is None
