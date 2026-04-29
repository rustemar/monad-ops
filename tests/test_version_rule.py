"""Unit tests for VersionRule transitions + reminder cadence."""

from __future__ import annotations

from monad_ops.collector.version import VersionStatus
from monad_ops.rules import Severity, VersionRule


def _status(
    installed: str,
    latest: str | None,
    status: str = "update_available",
) -> VersionStatus:
    if latest is not None and status == "update_available":
        extras: tuple[str, ...] = (latest,)
    else:
        extras = ()
    return VersionStatus(
        package="monad",
        installed=installed,
        latest=latest,
        extras_newer=extras,
        status=status,
        error=None,
    )


def _unknown() -> VersionStatus:
    return VersionStatus(
        package="monad", installed=None, latest=None,
        extras_newer=(), status="unknown", error="probe blip",
    )


def test_first_seen_update_emits_info():
    rule = VersionRule(reminder_interval_sec=24 * 3600)
    ev = rule.on_status(_status("0.14.1", "0.14.2"), now_sec=1000.0)
    assert ev is not None
    assert ev.severity is Severity.INFO
    assert ev.rule == "version_watch"
    assert "0.14.1" in ev.detail and "0.14.2" in ev.detail


def test_repeat_within_reminder_window_silent():
    rule = VersionRule(reminder_interval_sec=24 * 3600)
    rule.on_status(_status("0.14.1", "0.14.2"), now_sec=1000.0)
    # 12h later — under the 24h reminder gate, no event.
    ev = rule.on_status(_status("0.14.1", "0.14.2"), now_sec=1000.0 + 12 * 3600)
    assert ev is None


def test_reminder_fires_after_interval():
    rule = VersionRule(reminder_interval_sec=24 * 3600)
    rule.on_status(_status("0.14.1", "0.14.2"), now_sec=1000.0)
    ev = rule.on_status(_status("0.14.1", "0.14.2"), now_sec=1000.0 + 24 * 3600 + 1)
    assert ev is not None
    assert ev.severity is Severity.INFO
    assert "outstanding" in ev.title.lower() or "still" in ev.title.lower()


def test_newer_release_replaces_outstanding():
    rule = VersionRule(reminder_interval_sec=24 * 3600)
    first = rule.on_status(_status("0.14.1", "0.14.2"), now_sec=1000.0)
    second = rule.on_status(_status("0.14.1", "0.14.3"), now_sec=1000.0 + 60)
    assert first is not None and second is not None
    # Both fire because the latest changed; the second is a fresh
    # "new release available" not a reminder.
    assert "0.14.3" in second.detail


def test_upgrade_emits_recovered_and_clears_state():
    rule = VersionRule(reminder_interval_sec=24 * 3600)
    rule.on_status(_status("0.14.1", "0.14.2"), now_sec=1000.0)
    # Operator upgrades; next tick installed advanced to 0.14.2.
    ev = rule.on_status(_status("0.14.2", "0.14.2", status="up_to_date"), now_sec=1500.0)
    assert ev is not None
    assert ev.severity is Severity.RECOVERED
    assert "0.14.1" in ev.detail and "0.14.2" in ev.detail
    # After RECOVERED, the next up_to_date tick is silent.
    silent = rule.on_status(_status("0.14.2", "0.14.2", status="up_to_date"), now_sec=2000.0)
    assert silent is None


def test_unknown_status_is_soft_ignored():
    """A repo blip must not flap the rule state."""
    rule = VersionRule(reminder_interval_sec=24 * 3600)
    fired = rule.on_status(_status("0.14.1", "0.14.2"), now_sec=1000.0)
    assert fired is not None
    silent = rule.on_status(_unknown(), now_sec=1100.0)
    assert silent is None
    # And the next valid tick within the reminder window stays silent —
    # the unknown didn't reset state.
    silent2 = rule.on_status(_status("0.14.1", "0.14.2"), now_sec=1200.0)
    assert silent2 is None


def test_cold_start_up_to_date_silent():
    rule = VersionRule()
    ev = rule.on_status(_status("0.14.2", "0.14.2", status="up_to_date"), now_sec=1000.0)
    assert ev is None


def test_reminder_key_changes_per_day():
    """Reminder key must vary between firings so the deduping sink does
    not collapse two reminders into one. The build_event helper encodes
    the day-bucket into the key — verify it changes across a 25h gap.
    """
    rule = VersionRule(reminder_interval_sec=24 * 3600)
    rule.on_status(_status("0.14.1", "0.14.2"), now_sec=1000.0)
    r1 = rule.on_status(_status("0.14.1", "0.14.2"), now_sec=1000.0 + 25 * 3600)
    r2 = rule.on_status(_status("0.14.1", "0.14.2"), now_sec=1000.0 + 50 * 3600)
    assert r1 is not None and r2 is not None
    assert r1.key != r2.key
