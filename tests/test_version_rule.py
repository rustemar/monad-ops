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


def test_reminder_carries_how_long_the_package_has_been_in_apt():
    """The reminder is what the operator reads while deciding to wait, so
    it has to say how long the release has been sitting in the repo."""
    rule = VersionRule(reminder_interval_sec=24 * 3600)
    first = rule.on_status(_status("0.16.2", "0.16.3"), now_sec=1000.0)
    assert first is not None
    # A fresh release has no age worth printing.
    assert "has been in the" not in first.detail
    assert "announced" in first.detail

    ev = rule.on_status(_status("0.16.2", "0.16.3"), now_sec=1000.0 + 50 * 3600)
    assert ev is not None
    assert "2d 2h" in ev.detail


def test_pending_age_counts_from_first_sighting_not_first_alert():
    """A restart between the sighting and the alert must not reset the
    clock: state is restored from the meta table on every start."""
    rule = VersionRule(reminder_interval_sec=24 * 3600)
    rule.on_status(_status("0.16.2", "0.16.3"), now_sec=1000.0)

    restarted = VersionRule(reminder_interval_sec=24 * 3600)
    restarted.load_state(rule.to_state())
    ev = restarted.on_status(_status("0.16.2", "0.16.3"), now_sec=1000.0 + 30 * 3600)
    assert ev is not None
    assert "1d 6h" in ev.detail


def test_a_newer_release_restarts_the_apt_clock():
    rule = VersionRule(reminder_interval_sec=24 * 3600)
    rule.on_status(_status("0.16.2", "0.16.3"), now_sec=1000.0)
    # 0.16.4 lands two days later; its age is its own, not 0.16.3's.
    rule.on_status(_status("0.16.2", "0.16.4"), now_sec=1000.0 + 48 * 3600)
    ev = rule.on_status(_status("0.16.2", "0.16.4"), now_sec=1000.0 + 73 * 3600)
    assert ev is not None
    assert "1d 1h" in ev.detail


def test_upgrade_clears_the_pending_clock():
    rule = VersionRule(reminder_interval_sec=24 * 3600)
    rule.on_status(_status("0.16.2", "0.16.3"), now_sec=1000.0)
    rule.on_status(_status("0.16.3", "0.16.3", status="up_to_date"), now_sec=2000.0)
    assert rule.to_state()["pending_version"] is None
    assert rule.to_state()["pending_since_ts"] == 0.0


def test_state_without_a_clock_reports_no_age_rather_than_a_wrong_one():
    """monad-ops upgraded while a release was already outstanding: the
    saved state has no clock, and inventing one from the restart would
    understate how long the package has been sitting there."""
    rule = VersionRule(reminder_interval_sec=24 * 3600)
    rule.load_state({
        "last_alerted_version": "0.16.3",
        "last_reminder_ts": 1000.0,
        "last_seen_installed": "0.16.2",
    })
    ev = rule.on_status(_status("0.16.2", "0.16.3"), now_sec=1000.0 + 25 * 3600)
    assert ev is not None
    assert "has been in the" not in ev.detail
    assert "announced" in ev.detail
