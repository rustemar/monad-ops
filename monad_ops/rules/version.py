"""Version-availability rule.

Consumes ``VersionStatus`` snapshots from ``collector/version.py`` and
emits transition-shaped ``AlertEvent``s on a 24-hour reminder cadence
while an upgrade is outstanding.

Why this is a separate rule rather than another probe with the standard
``probe_loop`` auto-emit path:

  * The standard probe loop fires WARN/CRITICAL on every tick while a
    probe is non-ok; cooldown dedup is the only suppressor. For a new
    package release, an operator wants ONE alert when the version
    appears + a daily reminder while still outstanding, not a steady
    drumbeat suppressed only by cooldown.
  * Transitions go through INFO + RECOVERED — both map to GREEN under
    the Foundation colour-code framework. A new release is informational
    for normal-cadence operators; if Foundation flags it ORANGE/RED in
    their own announce, that arrives via #fullnode-announcements
    independently.
"""

from __future__ import annotations

import time
from dataclasses import dataclass

from monad_ops.collector.version import VersionStatus
from monad_ops.rules.events import AlertEvent, Severity


@dataclass(slots=True)
class VersionRule:
    reminder_interval_sec: int = 24 * 3600

    # State.
    _last_alerted_version: str | None = None     # the upstream version we last fired NEW for
    _last_reminder_ts: float = 0.0
    _last_seen_installed: str | None = None       # what we observed last tick

    def on_status(
        self,
        status: VersionStatus,
        *,
        now_sec: float | None = None,
    ) -> AlertEvent | None:
        now = now_sec if now_sec is not None else time.time()

        # Cannot evaluate — soft-ignore so a transient repo blip does
        # not flap the rule. State left untouched.
        if status.status == "unknown" or status.installed is None:
            return None

        installed = status.installed

        # Detect "operator upgraded since last tick". Fires once when the
        # installed version moves forward to clear an outstanding alert.
        if (
            self._last_seen_installed is not None
            and self._last_seen_installed != installed
            and self._last_alerted_version is not None
            and not self._is_lower(installed, self._last_alerted_version)
        ):
            event = AlertEvent(
                rule="version_watch",
                severity=Severity.RECOVERED,
                key=f"version_watch:upgraded:{installed}",
                title="Node upgraded",
                detail=(
                    f"Local {status.package} moved "
                    f"{self._last_seen_installed} → {installed}. "
                    f"Pending update reminder cleared."
                ),
            )
            self._last_seen_installed = installed
            self._last_alerted_version = None
            self._last_reminder_ts = 0.0
            return event

        self._last_seen_installed = installed

        if status.status == "up_to_date":
            # Operator might have upgraded mid-window without us
            # observing the previous installed value (cold start).
            # Clear any stale outstanding marker silently.
            if self._last_alerted_version is not None:
                self._last_alerted_version = None
                self._last_reminder_ts = 0.0
            return None

        if status.status != "update_available" or status.latest is None:
            return None

        latest = status.latest

        # New version we've never alerted on (or a newer-than-pending one).
        if self._last_alerted_version != latest:
            self._last_alerted_version = latest
            self._last_reminder_ts = now
            return self._build_event(status, is_reminder=False, now_sec=now)

        # Same outstanding version — daily reminder if interval elapsed.
        if (now - self._last_reminder_ts) >= self.reminder_interval_sec:
            self._last_reminder_ts = now
            return self._build_event(status, is_reminder=True, now_sec=now)

        return None

    @staticmethod
    def _is_lower(a: str, b: str) -> bool:
        """Lexicographic-ish proxy for 'a < b'.

        Avoids shelling out to dpkg from inside the rule; the rule is
        called on every probe tick and should stay deterministic +
        synchronous. The check is only used as a safety net for the
        upgrade-detection branch — a false negative here just means we
        skip a RECOVERED, not a wrong escalation. The probe layer has
        already done the authoritative comparison.
        """
        return a < b

    @staticmethod
    def _build_event(
        status: VersionStatus,
        *,
        is_reminder: bool,
        now_sec: float,
    ) -> AlertEvent:
        installed = status.installed or "?"
        latest = status.latest or "?"
        extras = list(status.extras_newer)
        # Trim the extras list defensively; the Telegram body has a
        # human-readable cap and the dashboard has its own popup view.
        if len(extras) > 6:
            extras = extras[:6]
        extras_part = ", ".join(extras) if extras else "—"

        title = (
            "New monad release available"
            if not is_reminder
            else "Monad release still outstanding"
        )
        # Severity: INFO for both first-fire and reminder (GREEN).
        # The single-tick fire vs. daily-reminder distinction is
        # carried in the alert key so the deduping sink does not
        # collapse a reminder into the original announcement. Reminder
        # keys are bucketed by day-of-now-sec so each daily reminder is
        # a distinct event for dedup purposes; using ``now_sec`` (not
        # real time) keeps the key deterministic in tests.
        key = (
            f"version_watch:available:{latest}"
            if not is_reminder
            else f"version_watch:reminder:{latest}:{int(now_sec) // 86400}"
        )
        detail = (
            f"{status.package} {installed} → {latest} available "
            f"in apt repo. Newer versions: {extras_part}."
        )
        return AlertEvent(
            rule="version_watch",
            severity=Severity.INFO,
            key=key,
            title=title,
            detail=detail,
        )
