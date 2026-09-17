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
from typing import Any

from monad_ops.collector.version import VersionStatus
from monad_ops.rules.events import AlertEvent, Severity


@dataclass(slots=True)
class VersionRule:
    reminder_interval_sec: int = 24 * 3600

    # State. Must be persisted across process restarts (caller loads
    # via load_state / saves via to_state) — a fresh in-memory rule on
    # every monad-ops restart would re-fire the first-time alert for an
    # already-known release, spamming Telegram. See feedback_no_per_tx_…
    # style operator complaint 2026-05-19 (three identical alerts in
    # 43 minutes from three service restarts in the same session).
    _last_alerted_version: str | None = None     # the upstream version we last fired NEW for
    _last_reminder_ts: float = 0.0
    _last_seen_installed: str | None = None       # what we observed last tick
    # When the pending release first showed up in the repo. A package can
    # sit in apt for days before it is announced, and "how long has it been
    # there" is what the operator weighs against waiting for the announce.
    _pending_version: str | None = None
    _pending_since_ts: float = 0.0

    def to_state(self) -> dict[str, Any]:
        """Serialize fields the caller must persist across restarts."""
        return {
            "last_alerted_version": self._last_alerted_version,
            "last_reminder_ts": self._last_reminder_ts,
            "last_seen_installed": self._last_seen_installed,
            "pending_version": self._pending_version,
            "pending_since_ts": self._pending_since_ts,
        }

    def load_state(self, state: dict[str, Any]) -> None:
        """Restore from a previously-saved to_state() dict. Ignores
        unknown keys and tolerates missing fields (treated as default)."""
        if not isinstance(state, dict):
            return
        lav = state.get("last_alerted_version")
        self._last_alerted_version = str(lav) if lav else None
        try:
            self._last_reminder_ts = float(state.get("last_reminder_ts") or 0.0)
        except (TypeError, ValueError):
            self._last_reminder_ts = 0.0
        lsi = state.get("last_seen_installed")
        self._last_seen_installed = str(lsi) if lsi else None
        pv = state.get("pending_version")
        self._pending_version = str(pv) if pv else None
        try:
            self._pending_since_ts = float(state.get("pending_since_ts") or 0.0)
        except (TypeError, ValueError):
            self._pending_since_ts = 0.0

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
            self._pending_version = None
            self._pending_since_ts = 0.0
            return event

        self._last_seen_installed = installed

        if status.status == "up_to_date":
            # Operator might have upgraded mid-window without us
            # observing the previous installed value (cold start).
            # Clear any stale outstanding marker silently.
            if self._last_alerted_version is not None:
                self._last_alerted_version = None
                self._last_reminder_ts = 0.0
            self._pending_version = None
            self._pending_since_ts = 0.0
            return None

        if status.status != "update_available" or status.latest is None:
            return None

        latest = status.latest

        # Start the clock the first tick this release is visible, not the
        # first tick we alert on it: a restart before the alert would
        # otherwise reset the age.
        if self._pending_version != latest:
            self._pending_version = latest
            self._pending_since_ts = now

        # New version we've never alerted on (or a newer-than-pending one).
        if self._last_alerted_version != latest:
            self._last_alerted_version = latest
            self._last_reminder_ts = now
            return self._build_event(
                status, is_reminder=False, now_sec=now,
                pending_since_sec=self._pending_since_ts,
            )

        # Same outstanding version — daily reminder if interval elapsed.
        if (now - self._last_reminder_ts) >= self.reminder_interval_sec:
            self._last_reminder_ts = now
            return self._build_event(
                status, is_reminder=True, now_sec=now,
                pending_since_sec=self._pending_since_ts,
            )

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
    def _fmt_age(seconds: float) -> str:
        total_min = int(max(0.0, seconds) // 60)
        if total_min < 60:
            return f"{total_min}m"
        hours, minutes = divmod(total_min, 60)
        if hours < 24:
            return f"{hours}h {minutes}m"
        days, hours = divmod(hours, 24)
        return f"{days}d {hours}h"

    @staticmethod
    def _build_event(
        status: VersionStatus,
        *,
        is_reminder: bool,
        now_sec: float,
        pending_since_sec: float = 0.0,
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
        # The reminder is the one an operator reads while deciding whether
        # to wait: it carries how long the package has been sitting there.
        if is_reminder and pending_since_sec > 0.0:
            age = VersionRule._fmt_age(now_sec - pending_since_sec)
            availability = (
                f"{status.package} {installed} → {latest} has been in the "
                f"apt repo for {age}."
            )
        else:
            availability = (
                f"{status.package} {installed} → {latest} available "
                f"in apt repo."
            )
        detail = (
            f"{availability} Newer versions: {extras_part}. "
            f"Upgrade once the release is announced — a package in apt "
            f"is not the announcement."
        )
        return AlertEvent(
            rule="version_watch",
            severity=Severity.INFO,
            key=key,
            title=title,
            detail=detail,
        )
