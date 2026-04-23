"""Shared alert-event types emitted by rules."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class Severity(StrEnum):
    INFO = "info"
    WARN = "warn"
    CRITICAL = "critical"
    RECOVERED = "recovered"


class CodeColor(StrEnum):
    """Monad Foundation colour-code framework (Jackson, 2026-03-26).

    GREEN = informational / no action. ORANGE = action recommended within
    24–48 h. RED = action recommended immediately. The framework is used
    by Foundation for network-wide announcements (subject prefixes in
    email, Discord ``#*-fullnode-announcements``, TG Node Announcements).

    Emitting the same vocabulary alongside each local alert lets an
    operator read their own node's events in the same language as the
    Foundation announcements they already subscribe to.
    """
    GREEN = "green"
    ORANGE = "orange"
    RED = "red"


# One mapping, one place. INFO and RECOVERED are informational — the
# operator doesn't need to act on either. WARN maps to ORANGE (action
# recommended, not urgent). CRITICAL maps to RED (immediate attention).
_SEVERITY_TO_CODE: dict[Severity, CodeColor] = {
    Severity.INFO: CodeColor.GREEN,
    Severity.RECOVERED: CodeColor.GREEN,
    Severity.WARN: CodeColor.ORANGE,
    Severity.CRITICAL: CodeColor.RED,
}


def code_color_for(severity: Severity) -> CodeColor:
    """Map an internal Severity to the Foundation colour-code language."""
    return _SEVERITY_TO_CODE[severity]


@dataclass(frozen=True, slots=True)
class AlertEvent:
    """A single alert emitted by a rule.

    ``key`` is used for deduplication — identical keys within a cooldown
    window are suppressed downstream.
    """
    rule: str
    severity: Severity
    key: str
    title: str
    detail: str
