"""Shared alert-event types emitted by rules."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class Severity(StrEnum):
    INFO = "info"
    WARN = "warn"
    CRITICAL = "critical"
    RECOVERED = "recovered"


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
