"""Public-safe formatting for alert payloads, without mutating stored alerts."""

from __future__ import annotations


def public_alert(alert: dict) -> dict:
    """Hide host-specific probe details in public alert responses.

    Old stored alerts and new live alerts both use the same formatter.
    Keep severity and unrelated alerts unchanged. Internal state and the
    alert delivery path retain their original diagnostic detail.
    """
    prefixes = {
        "probe:disk_usage": "Disk usage",
        "probe:fd_limits": "File descriptor limits",
        "probe:udp_config": "Authenticated UDP status",
    }
    rule = alert.get("rule")
    prefix = prefixes.get(rule) if isinstance(rule, str) else None
    if prefix is None:
        return alert
    severity = alert.get("severity")
    suffix = {
        "warn": "warning.",
        "critical": "critical.",
        "recovered": "recovered.",
    }.get(severity, "status update.") if isinstance(severity, str) else "status update."
    detail = f"{prefix} {suffix}"
    return {**alert, "detail": detail}
