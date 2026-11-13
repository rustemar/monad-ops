"""Public-safe formatting for alert payloads, without mutating stored alerts."""

from __future__ import annotations


def public_alert(alert: dict) -> dict:
    """Hide host-specific disk probe details in public alert responses.

    Old stored alerts and new live alerts both use the same formatter.
    Keep severity and all non-disk alerts unchanged. Internal state and
    the alert delivery path retain their original diagnostic detail.
    """
    if alert.get("rule") != "probe:disk_usage":
        return alert
    detail = {
        "warn": "Disk usage warning.",
        "critical": "Disk usage critical.",
        "recovered": "Disk usage recovered.",
    }.get(alert.get("severity"), "Disk usage status update.")
    return {**alert, "detail": detail}
