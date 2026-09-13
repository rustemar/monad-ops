"""Probe results → alerts, on transitions only.

The probe loop used to re-deliver a WARN every minute for as long as the
probe stayed warm; the Telegram sink deduped it, but the recording sink
in front of it wrote a row per minute, so the public dashboard filled
with copies of one alert (stale_deploy, 2026-09-13). One alert opens an
envelope, one RECOVERED closes it, nothing in between.
"""

from __future__ import annotations

from collections.abc import Iterable

from monad_ops.collector.probes import ProbeResult
from monad_ops.rules.events import AlertEvent, Severity

_ARMED = ("warn", "critical")

# key_backups is operator hygiene and its summary names key files, so it
# never reaches the (public) alert path.
_SKIP = frozenset({"key_backups"})


def open_probe_envelopes(alerts: Iterable[AlertEvent]) -> dict[str, str]:
    """Probe name → severity for probe alerts whose last event is still armed.

    Seeds the rule after a restart so an envelope opened by the previous
    process still gets its RECOVERED, and a probe that is still warm does
    not open a second one.
    """
    last: dict[str, str] = {}
    for a in alerts:
        if a.key.startswith("probe:"):
            last[a.key[len("probe:"):]] = a.severity.value
    return {name: sev for name, sev in last.items() if sev in _ARMED}


class ProbeAlertRule:
    def __init__(self, open_envelopes: dict[str, str] | None = None) -> None:
        self._prev: dict[str, str] = dict(open_envelopes or {})

    def evaluate(self, results: Iterable[ProbeResult]) -> list[AlertEvent]:
        out: list[AlertEvent] = []
        for r in results:
            if r.name in _SKIP:
                continue
            prev = self._prev.get(r.name)
            if r.status in _ARMED:
                if r.status != prev:
                    out.append(self._event(r, Severity(r.status), r.status.upper()))
                self._prev[r.name] = r.status
            elif r.status == "ok":
                if prev in _ARMED:
                    out.append(self._event(r, Severity.RECOVERED, "recovered"))
                self._prev[r.name] = "ok"
            # "unknown" says nothing about the probe either way: no event,
            # and the envelope (if any) stays as it was.
        return out

    @staticmethod
    def _event(r: ProbeResult, severity: Severity, word: str) -> AlertEvent:
        return AlertEvent(
            rule=f"probe:{r.name}",
            severity=severity,
            key=f"probe:{r.name}",
            title=f"Probe {r.name} {word}",
            detail=r.summary,
        )
