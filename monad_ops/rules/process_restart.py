"""Service-restart detector.

Fires WARN when a tracked systemd unit's ``InvocationID`` changes between
polls — the unit restarted (operator-triggered or systemd auto-restart
after a crash). Distinct from the existing infrastructure: ``StallRule``
fires on block-cadence gap (30 s after a real chain-side stop);
``probe:services`` checks current activity (returns "active" across an
entire uptime envelope, can't tell you the service just restarted).
This rule pings the operator AT THE MOMENT of restart so they know
what's happening before downstream signals catch up.

Severity: WARN. A restart isn't the same as a chain incident — could be
operator-triggered (planned upgrade), auto-restart on a transient
crash, or a real assertion failure. CRITICAL is reserved for sustained
chain-impact rules. The detail line carries enough context (service,
new InvocationID, sub_state) for the operator to decide whether it
needs follow-up.

First-sight policy: silent. Bootstrap of the rule must not page the
operator on monad-ops's own startup. The first time we see a service,
we just record its current InvocationID and wait for the next sample.

Probe-error policy: soft-ignore. If the systemctl call timed out or
returned non-zero, leave state untouched. Mirrors the
``ReferenceLagRule`` pattern — same lesson from the 2026-04-20 event-
loop freeze that produced phantom probe-services criticals.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from monad_ops.collector.process_restart import InvocationSnapshot
from monad_ops.rules.events import AlertEvent, Severity


@dataclass(slots=True)
class ProcessRestartRule:
    """Tracks one InvocationID per service and fires on any change.

    No ``RECOVERED`` event — a restart is a point event, not an envelope
    (matches the same convention as ``ReorgRule``).
    """

    # Maps service name → last-seen InvocationID. First-sight events
    # silently populate; subsequent changes fire.
    _last: dict[str, str] = field(default_factory=dict)

    def on_snapshot(self, snap: InvocationSnapshot) -> AlertEvent | None:
        # Probe failure: leave state untouched, soft-ignore.
        if snap.error is not None:
            return None
        if snap.invocation_id is None:
            # No InvocationID returned — unit may not exist or systemctl
            # didn't render the property. Treat like a probe failure.
            return None

        previous = self._last.get(snap.service)
        if previous is None:
            # First-sight: record silently.
            self._last[snap.service] = snap.invocation_id
            return None

        if previous == snap.invocation_id:
            return None

        # Restart detected.
        self._last[snap.service] = snap.invocation_id
        return AlertEvent(
            rule="process_restart",
            severity=Severity.WARN,
            # Per-service key so concurrent restarts on different units
            # don't collide in the deduping sink.
            key=f"process_restart:{snap.service}",
            title=f"Service restart detected: {snap.service}",
            detail=(
                f"{snap.service} InvocationID changed: {_short(previous)} "
                f"→ {_short(snap.invocation_id)}. "
                f"Current state: {snap.active_state}/{snap.sub_state}. "
                "Operator-triggered restart, systemd auto-restart, or "
                "crash recovery — check journal if unexpected."
            ),
        )


def _short(invocation_id: str) -> str:
    if len(invocation_id) <= 12:
        return invocation_id
    return f"{invocation_id[:8]}…{invocation_id[-4:]}"
