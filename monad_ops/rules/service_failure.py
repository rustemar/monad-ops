"""Systemd-level failure detector for the node services.

Every other rule in monad-ops reads the node's own output: the exec and
bft journals, the RPC, the enricher's counters. All of them assume the
node got far enough to produce output. The failures that hurt most are
the ones where it didn't — a bad config after an upgrade, a triedb the
binary refuses to open, ``io_uring_queue_init_params`` returning
``Invalid argument`` on a kernel that lost the sysctl, an OOM kill, a
core dump. The tailer sees nothing, so the stall rule takes its usual
30 s to notice the chain went quiet and reports it as a chain-side
stall, which is the wrong diagnosis and the wrong first action.

Systemd knows immediately, and it is cheap to ask. ``ProcessRestartRule``
already polls ``systemctl show`` for the ``InvocationID``; this rule
reads three more properties out of the same sample and answers a
different question. The restart rule says *it restarted*. This one says
*it fell over*, and what systemd thinks killed it.

Two conditions, deliberately shaped differently:

  * **The unit is down.** ``ActiveState`` is ``failed`` — systemd gave
    up, either on a start that never came up or after the restart limit.
    Nothing is coming back on its own. This is an envelope: it stays
    open until the unit is active again, and closes with one RECOVERED.
  * **The unit crashed and came back.** ``NRestarts`` moved. That
    counter only advances on a systemd auto-restart *after a failure*;
    a planned ``systemctl restart`` leaves it alone. So a bump means the
    process died on its own, even if the unit reads ``active`` again by
    the time we look. This one is a point event, like ``reorg`` and
    ``process_restart``: by the time we can report it the service is
    already back, so there is no state to recover from and a paired
    RECOVERED would be noise.

CRITICAL for both, unlike the WARN of ``process_restart``: a node
process that died is a chain-impact event for this operator, and the
crash case is exactly the one the restart rule cannot separate from a
planned upgrade. ``Result``, ``ExecMainStatus`` and ``ExecMainCode``
ride along in the detail line so the first question — signal, exit
code, or OOM — is answered without opening the journal.

Quiet-by-default policies, matching the rest of the ruleset:

  * **First sight takes a baseline for the counter, but not for the
    state.** ``NRestarts`` needs a previous value to have a delta, so
    the first sample only records it. A unit that is already ``failed``
    when monad-ops starts fires straight away — that is a node that is
    down right now, and staying silent about it to avoid a startup page
    would be the wrong trade.
  * **Probe errors are soft-ignored.** A timed-out or non-zero
    ``systemctl`` leaves state untouched, same as the restart rule. The
    2026-04-20 event-loop freeze produced seven phantom ``probe:services``
    criticals; nothing here should be able to repeat that.
  * **One RECOVERED per envelope.** Only the down envelope has one, and
    only the sample that finds the unit active again emits it.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from monad_ops.collector.process_restart import InvocationSnapshot
from monad_ops.rules.events import AlertEvent, Severity

# systemd's word for a healthy last run. Anything else in ``Result`` is
# a failure mode worth naming in the alert.
_RESULT_OK = "success"


@dataclass(slots=True)
class _ServiceState:
    """What we remember between samples, per unit."""
    n_restarts: int | None = None
    armed: bool = False


@dataclass(slots=True)
class ServiceFailureRule:
    """Per-service failure envelope over systemd unit properties."""

    _state: dict[str, _ServiceState] = field(default_factory=dict)

    def on_snapshot(self, snap: InvocationSnapshot) -> AlertEvent | None:
        # No information — do not touch state, do not emit.
        if snap.error is not None:
            return None

        state = self._state.get(snap.service)
        if state is None:
            state = _ServiceState()
            self._state[snap.service] = state

        crashed = (
            state.n_restarts is not None
            and snap.n_restarts is not None
            and snap.n_restarts > state.n_restarts
        )
        # Baseline the counter on every sample: once a crash is reported
        # the new value is the reference for the next one, so a single
        # crash cannot re-fire on every poll.
        state.n_restarts = snap.n_restarts

        down = snap.active_state == "failed"

        if down:
            if state.armed:
                # Already inside the envelope. Re-firing on every poll is
                # how a down node turns into a pager loop.
                return None
            state.armed = True
            return AlertEvent(
                rule="service_failure",
                severity=Severity.CRITICAL,
                key=f"service_failure:{snap.service}",
                title=f"Service down: {snap.service}",
                detail=_detail(snap, down=True),
            )

        if crashed and not state.armed:
            # Point event: the unit is already back up. Suppressed while
            # a down envelope is open, where the restart is part of the
            # same incident and will be reported by its RECOVERED.
            return AlertEvent(
                rule="service_failure",
                severity=Severity.CRITICAL,
                key=f"service_failure:{snap.service}",
                title=f"Service crashed and restarted: {snap.service}",
                detail=_detail(snap, down=False),
            )

        if state.armed and snap.active_state == "active":
            state.armed = False
            return AlertEvent(
                rule="service_failure",
                severity=Severity.RECOVERED,
                key=f"service_failure:{snap.service}",
                title=f"Service healthy again: {snap.service}",
                detail=(
                    f"{snap.service} is {snap.active_state}/{snap.sub_state} "
                    f"with no new failures since the alert "
                    f"(NRestarts={snap.n_restarts}). Systemd reports "
                    f"Result={snap.result or 'unknown'}."
                ),
            )

        return None


def _detail(snap: InvocationSnapshot, *, down: bool) -> str:
    """One line an operator can act on without opening the journal."""
    reason = snap.result if snap.result and snap.result != _RESULT_OK else None
    parts = [
        f"{snap.service} is {snap.active_state or 'unknown'}/"
        f"{snap.sub_state or 'unknown'}."
    ]
    if reason is not None:
        parts.append(f"Systemd result: {reason}.")
    if snap.exec_main_code or snap.exec_main_status:
        parts.append(
            f"Main process {snap.exec_main_code or 'exited'} with status "
            f"{snap.exec_main_status}."
        )
    if snap.n_restarts:
        parts.append(f"Auto-restarts since last clean start: {snap.n_restarts}.")
    parts.append(
        "Systemd gave up — the process is not coming back without you."
        if down
        else "Systemd restarted it; the exit was not planned."
    )
    return " ".join(parts)
