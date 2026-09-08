"""Periodic poll of systemd unit state per tracked service.

Used by ``ProcessRestartRule`` to detect when a service has restarted
between polls — operator-triggered or auto-restart by systemd. The
``InvocationID`` is a UUID systemd issues on each unit start; comparing
against the last-seen value is a clean change detector.

Why not ``NRestarts`` for the restart rule: that counter only increments
on systemd-driven auto-restart after failure, not on manual ``systemctl
restart``. We want to detect both. The same property is what makes it
the right signal for ``ServiceFailureRule``, which wants only the
crashes — the sample carries both so one poll feeds both rules.

Why not the existing ``probe_services`` (``systemctl is-active``):
``is-active`` returns the same string ("active") across an entire
uptime envelope; you can't tell from one sample whether the service
just restarted. ``InvocationID`` flips on every start.

The collector fails quiet on subprocess timeout / missing-binary —
mirroring the pattern in ``probes.py`` ``_run``: a 5-second journald
hiccup or a transient ``systemctl`` slowdown returns
``InvocationSnapshot(error=...)`` rather than raising. The rule then
soft-ignores that sample, leaving its state untouched. Without this,
the 2026-04-20 stress-test event-loop freeze that already caused 7
``probe:services`` false-positives would also produce phantom restart
alerts on every service.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class InvocationSnapshot:
    """One sample for a single systemd unit."""
    service: str
    invocation_id: str | None
    sub_state: str | None
    active_state: str | None
    error: str | None  # set when the systemctl call failed
    # systemd's verdict on the last run: "success" while healthy,
    # otherwise "core-dump", "exit-code", "signal", "oom-kill",
    # "timeout", "start-limit-hit". None when the property was absent.
    result: str | None = None
    # Auto-restarts after failure since the unit was last started
    # cleanly. Manual restarts do not move it.
    n_restarts: int | None = None
    # Exit status of the main process and how it died ("exited",
    # "killed", "dumped"). Both are 0 while the unit runs.
    exec_main_status: int | None = None
    exec_main_code: str | None = None


async def _run(cmd: list[str], timeout: float = 5.0) -> tuple[int, str, str]:
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        out, err = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        return proc.returncode or 0, out.decode(errors="replace"), err.decode(errors="replace")
    except (TimeoutError, FileNotFoundError) as e:
        return 127, "", str(e)


async def poll_invocation(
    service: str, timeout: float = 5.0
) -> InvocationSnapshot:
    """Sample one service's current InvocationID and state.

    Returns a snapshot with ``error`` populated on subprocess timeout or
    a non-zero rc — the rule treats those as "no information".
    """
    rc, out, err = await _run(
        [
            "systemctl",
            "show",
            service,
            "--property=InvocationID,SubState,ActiveState,Result,"
            "NRestarts,ExecMainStatus,ExecMainCode",
        ],
        timeout=timeout,
    )
    if rc == 127:
        return InvocationSnapshot(
            service=service, invocation_id=None,
            sub_state=None, active_state=None, error=err.strip()[:200],
        )
    if rc != 0:
        return InvocationSnapshot(
            service=service, invocation_id=None,
            sub_state=None, active_state=None,
            error=f"rc={rc}: {err.strip()[:200]}",
        )
    fields: dict[str, str] = {}
    for line in out.splitlines():
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        fields[key.strip()] = value.strip()
    invocation_id = fields.get("InvocationID") or None
    return InvocationSnapshot(
        service=service,
        invocation_id=invocation_id,
        sub_state=fields.get("SubState") or None,
        active_state=fields.get("ActiveState") or None,
        error=None,
        result=fields.get("Result") or None,
        n_restarts=_as_int(fields.get("NRestarts")),
        exec_main_status=_as_int(fields.get("ExecMainStatus")),
        exec_main_code=fields.get("ExecMainCode") or None,
    )


def _as_int(raw: str | None) -> int | None:
    """Parse a systemd numeric property, tolerating anything odd.

    A missing or unparsable value has to read as "no information" rather
    than as zero: zero is a meaningful value for both counters here.
    """
    if raw is None:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


async def poll_invocations(
    services: list[str], timeout: float = 5.0
) -> list[InvocationSnapshot]:
    """Sample multiple services concurrently. Order is preserved."""
    return await asyncio.gather(
        *(poll_invocation(svc, timeout=timeout) for svc in services)
    )
