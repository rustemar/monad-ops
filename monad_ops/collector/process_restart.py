"""Periodic poll of systemd InvocationID per tracked service.

Used by ``ProcessRestartRule`` to detect when a service has restarted
between polls — operator-triggered or auto-restart by systemd. The
``InvocationID`` is a UUID systemd issues on each unit start; comparing
against the last-seen value is a clean change detector.

Why not ``NRestarts``: that counter only increments on systemd-driven
auto-restart after failure, not on manual ``systemctl restart``. We
want to detect both.

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


async def _run(cmd: list[str], timeout: float = 5.0) -> tuple[int, str, str]:
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        out, err = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        return proc.returncode or 0, out.decode(errors="replace"), err.decode(errors="replace")
    except (FileNotFoundError, asyncio.TimeoutError) as e:
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
            "--property=InvocationID,SubState,ActiveState",
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
    )


async def poll_invocations(
    services: list[str], timeout: float = 5.0
) -> list[InvocationSnapshot]:
    """Sample multiple services concurrently. Order is preserved."""
    return await asyncio.gather(
        *(poll_invocation(svc, timeout=timeout) for svc in services)
    )
