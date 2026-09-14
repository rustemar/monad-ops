"""Run a short command and always collect the child.

A child still running when its task is cancelled keeps its pipes open;
once ``asyncio.run`` has closed the loop, the transport's finaliser has
no loop to schedule the close on and the stop logs "Event loop is
closed". Every subprocess site funnels through ``reap`` so the child is
gone before the loop is.
"""

from __future__ import annotations

import asyncio
import contextlib


async def reap(proc: asyncio.subprocess.Process) -> None:
    """Kill the child if it is still running and wait for it."""
    if proc.returncode is not None:
        return
    with contextlib.suppress(ProcessLookupError):
        proc.kill()
    with contextlib.suppress(ProcessLookupError):
        await proc.wait()


async def run_capture(
    cmd: list[str], timeout: float = 5.0,
) -> tuple[int, str, str]:
    """Run ``cmd``, return ``(rc, stdout, stderr)``.

    rc=127 stands for "not observed": the binary is missing or the call
    timed out. Callers treat it as no information rather than a failure.
    """
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except FileNotFoundError as e:
        return 127, "", str(e)
    try:
        out, err = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except TimeoutError as e:
        return 127, "", str(e)
    finally:
        await reap(proc)
    return proc.returncode or 0, out.decode(errors="replace"), err.decode(errors="replace")
