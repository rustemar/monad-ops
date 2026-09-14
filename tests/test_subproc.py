"""The child of a short command must be gone when the call returns,
whether it finished, timed out, or the caller was cancelled."""

from __future__ import annotations

import asyncio
import sys

import pytest

from monad_ops.collector import subproc
from monad_ops.collector.subproc import reap, run_capture

SLEEP = [sys.executable, "-c", "import time; time.sleep(30)"]


def _record_spawn(monkeypatch):
    """Route spawns through the real create_subprocess_exec, keeping a
    handle on the Process so the test can inspect it afterwards."""
    spawned: list[asyncio.subprocess.Process] = []
    real = asyncio.create_subprocess_exec

    async def spawn(*args, **kwargs):
        proc = await real(*args, **kwargs)
        spawned.append(proc)
        return proc

    monkeypatch.setattr(subproc.asyncio, "create_subprocess_exec", spawn)
    return spawned


@pytest.mark.asyncio
async def test_run_capture_returns_rc_and_streams():
    rc, out, err = await run_capture(
        [sys.executable, "-c",
         "import sys; print('hi'); print('oops', file=sys.stderr); sys.exit(3)"],
        timeout=10,
    )
    assert (rc, out.strip(), err.strip()) == (3, "hi", "oops")


@pytest.mark.asyncio
async def test_missing_binary_is_127():
    rc, out, err = await run_capture(["/nonexistent/binary-xyz"], timeout=1)
    assert rc == 127 and out == "" and err


@pytest.mark.asyncio
async def test_timeout_kills_and_collects_the_child(monkeypatch):
    spawned = _record_spawn(monkeypatch)
    rc, _, err = await run_capture(SLEEP, timeout=0.2)
    assert rc == 127
    assert len(spawned) == 1
    assert spawned[0].returncode is not None


@pytest.mark.asyncio
async def test_cancelled_caller_does_not_orphan_the_child(monkeypatch):
    spawned = _record_spawn(monkeypatch)
    task = asyncio.create_task(run_capture(SLEEP, timeout=30))
    while not spawned:
        await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert spawned[0].returncode is not None


@pytest.mark.asyncio
async def test_reap_after_exit_is_a_no_op():
    proc = await asyncio.create_subprocess_exec(sys.executable, "-c", "pass")
    await proc.wait()
    rc = proc.returncode
    await reap(proc)
    assert proc.returncode == rc
