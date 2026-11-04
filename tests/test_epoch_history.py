"""Bounded offline journal fixtures: partial history must stay explicit."""

from __future__ import annotations

import asyncio
import sys
from collections import deque
from types import SimpleNamespace

import pytest

from monad_ops.collector import epoch_probe

_BLOCK = object()


def marker(seq: int, epoch: int) -> bytes:
    return f'{{"block_seq_num":"{seq}","block_epoch":"{epoch}"}}\n'.encode()


class JournalProcess:
    """No real child or signals; events model stalled reads/exit/cleanup."""

    def __init__(self, lines=(), *, exit_code=0):
        self.lines = deque(lines)
        self.exit_code = exit_code
        self.returncode = None
        self.stdout = self
        self.read_blocked = asyncio.Event()
        self.exit_started = asyncio.Event()
        self.exit_release = asyncio.Event()
        self.exit_release.set()
        self.cleanup_started = asyncio.Event()
        self.cleanup_restarted = asyncio.Event()
        self.cleanup_release = asyncio.Event()
        self.cleanup_release.set()
        self.kills = 0
        self.communications = 0
        self.cleanup_error = None
        self.pipe_reader = None

    async def readline(self):
        item = self.lines.popleft() if self.lines else b""
        if item is _BLOCK:
            self.read_blocked.set()
            await asyncio.Future()
        if isinstance(item, Exception):
            raise item
        return item

    async def wait(self):
        self.exit_started.set()
        await self.exit_release.wait()
        self.returncode = self.exit_code
        return self.returncode

    def kill(self):
        self.kills += 1
        self.returncode = -9

    async def communicate(self):
        self.communications += 1
        self.cleanup_started.set()
        if self.communications > 1:
            self.cleanup_restarted.set()
        # Like Process.communicate(), own a separately scheduled pipe
        # reader that asyncio.run's final cancellation sweep can also hit.
        self.pipe_reader = asyncio.create_task(self.cleanup_release.wait())
        await self.pipe_reader
        if self.cleanup_error is not None:
            raise self.cleanup_error
        self.lines.clear()
        return b"", None


def install_journal(monkeypatch, proc):
    calls = []

    async def spawn(*args, **kwargs):
        calls.append((args, kwargs))
        if isinstance(proc, BaseException):
            raise proc
        return proc

    monkeypatch.setattr(epoch_probe.asyncio, "create_subprocess_exec", spawn)
    return calls


@pytest.mark.asyncio
async def test_completed_waits_for_success_and_preserves_accepted_order(monkeypatch):
    proc = JournalProcess([
        b"unrelated\n", marker(110, 3), marker(0, 3), marker(111, 0),
        b"\xff" + marker(112, 3), marker(112, 3), marker(210, 4),
    ])
    calls = install_journal(monkeypatch, proc)
    result = await epoch_probe.scan_epoch_history()

    assert result.status == "completed"
    assert result.observations == [(110, 3), (112, 3), (112, 3), (210, 4)]
    assert result.returncode == 0 and result.error is None
    assert result.elapsed_sec >= 0
    assert (result.unit, result.since, result.timeout_sec) == (
        "monad-bft", "24 hours ago", 600.0,
    )
    assert calls == [((
        "journalctl", "-u", "monad-bft", "-o", "cat", "--no-pager",
        "--since", "24 hours ago",
    ), {
        "stdout": asyncio.subprocess.PIPE,
        "stderr": asyncio.subprocess.DEVNULL, "limit": 1_000_000,
    })]
    assert proc.exit_started.is_set()
    assert proc.kills == 0 and proc.communications == 1


@pytest.mark.asyncio
async def test_empty_success_is_complete_and_reports_requested_window(monkeypatch):
    proc = JournalProcess()
    calls = install_journal(monkeypatch, proc)
    result = await epoch_probe.scan_epoch_history("custom-unit", "2 hours ago", 1.0)

    assert result.status == "completed" and result.observations == []
    assert (result.unit, result.since, result.timeout_sec) == (
        "custom-unit", "2 hours ago", 1.0,
    )
    assert calls[0][0][-1] == "2 hours ago"
    assert calls[0][0][2] == "custom-unit"


@pytest.mark.asyncio
@pytest.mark.parametrize("exit_code", [1, -15])
async def test_eof_with_nonzero_exit_is_failed_and_retains_history(monkeypatch, exit_code):
    proc = JournalProcess([marker(110, 3)], exit_code=exit_code)
    install_journal(monkeypatch, proc)
    result = await epoch_probe.scan_epoch_history()

    assert result.status == "failed"
    assert result.observations == [(110, 3)]
    assert result.returncode == exit_code
    assert result.error == f"journalctl exited with status {exit_code}"
    assert proc.kills == 0


@pytest.mark.asyncio
async def test_real_owned_child_nonzero_exit_preserves_partial_history(monkeypatch):
    real_spawn = asyncio.create_subprocess_exec
    children = []

    async def spawn(*args, **kwargs):
        assert args[0] == "journalctl"
        # Only this tiny owned Python child runs; no system journal read.
        child = await real_spawn(
            sys.executable, "-c",
            'print(\'{"block_seq_num":"110","block_epoch":"3"}\', flush=True); '
            'raise SystemExit(7)',
            **kwargs,
        )
        children.append(child)
        return child

    monkeypatch.setattr(epoch_probe.asyncio, "create_subprocess_exec", spawn)
    result = await epoch_probe.scan_epoch_history(timeout_sec=2.0)

    assert result.status == "failed"
    assert result.observations == [(110, 3)]
    assert result.returncode == 7
    assert result.error == "journalctl exited with status 7"
    assert len(children) == 1 and children[0].returncode == 7


@pytest.mark.asyncio
async def test_timeout_keeps_partial_history_and_collects_child(monkeypatch):
    proc = JournalProcess([marker(110, 3), _BLOCK])
    install_journal(monkeypatch, proc)
    result = await epoch_probe.scan_epoch_history(timeout_sec=0.01)

    assert result.status == "timed_out"
    assert result.observations == [(110, 3)]
    assert result.returncode is None  # No natural exit was observed.
    assert proc.kills == 1 and proc.communications == 1
    assert not proc.lines


@pytest.mark.asyncio
async def test_eof_does_not_escape_exit_timeout(monkeypatch):
    proc = JournalProcess([marker(110, 3)])
    proc.exit_release.clear()
    install_journal(monkeypatch, proc)
    result = await epoch_probe.scan_epoch_history(timeout_sec=0.01)

    assert proc.exit_started.is_set()
    assert result.status == "timed_out"
    assert result.observations == [(110, 3)]
    assert proc.kills == 1 and proc.communications == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [
    FileNotFoundError("journalctl missing"), PermissionError("journal unavailable"),
])
async def test_spawn_failure_is_explicit(monkeypatch, error):
    install_journal(monkeypatch, error)
    result = await epoch_probe.scan_epoch_history()

    assert result.status == "failed" and result.observations == []
    assert result.error == f"{type(error).__name__}: {error}"
    assert result.returncode is None


@pytest.mark.asyncio
async def test_read_failure_preserves_partial_and_drains_even_after_exit(monkeypatch):
    proc = JournalProcess([marker(110, 3), ValueError("oversized journal line"), b"pending"])
    proc.returncode = 2
    install_journal(monkeypatch, proc)
    result = await epoch_probe.scan_epoch_history()

    assert result.status == "failed"
    assert result.observations == [(110, 3)]
    assert result.error == "ValueError: oversized journal line"
    assert proc.kills == 0 and proc.communications == 1
    assert not proc.lines


@pytest.mark.asyncio
async def test_elapsed_includes_spawn_through_cleanup(monkeypatch):
    proc = JournalProcess()
    install_journal(monkeypatch, proc)
    ticks = iter([10.0, 12.75])
    monkeypatch.setattr(epoch_probe, "time", SimpleNamespace(monotonic=lambda: next(ticks)))
    result = await epoch_probe.scan_epoch_history()

    assert proc.communications == 1
    assert result.elapsed_sec == 2.75


@pytest.mark.asyncio
async def test_cleanup_failure_is_not_reported_complete(monkeypatch):
    proc = JournalProcess()
    proc.cleanup_error = OSError("pipe cleanup failed")
    install_journal(monkeypatch, proc)
    result = await epoch_probe.scan_epoch_history()

    assert result.status == "failed"
    assert result.error == "cleanup OSError: pipe cleanup failed"


@pytest.mark.asyncio
async def test_cleanup_has_its_own_bound(monkeypatch):
    proc = JournalProcess([marker(110, 3), ValueError("oversized journal line")])
    proc.cleanup_release.clear()
    install_journal(monkeypatch, proc)
    monkeypatch.setattr(epoch_probe, "_HISTORY_CLEANUP_TIMEOUT_SEC", 0.01)
    result = await asyncio.wait_for(epoch_probe.scan_epoch_history(), timeout=0.5)

    assert result.status == "failed"
    assert result.observations == [(110, 3)]
    assert result.error == "ValueError: oversized journal line; cleanup TimeoutError: "
    assert proc.kills == 1


@pytest.mark.asyncio
async def test_cancel_propagates_after_cleanup_despite_second_cancel(monkeypatch):
    proc = JournalProcess([marker(110, 3), _BLOCK])
    proc.cleanup_release.clear()
    install_journal(monkeypatch, proc)
    task = asyncio.create_task(epoch_probe.scan_epoch_history())
    await asyncio.wait_for(proc.read_blocked.wait(), timeout=0.5)
    task.cancel()
    await asyncio.wait_for(proc.cleanup_started.wait(), timeout=0.5)
    task.cancel()
    await asyncio.wait_for(proc.cleanup_restarted.wait(), timeout=0.5)
    proc.cleanup_release.set()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=0.5)
    assert proc.kills == 1 and proc.communications == 2
    assert not proc.lines


@pytest.mark.asyncio
async def test_shutdown_cancels_scanner_and_pipe_reader_during_timeout_cleanup(monkeypatch):
    proc = JournalProcess([marker(110, 3), _BLOCK])
    proc.cleanup_release.clear()
    install_journal(monkeypatch, proc)
    task = asyncio.create_task(epoch_probe.scan_epoch_history(timeout_sec=0.01))
    await asyncio.wait_for(proc.cleanup_started.wait(), timeout=0.5)

    # Mimic asyncio.run's _cancel_all_tasks snapshot. Shielding the
    # scanner alone cannot protect a cleanup/pipe task cancelled directly.
    pipe_reader = proc.pipe_reader
    task.cancel()
    pipe_reader.cancel()
    await asyncio.wait_for(proc.cleanup_restarted.wait(), timeout=0.5)
    proc.cleanup_release.set()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=0.5)
    assert pipe_reader.cancelled()
    assert proc.kills == 1 and proc.communications == 2
    assert proc.pipe_reader.done() and not proc.pipe_reader.cancelled()
    assert not proc.lines


@pytest.mark.asyncio
async def test_cancel_is_not_replaced_by_cleanup_failure(monkeypatch):
    proc = JournalProcess([_BLOCK])
    proc.cleanup_error = OSError("pipe cleanup failed")
    install_journal(monkeypatch, proc)
    task = asyncio.create_task(epoch_probe.scan_epoch_history())
    await asyncio.wait_for(proc.read_blocked.wait(), timeout=0.5)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=0.5)
    assert proc.kills == 1 and proc.communications == 1
