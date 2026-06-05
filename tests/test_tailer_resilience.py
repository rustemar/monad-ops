"""Self-healing journalctl follow — collector/journal.py tail_raw_lines.

Covers the 2026-06-05 failure: a live ``journalctl -f`` stops delivering
lines after a journald rotation (readline blocks forever, no EOF) while
the node keeps producing. The tailer must detect the silence, respawn
the child, keep yielding, and escalate to a TailError if respawning
never restores flow.

The subprocess is mocked at the ``spawn`` seam so no journalctl runs.
"""

from __future__ import annotations

import asyncio
import itertools

import pytest

from monad_ops.collector.journal import (
    TailError,
    tail_execution_blocks,
    tail_raw_lines,
)


class _FakeStream:
    """Minimal asyncio.StreamReader stand-in.

    Emits the queued lines, then either hangs forever (a broken follow:
    readline never returns) or returns b"" (EOF).
    """

    def __init__(self, lines: list[bytes], hang_after: bool):
        self._lines = list(lines)
        self._hang_after = hang_after

    async def readline(self) -> bytes:
        if self._lines:
            return self._lines.pop(0)
        if self._hang_after:
            await asyncio.Event().wait()  # never set → simulates a dead follow
        return b""  # EOF

    async def read(self) -> bytes:
        return b""


class _FakeProc:
    def __init__(self, lines: list[bytes], hang_after: bool):
        self.stdout = _FakeStream(lines, hang_after)
        self.stderr = _FakeStream([], hang_after=False)
        self.returncode: int | None = None

    def terminate(self) -> None:
        self.returncode = -15

    def kill(self) -> None:
        self.returncode = -9

    async def wait(self) -> int:
        if self.returncode is None:
            self.returncode = 0
        return self.returncode


def _make_spawn(procs: list[_FakeProc]):
    """Return a spawn() that hands out the given procs in order, recording
    each call. Runs out → raises, which surfaces a real test failure
    rather than hanging."""
    it = iter(procs)
    calls: list[tuple] = []

    async def spawn(*args, **kwargs):
        calls.append((args, kwargs))
        return next(it)

    spawn.calls = calls  # type: ignore[attr-defined]
    return spawn


@pytest.mark.asyncio
async def test_respawns_on_stalled_follow_and_keeps_yielding():
    """First child emits two lines then hangs (broken follow); the tailer
    respawns and the second child's lines come through, then EOF ends it."""
    spawn = _make_spawn([
        _FakeProc([b"line-1\n", b"line-2\n"], hang_after=True),
        _FakeProc([b"line-3\n"], hang_after=False),
    ])

    got: list[object] = []
    async for item in tail_raw_lines(
        "monad-execution", idle_timeout_sec=0.05, spawn=spawn,
    ):
        got.append(item)

    lines = [x for x in got if isinstance(x, str)]
    assert lines == ["line-1", "line-2", "line-3"]
    # Exactly one respawn: the original child plus one live re-tail.
    assert len(spawn.calls) == 2
    # The respawn must drop the lookback and tail live-only so the
    # consumer's non-idempotent counters don't double-count history.
    assert "--lines=0" in spawn.calls[1][0]


@pytest.mark.asyncio
async def test_respawn_is_live_only_even_after_lookback_start():
    """A lookback start (history then live) must still respawn live-only."""
    spawn = _make_spawn([
        _FakeProc([b"hist\n"], hang_after=True),
        _FakeProc([], hang_after=False),
    ])

    async for _ in tail_raw_lines(
        "monad-execution", lookback="5 min ago",
        idle_timeout_sec=0.05, spawn=spawn,
    ):
        pass

    assert "--since" in spawn.calls[0][0]          # first start replays history
    assert "--since" not in spawn.calls[1][0]      # respawn does not
    assert "--lines=0" in spawn.calls[1][0]


@pytest.mark.asyncio
async def test_escalates_when_respawning_never_helps():
    """Every child hangs immediately. After max_respawns, the tailer must
    yield a non-graceful TailError instead of spinning forever."""
    spawn = _make_spawn([_FakeProc([], hang_after=True) for _ in range(6)])

    errors: list[TailError] = []
    async for item in tail_raw_lines(
        "monad-bft",
        idle_timeout_sec=0.02,
        max_respawns=2,
        respawn_window_sec=300.0,
        spawn=spawn,
    ):
        if isinstance(item, TailError):
            errors.append(item)

    assert len(errors) == 1
    assert errors[0].graceful is False
    assert "stalled" in errors[0].message
    # proc0 + respawn1 + respawn2 emit fine; respawn3's accounting pushes
    # the count past max_respawns=2 and escalates before a 4th spawn.
    assert len(spawn.calls) == 3


@pytest.mark.asyncio
async def test_rapid_respawn_window_evicts_old_timestamps():
    """Respawns spaced past the window must NOT accumulate toward the cap
    — otherwise a healthy node that rotates occasionally would eventually
    trip the escalation. Each child emits one line then hangs, so the
    loop keeps yielding while the (mocked) clock jumps 1000s per respawn."""
    clock = itertools.count(0, 1000)  # each respawn 1000s apart, window is 300

    spawn = _make_spawn(
        [_FakeProc([f"L{i}\n".encode()], hang_after=True) for i in range(5)]
    )
    n = 0
    async for item in tail_raw_lines(
        "monad-execution",
        idle_timeout_sec=0.02,
        max_respawns=2,
        respawn_window_sec=300.0,
        spawn=spawn,
        now=lambda: next(clock),
    ):
        if isinstance(item, TailError):
            pytest.fail("should not escalate: respawns are outside the window")
        n += 1
        if n >= 4:
            break  # 4 lines from 4 children, never escalated


@pytest.mark.asyncio
async def test_eof_surfaces_graceful_tailerror():
    """A signal-terminated child (rc<0) is a graceful exit, not an alert."""
    proc = _FakeProc([b"only\n"], hang_after=False)
    proc.returncode = None

    spawn = _make_spawn([proc])
    got = []
    async for item in tail_raw_lines("monad-execution", spawn=spawn):
        got.append(item)

    assert got[0] == "only"
    assert isinstance(got[-1], TailError)
    # rc=0 EOF → not graceful (positive/zero rc = real exit).
    assert got[-1].graceful is False


@pytest.mark.asyncio
async def test_backfill_does_not_respawn():
    """follow=False is EOF-bounded; a silent tail must not respawn."""
    spawn = _make_spawn([_FakeProc([b"a\n", b"b\n"], hang_after=False)])

    got = [
        x async for x in tail_raw_lines(
            "monad-execution", follow=False, lookback="1 min ago", spawn=spawn,
        )
    ]
    assert [x for x in got if isinstance(x, str)] == ["a", "b"]
    assert len(spawn.calls) == 1


@pytest.mark.asyncio
async def test_execution_wrapper_parses_and_passes_through_respawn():
    """The exec wrapper parses real __exec_block lines and inherits the
    respawn behaviour from tail_raw_lines."""
    exec_line = (
        b"2026-06-05 20:49:52.110317265 [2225383] runloop_monad.cpp:361 "
        b"LOG_INFO\t__exec_block,bl=36389244,"
        b"id=0x2f287699ebe941ca193d7b39374d6de3f61d2e8a452d3bb61e31f9d44692bd1a,"
        b"ts=1780692592105,tx=    5,rt=   3,rtp=60.00%,sr=  132\xc2\xb5s,"
        b"txe= 2976\xc2\xb5s,cmt= 1703\xc2\xb5s,tot= 4947\xc2\xb5s,"
        b"tpse= 1680,tps= 1010,gas=  5835291,gpse=1960,gps=1179\n"
    )
    spawn = _make_spawn([
        _FakeProc([exec_line], hang_after=True),
        _FakeProc([], hang_after=False),
    ])

    blocks = []
    async for item in tail_execution_blocks(idle_timeout_sec=0.05, spawn=spawn):
        if not isinstance(item, (TailError, str)):
            blocks.append(item)

    assert len(blocks) == 1
    assert blocks[0].block_number == 36389244
    assert len(spawn.calls) == 2  # respawned after the hang
