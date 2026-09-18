"""The sink chain around the gate: history records everything, Telegram
hears only what the window allows, and the summary is recorded too."""

from __future__ import annotations

import time

import pytest

from monad_ops.alerts.telegram import TelegramSink
from monad_ops.cli import _wrap_sink
from monad_ops.rules.events import AlertEvent, Severity
from monad_ops.state import State
from monad_ops.storage import Storage


class _Capture:
    def __init__(self) -> None:
        self.events: list[AlertEvent] = []

    async def deliver(self, event: AlertEvent) -> None:
        self.events.append(event)


@pytest.mark.asyncio
async def test_held_events_reach_the_history_but_not_the_channel(tmp_path) -> None:
    storage = Storage(tmp_path / "state.db")
    state = State(storage=storage)
    inner = _Capture()
    sink, gate = _wrap_sink(inner, storage, state)
    assert gate is not None

    # stall has recovered before, so it counts as an envelope the summary can call open
    storage.write_alert(
        AlertEvent(rule="stall", severity=Severity.RECOVERED, key="stall", title="t", detail="d"),
        ts=time.time() - 3600,
    )
    storage.open_maintenance(time.time() + 600)
    ev = AlertEvent(
        rule="stall", severity=Severity.CRITICAL, key="stall:critical", title="t", detail="d"
    )
    await sink.deliver(ev)

    since, until = storage.maintenance_window()
    rows = storage.alerts_between(since, until)
    assert [(r[1], r[2]) for r in rows] == [("stall", "critical")]
    assert inner.events == []

    storage.open_maintenance(time.time())  # `--off`: the window ends now
    await gate.flush()
    assert [e.rule for e in inner.events] == ["maintenance"]
    assert inner.events[0].severity is Severity.CRITICAL  # stall never recovered
    later = storage.alerts_between(since, time.time() + 1)
    assert any(r[1] == "maintenance" for r in later)
    storage.close()


@pytest.mark.asyncio
async def test_without_persistence_there_is_no_gate(tmp_path) -> None:
    state = State(storage=None)
    inner = _Capture()
    sink, gate = _wrap_sink(inner, None, state)
    assert gate is None
    await sink.deliver(
        AlertEvent(rule="stall", severity=Severity.WARN, key="k", title="t", detail="d")
    )
    assert len(inner.events) == 1


@pytest.mark.asyncio
async def test_telegram_lets_the_summary_and_ping_through_the_info_drop(monkeypatch) -> None:
    sink = TelegramSink(bot_token="x", chat_id=1)
    sent: list = []

    class _Stub:
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return False
        async def post(self, *a, **kw):
            sent.append(kw.get("json"))
            class R:
                def raise_for_status(self): pass
            return R()

    import httpx
    monkeypatch.setattr(httpx, "AsyncClient", lambda **_: _Stub())
    for rule in ("maintenance", "ping", "reorg"):
        await sink.deliver(
            AlertEvent(rule=rule, severity=Severity.INFO, key=rule, title="t", detail="d")
        )
    assert len(sent) == 2  # generic INFO still dropped
