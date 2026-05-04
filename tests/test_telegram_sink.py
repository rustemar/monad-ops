"""Telegram sink severity-filter."""

from __future__ import annotations

import pytest

from monad_ops.alerts.telegram import TelegramSink
from monad_ops.rules.events import AlertEvent, Severity


def _ev(sev: Severity) -> AlertEvent:
    return AlertEvent(rule="reorg", severity=sev, key="k", title="t", detail="d")


@pytest.mark.asyncio
async def test_telegram_drops_info_by_default(monkeypatch) -> None:
    sink = TelegramSink(bot_token="x", chat_id=1)
    sent: list = []

    class _Stub:
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return False
        async def post(self, *a, **kw):
            sent.append(kw.get("json") or a)
            class R:
                def raise_for_status(self): pass
            return R()

    import httpx
    monkeypatch.setattr(httpx, "AsyncClient", lambda **_: _Stub())
    await sink.deliver(_ev(Severity.INFO))
    await sink.deliver(_ev(Severity.WARN))
    await sink.deliver(_ev(Severity.CRITICAL))
    await sink.deliver(_ev(Severity.RECOVERED))
    assert len(sent) == 3  # info dropped


@pytest.mark.asyncio
async def test_telegram_respects_explicit_drop_set(monkeypatch) -> None:
    sink = TelegramSink(
        bot_token="x", chat_id=1,
        drop_severities=frozenset({Severity.INFO, Severity.RECOVERED}),
    )
    sent: list = []

    class _Stub:
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return False
        async def post(self, *a, **kw):
            sent.append(kw)
            class R:
                def raise_for_status(self): pass
            return R()

    import httpx
    monkeypatch.setattr(httpx, "AsyncClient", lambda **_: _Stub())
    for sev in Severity:
        await sink.deliver(_ev(sev))
    assert len(sent) == 2  # only WARN + CRITICAL pass
