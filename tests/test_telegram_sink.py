"""Telegram sink severity-filter."""

from __future__ import annotations

from html import unescape

import pytest

from monad_ops.alerts.telegram import TelegramSink
from monad_ops.collector.probes import ProbeResult
from monad_ops.rules.events import AlertEvent, Severity
from monad_ops.rules.probe_alerts import ProbeAlertRule


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
async def test_telegram_bypasses_drop_for_version_watch_info(monkeypatch) -> None:
    """version_watch INFO must reach Telegram even when INFO is in
    drop_severities — the rule fires once per release, suppressing it
    silently is the bug we're guarding against."""
    sink = TelegramSink(bot_token="x", chat_id=1)  # default drop=INFO, bypass=version_watch
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

    version_info = AlertEvent(
        rule="version_watch", severity=Severity.INFO, key="k", title="t", detail="d",
    )
    other_info = AlertEvent(
        rule="reorg", severity=Severity.INFO, key="k", title="t", detail="d",
    )
    await sink.deliver(version_info)
    await sink.deliver(other_info)
    assert len(sent) == 1  # version_watch passed; reorg INFO dropped


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


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["post", "status"])
async def test_telegram_failures_never_log_request_url(monkeypatch, capsys, stage) -> None:
    """Both connection and HTTP errors may contain the credential-bearing URL."""
    import httpx

    fake_token = "12345:synthetic_fake_token"
    sink = TelegramSink(bot_token=fake_token, chat_id=1)
    url = f"https://api.telegram.org/bot{fake_token}/sendMessage"

    class _Stub:
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return False
        async def post(self, actual_url, **kwargs):
            assert actual_url == url
            if stage == "post":
                raise httpx.ConnectError(f"network failure at {actual_url}")

            class Response:
                def raise_for_status(self):
                    request = httpx.Request("POST", actual_url)
                    raise httpx.HTTPStatusError(
                        f"HTTP error for {actual_url}", request=request,
                        response=httpx.Response(500, request=request),
                    )
            return Response()

    monkeypatch.setattr(httpx, "AsyncClient", lambda **_: _Stub())
    await sink.deliver(_ev(Severity.WARN))
    captured = capsys.readouterr()
    assert fake_token not in captured.out + captured.err
    assert url not in captured.out + captured.err
    assert ("ConnectError" if stage == "post" else "HTTPStatusError") in captured.err


@pytest.mark.asyncio
async def test_telegram_escapes_plain_text_fields_but_retains_wrapper_markup(monkeypatch) -> None:
    sent = []

    class Stub:
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return False
        async def post(self, url, *, json):
            sent.append(json)
            class Response:
                def raise_for_status(self): pass
            return Response()

    import httpx
    monkeypatch.setattr(httpx, "AsyncClient", lambda **_: Stub())
    event = AlertEvent(rule="r<&>", severity=Severity.WARN, key="k",
                       title="Title <x> & more", detail="detail <x> & more")
    sink = TelegramSink(bot_token="fake", chat_id=123, topic_id=42, source_tag="node<&>")
    await sink.deliver(event)
    assert len(sent) == 1
    assert sent[0]["parse_mode"] == "HTML"
    assert sent[0]["chat_id"] == 123 and sent[0]["message_thread_id"] == 42
    assert sent[0]["text"] == (
        "🟠 <b>[node&lt;&amp;&gt;]</b> Title &lt;x&gt; &amp; more\n"
        "<i>r&lt;&amp;&gt; · warn</i>\n\n"
        "detail &lt;x&gt; &amp; more"
    )
    assert unescape(sent[0]["text"]) == (
        "🟠 <b>[node<&>]</b> Title <x> & more\n<i>r<&> · warn</i>\n\n"
        "detail <x> & more"
    )


@pytest.mark.asyncio
async def test_probe_alerts_escape_thresholds_in_telegram_payload(monkeypatch) -> None:
    sent = []

    class Stub:
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return False
        async def post(self, url, *, json):
            sent.append(json)
            class Response:
                def raise_for_status(self): pass
            return Response()

    import httpx
    monkeypatch.setattr(httpx, "AsyncClient", lambda **_: Stub())
    rule = ProbeAlertRule({"disk_usage": "warn"})
    samples = [
        ProbeResult("fd_limits", "critical", "monad-execution nofile soft=1024 < 4096", {}),
        ProbeResult("disk_usage", "ok", "all mounts < 85% (peak 45%)", {}),
    ]
    events = rule.evaluate(samples)
    assert [event.severity for event in events] == [Severity.CRITICAL, Severity.RECOVERED]
    sink = TelegramSink(bot_token="fake", chat_id=1)
    for event in events:
        await sink.deliver(event)
    assert len(sent) == 2
    assert "soft=1024 &lt; 4096" in sent[0]["text"]
    assert "all mounts &lt; 85%" in sent[1]["text"]
    assert all(payload["parse_mode"] == "HTML" for payload in sent)
