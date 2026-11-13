"""Telegram Bot API alert sink.

Uses the sendMessage endpoint with optional ``message_thread_id`` for
groups that have topics enabled (supergroups).
"""

from __future__ import annotations

from html import escape

import httpx

from monad_ops.rules.events import AlertEvent, Severity

_SEVERITY_EMOJI = {
    Severity.INFO: "🟦",
    Severity.WARN: "🟠",
    Severity.CRITICAL: "🔴",
    Severity.RECOVERED: "🟢",
}


class TelegramSink:
    def __init__(
        self,
        bot_token: str,
        chat_id: int,
        topic_id: int = 0,
        source_tag: str = "monad-ops",
        timeout_sec: float = 15.0,
        drop_severities: frozenset[Severity] = frozenset({Severity.INFO}),
        bypass_drop_for_rules: frozenset[str] = frozenset(
            {"version_watch", "maintenance", "ping"}
        ),
    ) -> None:
        self._url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        self._chat_id = chat_id
        self._topic_id = topic_id
        self._source_tag = source_tag
        self._timeout = timeout_sec
        self._drop = drop_severities
        # Rules whose events bypass the severity drop set. Carved out
        # for transition-shaped INFO signals (new package release,
        # daily upgrade reminder, the maintenance-window summary, the
        # operator's own ping) where suppressing the only emission we
        # ever make would defeat the purpose of the rule.
        self._bypass = bypass_drop_for_rules

    async def deliver(self, event: AlertEvent) -> None:
        if event.severity in self._drop and event.rule not in self._bypass:
            return
        emoji = _SEVERITY_EMOJI.get(event.severity, "⚪")
        # Alert fields are plain text; only the wrapper tags are markup.
        # Probe summaries and log-derived details can contain <, >, or &.
        text = (
            f"{emoji} <b>[{escape(self._source_tag, quote=False)}]</b> "
            f"{escape(event.title, quote=False)}\n"
            f"<i>{escape(event.rule, quote=False)} · {event.severity.value}</i>\n\n"
            f"{escape(event.detail, quote=False)}"
        )
        payload: dict[str, object] = {
            "chat_id": self._chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        if self._topic_id:
            payload["message_thread_id"] = self._topic_id

        async with httpx.AsyncClient(timeout=self._timeout) as client:
            try:
                resp = await client.post(self._url, json=payload)
                resp.raise_for_status()
            except Exception as e:  # noqa: BLE001 — we never want to crash the loop
                import sys
                print(f"[telegram] send failed: {type(e).__name__}", file=sys.stderr, flush=True)
