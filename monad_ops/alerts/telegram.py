"""Telegram Bot API alert sink.

Uses the sendMessage endpoint with optional ``message_thread_id`` for
groups that have topics enabled (supergroups).
"""

from __future__ import annotations

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
    ) -> None:
        self._url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        self._chat_id = chat_id
        self._topic_id = topic_id
        self._source_tag = source_tag
        self._timeout = timeout_sec

    async def deliver(self, event: AlertEvent) -> None:
        emoji = _SEVERITY_EMOJI.get(event.severity, "⚪")
        text = (
            f"{emoji} <b>[{self._source_tag}]</b> {event.title}\n"
            f"<i>{event.rule} · {event.severity.value}</i>\n\n"
            f"{event.detail}"
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
                print(f"[telegram] send failed: {e}", file=sys.stderr, flush=True)
