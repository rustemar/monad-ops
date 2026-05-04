"""In-memory per-peer error counters with rolling minute buckets.

Hot path is O(1) per event (dict lookup + bucket increment). Snapshot
walks the dict and trims old buckets — cheap enough for /api/peers
to serve at 1-second poll cadence without touching SQLite.
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from threading import Lock

from monad_ops.parser import ConsensusEvent, ConsensusEventKind


@dataclass(slots=True)
class _Bucket:
    ts_minute: int
    decrypt: int = 0
    session: int = 0


@dataclass(slots=True)
class _PeerEntry:
    addr: str
    ip: str
    last_event_ts_ms: int = 0
    last_event_kind: str = ""
    last_decrypt_error: str = ""
    buckets: deque = field(default_factory=lambda: deque(maxlen=120))


_KIND_MAP = {
    ConsensusEventKind.NETWORK_DECRYPT_FAIL: "decrypt",
    ConsensusEventKind.NETWORK_SESSION_TIMEOUT: "session",
}


class PeerTracker:
    def __init__(self) -> None:
        self._peers: dict[str, _PeerEntry] = {}
        # Per-IP enrichment shared across addr:port variants of the same peer.
        # Lazy-populated by background tasks (latency probe, geo resolver).
        self._enrichment: dict[str, dict] = {}
        self._lock = Lock()

    def set_enrichment(self, ip: str, **fields) -> None:
        with self._lock:
            self._enrichment.setdefault(ip, {}).update(fields)

    def known_ips(self) -> list[str]:
        with self._lock:
            return sorted({e.ip for e in self._peers.values()})

    def known_addrs(self) -> list[str]:
        with self._lock:
            return list(self._peers.keys())

    def on_event(self, ev: ConsensusEvent) -> None:
        kind = _KIND_MAP.get(ev.kind)
        if kind is None or not ev.peer_addr:
            return
        addr = ev.peer_addr
        ts = ev.ts_ms or int(time.time() * 1000)
        minute = (ts // 60_000) * 60_000
        with self._lock:
            entry = self._peers.get(addr)
            if entry is None:
                entry = _PeerEntry(addr=addr, ip=addr.split(":", 1)[0])
                self._peers[addr] = entry
            if not entry.buckets or entry.buckets[-1].ts_minute != minute:
                entry.buckets.append(_Bucket(ts_minute=minute))
            b = entry.buckets[-1]
            if kind == "decrypt":
                b.decrypt += 1
            else:
                b.session += 1
            if ts >= entry.last_event_ts_ms:
                entry.last_event_ts_ms = ts
                entry.last_event_kind = kind

    def snapshot(self, window_min: int = 60, now_ms: int | None = None) -> list[dict]:
        now_ms = now_ms or int(time.time() * 1000)
        cutoff = now_ms - window_min * 60_000
        out: list[dict] = []
        with self._lock:
            for entry in self._peers.values():
                d = s = 0
                for b in entry.buckets:
                    if b.ts_minute >= cutoff:
                        d += b.decrypt
                        s += b.session
                if d == 0 and s == 0 and entry.last_event_ts_ms < cutoff:
                    continue
                row = {
                    "addr": entry.addr,
                    "ip": entry.ip,
                    "decrypt": d,
                    "session": s,
                    "total": d + s,
                    "last_ts_ms": entry.last_event_ts_ms,
                    "last_kind": entry.last_event_kind,
                }
                row.update(self._enrichment.get(entry.ip, {}))
                out.append(row)
        out.sort(key=lambda x: (-x["total"], -x["last_ts_ms"]))
        return out
