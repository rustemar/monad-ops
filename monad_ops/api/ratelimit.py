"""Per-client request limiter for the JSON API.

Token bucket keyed by client IP. The bucket holds ``requests`` tokens
and refills at ``requests / window_sec`` per second, so a client can
burst up to the full allowance and then sustain the average rate.
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass

from starlette.requests import Request

# Idle buckets are dropped once the table grows past this many keys, so
# a scan of many addresses cannot grow memory without bound.
_PRUNE_ABOVE = 10_000


@dataclass(frozen=True)
class Verdict:
    allowed: bool
    remaining: int
    retry_after_sec: int


def client_key(request: Request) -> str:
    """The address the limit is keyed on.

    The dashboard sits behind Cloudflare and a host nginx, so the socket
    peer is never the visitor. Cloudflare's own header wins, then the
    first hop of X-Forwarded-For, then whatever uvicorn resolved.
    """
    headers = request.headers
    cf = headers.get("cf-connecting-ip")
    if cf:
        return cf.strip()
    xff = headers.get("x-forwarded-for")
    if xff:
        first = xff.split(",")[0].strip()
        if first:
            return first
    real = headers.get("x-real-ip")
    if real:
        return real.strip()
    return request.client.host if request.client else "unknown"


class TokenBucketLimiter:
    def __init__(self, requests: int, window_sec: float) -> None:
        if requests < 1 or window_sec <= 0:
            raise ValueError("requests must be >= 1 and window_sec > 0")
        self.capacity = float(requests)
        self.rate = requests / window_sec
        self._buckets: dict[str, tuple[float, float]] = {}

    def check(self, key: str, now: float | None = None) -> Verdict:
        now = time.monotonic() if now is None else now
        tokens, last = self._buckets.get(key, (self.capacity, now))
        tokens = min(self.capacity, tokens + (now - last) * self.rate)
        if tokens >= 1.0:
            tokens -= 1.0
            self._buckets[key] = (tokens, now)
            if len(self._buckets) > _PRUNE_ABOVE:
                self._prune(now)
            return Verdict(True, int(tokens), 0)
        self._buckets[key] = (tokens, now)
        wait = (1.0 - tokens) / self.rate
        return Verdict(False, 0, max(1, math.ceil(wait)))

    def _prune(self, now: float) -> None:
        full_after = self.capacity / self.rate
        self._buckets = {
            k: v for k, v in self._buckets.items() if now - v[1] < full_after
        }
