"""Per-client rate limit on the JSON API."""

from __future__ import annotations

import httpx
import pytest

from monad_ops.api.app import build_app
from monad_ops.api.ratelimit import TokenBucketLimiter
from monad_ops.config import ApiConfig, Config, NodeConfig
from monad_ops.state import State

# ---------------------------------------------------------------------------
# Bucket
# ---------------------------------------------------------------------------

def test_bucket_allows_burst_then_refuses():
    lim = TokenBucketLimiter(requests=3, window_sec=30)
    verdicts = [lim.check("a", now=100.0) for _ in range(4)]
    assert [v.allowed for v in verdicts] == [True, True, True, False]
    assert [v.remaining for v in verdicts[:3]] == [2, 1, 0]
    # One token refills every 10 s; the refusal says so.
    assert verdicts[3].retry_after_sec == 10


def test_bucket_refills_with_time():
    lim = TokenBucketLimiter(requests=2, window_sec=10)
    assert lim.check("a", now=0.0).allowed
    assert lim.check("a", now=0.0).allowed
    assert not lim.check("a", now=0.0).allowed
    assert lim.check("a", now=5.0).allowed
    assert not lim.check("a", now=5.0).allowed


def test_bucket_keys_are_independent():
    lim = TokenBucketLimiter(requests=1, window_sec=10)
    assert lim.check("a", now=0.0).allowed
    assert not lim.check("a", now=0.0).allowed
    assert lim.check("b", now=0.0).allowed


def test_bucket_prunes_idle_keys(monkeypatch):
    import monad_ops.api.ratelimit as rl
    monkeypatch.setattr(rl, "_PRUNE_ABOVE", 2)
    lim = TokenBucketLimiter(requests=1, window_sec=10)
    lim.check("a", now=0.0)
    lim.check("b", now=0.0)
    lim.check("c", now=100.0)  # a and b are full again by now and get dropped
    assert set(lim._buckets) == {"c"}


def test_bucket_rejects_bad_settings():
    with pytest.raises(ValueError):
        TokenBucketLimiter(requests=0, window_sec=10)
    with pytest.raises(ValueError):
        TokenBucketLimiter(requests=1, window_sec=0)


# ---------------------------------------------------------------------------
# Middleware
# ---------------------------------------------------------------------------

def _client(api: ApiConfig) -> httpx.AsyncClient:
    cfg = Config(node=NodeConfig(name="test-node", rpc_url="http://127.0.0.1:9999"), api=api)
    app = build_app(State(), cfg)
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")


@pytest.mark.asyncio
async def test_api_returns_429_with_headers_after_limit():
    async with _client(ApiConfig(rate_limit_requests=2, rate_limit_window_sec=60)) as c:
        r1 = await c.get("/api/status/errors")
        r2 = await c.get("/api/status/errors")
        r3 = await c.get("/api/status/errors")
    assert (r1.status_code, r2.status_code, r3.status_code) == (200, 200, 429)
    assert r1.headers["x-ratelimit-limit"] == "2"
    assert r1.headers["x-ratelimit-remaining"] == "1"
    assert r2.headers["x-ratelimit-remaining"] == "0"
    assert r3.headers["retry-after"] == "30"
    assert r3.headers["access-control-allow-origin"] == "*"
    assert r3.json() == {"error": "rate_limited", "retry_after_sec": 30}


@pytest.mark.asyncio
async def test_api_limit_is_per_forwarded_client():
    async with _client(ApiConfig(rate_limit_requests=1, rate_limit_window_sec=60)) as c:
        a1 = await c.get("/api/status/errors", headers={"CF-Connecting-IP": "203.0.113.1"})
        a2 = await c.get("/api/status/errors", headers={"CF-Connecting-IP": "203.0.113.1"})
        b1 = await c.get("/api/status/errors", headers={"X-Forwarded-For": "203.0.113.2, 10.0.0.1"})
    assert (a1.status_code, a2.status_code, b1.status_code) == (200, 429, 200)


@pytest.mark.asyncio
async def test_html_pages_are_not_limited():
    async with _client(ApiConfig(rate_limit_requests=1, rate_limit_window_sec=60)) as c:
        r1 = await c.get("/api")
        r2 = await c.get("/api")
        r3 = await c.get("/")
    assert (r1.status_code, r2.status_code, r3.status_code) == (200, 200, 200)
    assert "x-ratelimit-limit" not in r1.headers
    assert "and the app itself allows 1 requests / 60 s per IP" in r1.text


@pytest.mark.asyncio
async def test_429_is_counted_as_an_error():
    async with _client(ApiConfig(rate_limit_requests=1, rate_limit_window_sec=60)) as c:
        await c.get("/api/state")
        await c.get("/api/state")
        counts = (await c.get("/api/status/errors", headers={"X-Real-IP": "198.51.100.9"})).json()
    assert counts["counts"].get("429") == 1


@pytest.mark.asyncio
async def test_limit_can_be_disabled():
    async with _client(ApiConfig(rate_limit_enabled=False, rate_limit_requests=1)) as c:
        r1 = await c.get("/api/status/errors")
        r2 = await c.get("/api/status/errors")
        page = await c.get("/api")
    assert (r1.status_code, r2.status_code) == (200, 200)
    assert "x-ratelimit-limit" not in r1.headers
    assert "the app itself allows" not in page.text
