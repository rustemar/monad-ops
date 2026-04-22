"""Response-shape smoke tests for the FastAPI app.

Validates that every public endpoint returns the expected status code,
content-type, and (for JSON endpoints) top-level keys.  Does NOT
exercise business logic — that lives in test_storage / test_rules.

Uses httpx.AsyncClient with ASGITransport so the full ASGI stack
(middleware, exception handlers, static file mounts) is exercised
without a real TCP socket.
"""

from __future__ import annotations

from pathlib import Path

import httpx
import pytest

from monad_ops.api.app import build_app
from monad_ops.config import Config, NodeConfig
from monad_ops.state import State
from monad_ops.storage import Storage


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _minimal_config() -> Config:
    return Config(node=NodeConfig(name="test-node", rpc_url="http://127.0.0.1:9999"))


@pytest.fixture()
def state_with_storage(tmp_path: Path) -> State:
    storage = Storage(tmp_path / "state.db")
    state = State(storage=storage)
    return state


@pytest.fixture()
def client(state_with_storage: State) -> httpx.AsyncClient:
    app = build_app(state_with_storage, _minimal_config(), enricher=None, labels=None)
    transport = httpx.ASGITransport(app=app)
    return httpx.AsyncClient(transport=transport, base_url="http://testserver")


# ---------------------------------------------------------------------------
# JSON endpoint shapes
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_healthz_shape(client: httpx.AsyncClient) -> None:
    r = await client.get("/healthz")
    assert r.status_code == 200
    body = r.json()
    assert body == {"ok": True}


@pytest.mark.asyncio
async def test_api_state_shape(client: httpx.AsyncClient) -> None:
    r = await client.get("/api/state")
    assert r.status_code == 200
    body = r.json()
    # Top-level keys that the dashboard depends on.
    expected_keys = {
        "node_name", "started_at", "uptime_sec", "blocks_seen",
        "last_block", "last_block_seen_ms",
        "blocks_per_sec_1m", "rtp_avg_1m", "rtp_avg_5m", "rtp_max_1m",
        "tx_per_sec_1m", "gas_per_sec_1m",
        "tps_effective_peak_1m", "tps_effective_avg_1m", "tps_eff_peak_block",
        "gas_per_sec_effective_peak_1m", "gas_eff_peak_block",
        "reorg_count", "last_reorg_number", "last_reorg_old_id",
        "last_reorg_new_id", "last_reorg_ts_ms",
        "reference_block", "reference_checked_ms", "reference_error",
        "reference_local_at_sample",
        "current_alerts", "epoch",
    }
    assert expected_keys <= set(body.keys())
    assert body["node_name"] == "test-node"
    # Epoch is a nested dict.
    assert isinstance(body["epoch"], dict)
    assert {"number", "blocks_in", "typical_length", "eta_sec"} <= set(body["epoch"].keys())


@pytest.mark.asyncio
async def test_enrichment_status_disabled(client: httpx.AsyncClient) -> None:
    r = await client.get("/api/enrichment/status")
    assert r.status_code == 200
    body = r.json()
    assert body["enabled"] is False


@pytest.mark.asyncio
async def test_api_blocks_returns_list(client: httpx.AsyncClient) -> None:
    r = await client.get("/api/blocks")
    assert r.status_code == 200
    assert isinstance(r.json(), list)


@pytest.mark.asyncio
async def test_api_alerts_returns_list(client: httpx.AsyncClient) -> None:
    r = await client.get("/api/alerts")
    assert r.status_code == 200
    assert isinstance(r.json(), list)


@pytest.mark.asyncio
async def test_api_alerts_history_returns_dict(client: httpx.AsyncClient) -> None:
    r = await client.get("/api/alerts/history")
    assert r.status_code == 200
    body = r.json()
    assert "count" in body
    assert "alerts" in body
    assert isinstance(body["alerts"], list)


@pytest.mark.asyncio
async def test_api_reorgs_returns_dict(client: httpx.AsyncClient) -> None:
    r = await client.get("/api/reorgs")
    assert r.status_code == 200
    body = r.json()
    assert "count" in body
    assert "reorgs" in body


@pytest.mark.asyncio
async def test_api_probes_public_shape(client: httpx.AsyncClient) -> None:
    r = await client.get("/api/probes/public")
    assert r.status_code == 200
    body = r.json()
    assert "probes" in body
    assert isinstance(body["probes"], list)


@pytest.mark.asyncio
async def test_api_contracts_labels_shape(client: httpx.AsyncClient) -> None:
    r = await client.get("/api/contracts/labels")
    assert r.status_code == 200
    body = r.json()
    assert "count" in body
    assert "labels" in body


# ---------------------------------------------------------------------------
# HEAD requests return 200 with empty body
# ---------------------------------------------------------------------------

_HEAD_ENDPOINTS = [
    "/healthz",
    "/api/state",
    "/api/blocks",
    "/api/alerts",
    "/api/alerts/history",
    "/api/reorgs",
    "/api/probes/public",
    "/api/contracts/labels",
    "/api/enrichment/status",
]


@pytest.mark.asyncio
@pytest.mark.parametrize("path", _HEAD_ENDPOINTS)
async def test_head_returns_200(client: httpx.AsyncClient, path: str) -> None:
    r = await client.head(path)
    assert r.status_code == 200
    # HEAD responses must not have a body.
    assert r.content == b""


# ---------------------------------------------------------------------------
# Validation: bad query params return 422
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_alerts_history_bad_severity_returns_422(client: httpx.AsyncClient) -> None:
    r = await client.get("/api/alerts/history", params={"severity": "foo"})
    assert r.status_code == 422


@pytest.mark.asyncio
async def test_blocks_bad_limit_returns_422(client: httpx.AsyncClient) -> None:
    r = await client.get("/api/blocks", params={"limit": "0"})
    assert r.status_code == 422


# ---------------------------------------------------------------------------
# HTML pages
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_root_returns_html(client: httpx.AsyncClient) -> None:
    r = await client.get("/")
    assert r.status_code == 200
    assert "text/html" in r.headers["content-type"]


@pytest.mark.asyncio
async def test_alerts_page_returns_html(client: httpx.AsyncClient) -> None:
    r = await client.get("/alerts")
    assert r.status_code == 200
    assert "text/html" in r.headers["content-type"]


@pytest.mark.asyncio
async def test_api_docs_page_returns_html(client: httpx.AsyncClient) -> None:
    r = await client.get("/api")
    assert r.status_code == 200
    assert "text/html" in r.headers["content-type"]


# ---------------------------------------------------------------------------
# Static well-known files
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_robots_txt(client: httpx.AsyncClient) -> None:
    r = await client.get("/robots.txt")
    assert r.status_code == 200
    assert "text/plain" in r.headers["content-type"]


@pytest.mark.asyncio
async def test_sitemap_xml(client: httpx.AsyncClient) -> None:
    r = await client.get("/sitemap.xml")
    assert r.status_code == 200
    assert "xml" in r.headers["content-type"]


@pytest.mark.asyncio
async def test_security_txt(client: httpx.AsyncClient) -> None:
    r = await client.get("/.well-known/security.txt")
    assert r.status_code == 200
    assert "text/plain" in r.headers["content-type"]


# ---------------------------------------------------------------------------
# 404 handling
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_unknown_path_returns_404(client: httpx.AsyncClient) -> None:
    r = await client.get("/nonexistent")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_api_404_returns_json(client: httpx.AsyncClient) -> None:
    r = await client.get("/api/nonexistent")
    assert r.status_code == 404
    body = r.json()
    assert "detail" in body


# ---------------------------------------------------------------------------
# Cache behavior — TTL hit/miss + key isolation
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_api_state_cache_collapses_concurrent_calls(
    state_with_storage: State,
) -> None:
    """Repeated /api/state hits within TTL share a single snapshot.

    Counts how many times state.snapshot() runs across two near-
    simultaneous requests. With a 1s TTL and no real time elapsing,
    the second request must hit the cache.
    """
    counter = {"snapshot": 0}
    original = state_with_storage.snapshot
    def _wrapped():
        counter["snapshot"] += 1
        return original()
    state_with_storage.snapshot = _wrapped
    app = build_app(state_with_storage, _minimal_config(), enricher=None, labels=None)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r1 = await c.get("/api/state")
        r2 = await c.get("/api/state")
    assert r1.status_code == 200
    assert r2.status_code == 200
    assert r1.json() == r2.json()
    assert counter["snapshot"] == 1


@pytest.mark.asyncio
async def test_api_alerts_cache_isolates_by_limit(
    state_with_storage: State,
) -> None:
    """Different ?limit= values get different cache slots."""
    counter = {"calls": 0}
    original = state_with_storage.recent_alerts_with_ts
    def _wrapped(limit: int = 50):
        counter["calls"] += 1
        return original(limit=limit)
    state_with_storage.recent_alerts_with_ts = _wrapped
    app = build_app(state_with_storage, _minimal_config(), enricher=None, labels=None)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        await c.get("/api/alerts?limit=5")
        await c.get("/api/alerts?limit=5")    # cache hit
        await c.get("/api/alerts?limit=10")   # cache miss — different key
        await c.get("/api/alerts?limit=10")   # cache hit
    # Two distinct cache keys → two underlying calls.
    assert counter["calls"] == 2


@pytest.mark.asyncio
async def test_api_probes_public_cached(
    state_with_storage: State,
) -> None:
    """/api/probes and /api/probes/public have independent cache slots
    so the sanitized public payload never bleeds into the internal one
    (or vice versa) just because both wrap state.probes().
    """
    counter = {"calls": 0}
    original = state_with_storage.probes
    def _wrapped():
        counter["calls"] += 1
        return original()
    state_with_storage.probes = _wrapped
    app = build_app(state_with_storage, _minimal_config(), enricher=None, labels=None)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r1 = await c.get("/api/probes")
        r2 = await c.get("/api/probes")          # public cache hit
        r3 = await c.get("/api/probes/public")   # separate bucket
        r4 = await c.get("/api/probes/public")   # public cache hit
    assert r1.json() == r2.json()
    assert r3.json() == r4.json()
    # /api/probes carries `details` (operator-sensitive); public must not.
    assert "details" in r1.json()["probes"][0] if r1.json()["probes"] else True
    assert all("details" not in p for p in r3.json()["probes"])
    # Two buckets, each computed once.
    assert counter["calls"] == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
