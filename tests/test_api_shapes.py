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
from monad_ops.rules.events import AlertEvent, Severity
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
        "current_alerts", "epoch", "consensus",
    }
    assert expected_keys <= set(body.keys())
    assert body["node_name"] == "test-node"
    # Epoch is a nested dict.
    assert isinstance(body["epoch"], dict)
    assert {"number", "blocks_in", "typical_length", "eta_sec"} <= set(body["epoch"].keys())
    # Consensus is a nested dict — Foundation's <3% headline KPI plus
    # the operator-side complement (this node's pacemaker fires).
    assert isinstance(body["consensus"], dict)
    assert {
        "validator_timeout_pct_5m",
        "local_timeout_per_min_5m",
        "rounds_observed_5m",
        "local_timeouts_5m",
    } <= set(body["consensus"].keys())


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
async def test_base_fee_series_endpoint_shape(client: httpx.AsyncClient) -> None:
    """``/api/base_fee_series`` returns a downsampled per-block series.
    Smoke against fresh DB — empty bins, valid envelope."""
    r = await client.get("/api/base_fee_series?from_ts_ms=1000&to_ts_ms=60001")
    assert r.status_code == 200
    body = r.json()
    assert {"from_ts_ms", "to_ts_ms", "bin_ms", "bins"} <= set(body.keys())
    assert isinstance(body["bins"], list)


@pytest.mark.asyncio
async def test_window_summary_includes_base_fee_aggregate(
    client: httpx.AsyncClient,
) -> None:
    """``/api/window_summary`` carries a ``base_fee`` block with
    avg/min/max in gwei. Empty DB → all zeros, matching the consensus
    block's empty contract."""
    r = await client.get("/api/window_summary?from_ts_ms=1000&to_ts_ms=2000")
    assert r.status_code == 200
    body = r.json()
    assert "base_fee" in body
    base = body["base_fee"]
    assert {"samples", "base_fee_gwei_avg", "base_fee_gwei_min",
            "base_fee_gwei_max"} <= set(base.keys())
    assert base["samples"] == 0


@pytest.mark.asyncio
async def test_bft_series_endpoint_shape(client: httpx.AsyncClient) -> None:
    """``/api/bft_series`` returns a per-minute series for the chart.
    Smoke against fresh DB — empty bins, valid envelope."""
    r = await client.get("/api/bft_series?from_ts_ms=1000&to_ts_ms=60001")
    assert r.status_code == 200
    body = r.json()
    assert {"from_ts_ms", "to_ts_ms", "bin_ms", "bins"} <= set(body.keys())
    assert body["bin_ms"] == 60_000
    assert isinstance(body["bins"], list)


@pytest.mark.asyncio
async def test_bft_series_rejects_oversized_span(client: httpx.AsyncClient) -> None:
    """Span > 33 hours trips the storage cap; better to return 400 than
    an empty payload that would silently mislead a debugger."""
    r = await client.get("/api/bft_series?from_ts_ms=0&to_ts_ms=999999999999")
    assert r.status_code == 400


@pytest.mark.asyncio
async def test_window_summary_includes_consensus_aggregate(
    state_with_storage: State,
    client: httpx.AsyncClient,
) -> None:
    """``/api/window_summary`` carries a ``consensus`` block with the
    chain-wide validator-timeout %. Smoke-test against a fresh DB
    (no bft data) — the aggregate path must return zeros, not 500."""
    # Window must be > 0 span to pass the validator.
    r = await client.get("/api/window_summary?from_ts_ms=1000&to_ts_ms=2000")
    assert r.status_code == 200
    body = r.json()
    assert "consensus" in body
    consensus = body["consensus"]
    assert {
        "rounds_total", "rounds_tc", "local_timeouts",
        "validator_timeout_pct", "local_timeout_per_min",
        "minutes",
    } <= set(consensus.keys())
    # Empty DB → all zeros, no NaN, no None.
    assert consensus["rounds_total"] == 0
    assert consensus["validator_timeout_pct"] == 0.0


@pytest.mark.asyncio
async def test_api_alerts_history_returns_dict(client: httpx.AsyncClient) -> None:
    r = await client.get("/api/alerts/history")
    assert r.status_code == 200
    body = r.json()
    assert "count" in body
    assert "alerts" in body
    assert isinstance(body["alerts"], list)


@pytest.mark.asyncio
async def test_alert_endpoints_carry_code_color(
    state_with_storage: State,
) -> None:
    """Every alert payload served by the API must carry a ``code_color``
    field mapping severity onto the Foundation's 2026-03-26
    colour-code vocabulary. Operators read Foundation announcements in
    that language (CODE RED / ORANGE / GREEN) — aligning the local
    dashboard and ``/api/alerts*`` payloads with the same chips removes
    a translation step during incidents.
    """
    # One alert per severity so the mapping is exercised end-to-end.
    state_with_storage.add_alert(AlertEvent(
        rule="assertion", severity=Severity.CRITICAL,
        key="k-crit", title="halt", detail="",
    ))
    state_with_storage.add_alert(AlertEvent(
        rule="retry_spike", severity=Severity.WARN,
        key="k-warn", title="spike", detail="",
    ))
    state_with_storage.add_alert(AlertEvent(
        rule="retry_spike", severity=Severity.RECOVERED,
        key="k-rec", title="normalized", detail="",
    ))
    state_with_storage.add_alert(AlertEvent(
        rule="reference_lag", severity=Severity.INFO,
        key="k-info", title="info", detail="",
    ))

    app = build_app(
        state_with_storage, _minimal_config(), enricher=None, labels=None,
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        live = (await c.get("/api/alerts")).json()
        history = (await c.get("/api/alerts/history")).json()["alerts"]
        state_body = (await c.get("/api/state")).json()

    expected = {
        "critical": "red",
        "warn": "orange",
        "recovered": "green",
        "info": "green",
    }
    for payload_name, payload in (
        ("live", live), ("history", history), ("state.current_alerts", state_body["current_alerts"]),
    ):
        assert payload, f"{payload_name} empty — test setup issue"
        for a in payload:
            assert "code_color" in a, f"missing code_color in {payload_name}: {a}"
            assert a["code_color"] == expected[a["severity"]], (
                f"mismatch in {payload_name}: sev={a['severity']} "
                f"code_color={a['code_color']} (expected {expected[a['severity']]})"
            )


@pytest.mark.asyncio
async def test_api_reorgs_returns_dict(client: httpx.AsyncClient) -> None:
    r = await client.get("/api/reorgs")
    assert r.status_code == 200
    body = r.json()
    assert "count" in body
    assert "reorgs" in body


@pytest.mark.asyncio
async def test_api_reorg_journal_404_without_artifact(
    state_with_storage: State, tmp_path: Path,
) -> None:
    """Endpoint returns 404 when no artifact has been captured yet."""
    journal_dir = tmp_path / "reorgs"
    app = build_app(
        state_with_storage, _minimal_config(),
        enricher=None, labels=None, journal_capture_dir=journal_dir,
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as c:
        r = await c.get("/api/reorgs/12345/journal")
        assert r.status_code == 404
        assert "error" in r.json()


@pytest.mark.asyncio
async def test_api_reorg_journal_serves_gzipped_artifact(
    state_with_storage: State, tmp_path: Path,
) -> None:
    """Endpoint serves the gzipped JSONL with an attachment filename."""
    import gzip
    journal_dir = tmp_path / "reorgs"
    journal_dir.mkdir(parents=True)
    payload = b'{"hello":"world"}\n'
    artifact = journal_dir / "12345-1700000000.jsonl.gz"
    with gzip.open(artifact, "wb") as fh:
        fh.write(payload)

    app = build_app(
        state_with_storage, _minimal_config(),
        enricher=None, labels=None, journal_capture_dir=journal_dir,
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as c:
        r = await c.get("/api/reorgs/12345/journal")
        assert r.status_code == 200
        # FileResponse adds a Content-Disposition with the filename hint.
        cd = r.headers.get("content-disposition", "")
        assert "reorg-12345.jsonl.gz" in cd
        assert gzip.decompress(r.content) == payload


@pytest.mark.asyncio
async def test_api_reorg_trace_partial_post_window_uses_short_ttl(
    state_with_storage: State,
) -> None:
    """When the post-event window is truncated (tailer hasn't caught up
    to ``block_number + window`` yet), the cache slot must use the
    short TTL so the next call returns the full trace, not a five-minute
    stale partial."""
    from monad_ops.parser import ExecBlock
    from monad_ops.rules.events import AlertEvent, Severity

    storage = state_with_storage.storage
    # Seed blocks 95..102 — leaves the post-side of a window=5 trace
    # truncated for reorg block 100 (only 100..102 ingested, missing
    # 103..105).
    def mk(n: int) -> ExecBlock:
        return ExecBlock(
            block_number=n, block_id=f"0x{n:064x}",
            timestamp_ms=1_700_000_000_000 + n * 400,
            tx_count=1, retried=0, retry_pct=0.0,
            state_reset_us=0, tx_exec_us=0, commit_us=0, total_us=0,
            tps_effective=0, tps_avg=0, gas_used=0,
            gas_per_sec_effective=0, gas_per_sec_avg=0,
            active_chunks=0, slow_chunks=0,
        )
    for n in range(95, 103):
        storage.write_block(mk(n))
    storage.write_alert(AlertEvent(
        rule="reorg",
        severity=Severity.CRITICAL,
        key=f"reorg:100:{'0x' + 'a' * 64}",
        title="Chain reorg detected",
        detail="Block #100 id changed: 0xaaaa…aaaa → 0xbbbb…bbbb. "
               "Total reorgs observed: 1.",
    ))

    app = build_app(state_with_storage, _minimal_config(), enricher=None, labels=None)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as c:
        r = await c.get("/api/reorgs/100", params={"window": 5, "level": "full"})
        assert r.status_code == 200

    # Cache shape is {bucket: {key: (stored_at, ttl, value)}}.
    bucket_store = app.state.cache_store["reorg_trace"]
    assert bucket_store, "expected at least one cached reorg trace"
    (_stored_at, ttl_sec, value) = next(iter(bucket_store.values()))
    # Partial TTL is 5 s by definition (see _REORG_TRACE_PARTIAL_TTL).
    assert ttl_sec == pytest.approx(5.0)
    # Sanity: only 8 blocks were ingested (95..102), so the post side is
    # truly truncated for window=5 (would need 95..105).
    assert len(value["blocks"]) == 8


@pytest.mark.asyncio
async def test_api_reorg_trace_complete_window_uses_long_ttl(
    state_with_storage: State,
) -> None:
    """A trace whose post-event window is fully populated stays cached
    at the full 5-minute TTL — historical reorgs are immutable."""
    from monad_ops.parser import ExecBlock
    from monad_ops.rules.events import AlertEvent, Severity

    storage = state_with_storage.storage
    def mk(n: int) -> ExecBlock:
        return ExecBlock(
            block_number=n, block_id=f"0x{n:064x}",
            timestamp_ms=1_700_000_000_000 + n * 400,
            tx_count=1, retried=0, retry_pct=0.0,
            state_reset_us=0, tx_exec_us=0, commit_us=0, total_us=0,
            tps_effective=0, tps_avg=0, gas_used=0,
            gas_per_sec_effective=0, gas_per_sec_avg=0,
            active_chunks=0, slow_chunks=0,
        )
    # Fully ingested 95..105 — covers ±5 around block 100.
    for n in range(95, 106):
        storage.write_block(mk(n))
    storage.write_alert(AlertEvent(
        rule="reorg", severity=Severity.CRITICAL,
        key=f"reorg:100:{'0x' + 'a' * 64}",
        title="Chain reorg detected",
        detail="Block #100 id changed: …",
    ))

    app = build_app(state_with_storage, _minimal_config(), enricher=None, labels=None)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as c:
        r = await c.get("/api/reorgs/100", params={"window": 5, "level": "full"})
        assert r.status_code == 200

    bucket_store = app.state.cache_store["reorg_trace"]
    (_stored_at, ttl_sec, value) = next(iter(bucket_store.values()))
    assert ttl_sec == pytest.approx(300.0)
    assert len(value["blocks"]) == 11  # 95..105 inclusive


@pytest.mark.asyncio
async def test_api_reorgs_marks_has_journal(
    state_with_storage: State, tmp_path: Path,
) -> None:
    """`/api/reorgs` annotates each row with has_journal based on which
    artifacts exist on disk."""
    import gzip
    from monad_ops.rules.events import AlertEvent, Severity

    # Two reorgs in storage; one has an artifact, one doesn't.
    # Use the storage write_alert path so the row has the canonical
    # `reorg:<n>:<id>` key shape that list_reorgs parses.
    storage = state_with_storage.storage
    for bn, bid in ((100, "0x" + "a" * 64), (200, "0x" + "b" * 64)):
        storage.write_alert(AlertEvent(
            rule="reorg",
            severity=Severity.CRITICAL,
            key=f"reorg:{bn}:{bid}",
            title="Chain reorg detected",
            detail=f"Block #{bn} id changed: 0xaaaa…aaaa → 0xbbbb…bbbb. "
                   "Total reorgs observed: 1.",
        ))

    journal_dir = tmp_path / "reorgs"
    journal_dir.mkdir(parents=True)
    # Only block 100 has a captured artifact.
    with gzip.open(journal_dir / "100-1700000000.jsonl.gz", "wb") as fh:
        fh.write(b'{"x":1}\n')

    app = build_app(
        state_with_storage, _minimal_config(),
        enricher=None, labels=None, journal_capture_dir=journal_dir,
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as c:
        r = await c.get("/api/reorgs")
        assert r.status_code == 200
        body = r.json()
        by_block = {row["block_number"]: row for row in body["reorgs"]}
        assert by_block[100]["has_journal"] is True
        assert by_block[200]["has_journal"] is False


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


# ---------------------------------------------------------------------------
# Detail-popup endpoints
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_api_block_detail_404_on_missing(client: httpx.AsyncClient) -> None:
    # Empty storage — any block_number is absent.
    r = await client.get("/api/blocks/12345")
    assert r.status_code == 404
    body = r.json()
    assert "error" in body


@pytest.mark.asyncio
async def test_api_block_detail_shape_with_data(
    state_with_storage, client: httpx.AsyncClient
) -> None:
    from monad_ops.parser import ExecBlock
    from monad_ops.storage import ContractBlockAgg

    def mk(n, rtp=0.0, tx=10, retried=0):
        return ExecBlock(
            block_number=n, block_id=f"0x{n:064x}",
            timestamp_ms=1776_000_000_000 + n * 400,
            tx_count=tx, retried=retried, retry_pct=rtp,
            state_reset_us=73, tx_exec_us=653, commit_us=448, total_us=1232,
            tps_effective=4594, tps_avg=2435, gas_used=450276,
            gas_per_sec_effective=689, gas_per_sec_avg=365,
            active_chunks=1, slow_chunks=0,
        )
    # Seed blocks 100..104 and a per-(block, contract) row for block 102.
    for n in range(100, 105):
        state_with_storage.storage.write_block(mk(n, rtp=50.0 if n == 102 else 0.0, tx=10, retried=5 if n == 102 else 0))
    state_with_storage.storage.write_tx_contract_block([
        ContractBlockAgg(block_number=102, to_addr="0x" + "ab" * 20, tx_count=7, total_gas=1_000_000),
        ContractBlockAgg(block_number=102, to_addr="0x" + "cd" * 20, tx_count=3, total_gas=500_000),
    ])
    r = await client.get("/api/blocks/102", params={"neighbor_window": 2, "top_contracts_limit": 5})
    assert r.status_code == 200
    body = r.json()
    assert set(body.keys()) >= {"block", "top_contracts", "neighbors", "reorg_near"}
    assert body["block"]["n"] == 102
    assert body["block"]["rtp"] == 50.0
    assert len(body["top_contracts"]) == 2
    # Ordered by tx_count DESC; share computed on block.tx_count.
    assert body["top_contracts"][0]["tx_count"] == 7
    assert body["top_contracts"][0]["share"] == 0.7
    # Label is None because no labels registry was provided to build_app.
    assert body["top_contracts"][0]["label"] is None
    # Neighbors span requested window, inclusive.
    assert len(body["neighbors"]) == 5  # 100..104
    assert body["reorg_near"] is None


@pytest.mark.asyncio
async def test_api_block_detail_negative_returns_400(client: httpx.AsyncClient) -> None:
    # FastAPI path param validates int, so negative is accepted only via
    # signed path — our handler checks ``< 0`` explicitly.
    r = await client.get("/api/blocks/-1")
    # Starlette rejects the negative path at routing time (int coerce),
    # returning 404. Either 400 or 404 is an acceptable "not valid" signal.
    assert r.status_code in (400, 404, 422)


@pytest.mark.asyncio
async def test_api_contract_detail_bad_addr_returns_400(client: httpx.AsyncClient) -> None:
    r = await client.get("/api/contracts/not-an-addr")
    assert r.status_code == 400
    assert "error" in r.json()


@pytest.mark.asyncio
async def test_api_contract_detail_shape_empty_window(client: httpx.AsyncClient) -> None:
    # No data — endpoint still returns the full shape with zeros.
    r = await client.get(
        "/api/contracts/0x" + "0" * 40,
        params={"hours": 1},
    )
    assert r.status_code == 200
    body = r.json()
    for k in ("to_addr", "window", "stats", "dominance", "peak_block", "rank", "hourly", "label", "category"):
        assert k in body
    assert body["to_addr"] == "0x" + "0" * 40
    assert body["stats"]["blocks_appeared"] == 0
    assert body["stats"]["tx_count"] == 0
    assert body["peak_block"] is None
    # Dominance buckets present (3 defaults) but all zero.
    assert len(body["dominance"]) == 3
    for d in body["dominance"]:
        assert d["blocks"] == 0
        assert d["avg_rtp"] == 0.0


@pytest.mark.asyncio
async def test_api_contract_detail_invalid_window(client: httpx.AsyncClient) -> None:
    # to_ts_ms < from_ts_ms → 400.
    r = await client.get(
        "/api/contracts/0x" + "0" * 40,
        params={"from_ts_ms": 2000, "to_ts_ms": 1000},
    )
    assert r.status_code == 400


@pytest.mark.asyncio
async def test_api_contract_detail_empty_hides_rank(
    client: httpx.AsyncClient,
) -> None:
    # iter-11 audit G1-2: rank should be null on an empty contract.
    r = await client.get(
        "/api/contracts/0x" + "0" * 40, params={"hours": 1}
    )
    assert r.status_code == 200
    body = r.json()
    assert body["rank"] is None
    assert body["pattern"] is None


@pytest.mark.asyncio
async def test_api_contract_detail_suppresses_pattern_for_system(
    state_with_storage, tmp_path,
) -> None:
    # Build an app with a labels registry that marks our test address
    # as category=system. With 60 blocks of 1 tx each (uniform hit),
    # storage would flag pattern; API should suppress it.
    from monad_ops.api.app import build_app
    from monad_ops.labels import ContractLabels, Label
    from monad_ops.storage import ContractBlockAgg
    from monad_ops.parser import ExecBlock

    sys_addr = "0x" + "ab" * 20
    labels = ContractLabels({sys_addr: Label(name="Test System", category="system")})

    def mk(n, rtp=0.0, tx=10, retried=0):
        return ExecBlock(
            block_number=n, block_id=f"0x{n:064x}",
            timestamp_ms=1776_000_000_000 + n * 400, tx_count=tx,
            retried=retried, retry_pct=rtp,
            state_reset_us=1, tx_exec_us=1, commit_us=1, total_us=3,
            tps_effective=1, tps_avg=1, gas_used=1,
            gas_per_sec_effective=1, gas_per_sec_avg=1,
            active_chunks=0, slow_chunks=0,
        )
    for n in range(100, 160):
        state_with_storage.storage.write_block(mk(n, rtp=10.0))
        state_with_storage.storage.write_tx_contract_block([
            ContractBlockAgg(block_number=n, to_addr=sys_addr, tx_count=1, total_gas=21_000),
        ])

    app = build_app(state_with_storage, _minimal_config(), enricher=None, labels=labels)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://t"
    ) as c:
        # Fixture timestamps live around 1776_000_000_000; use a 1h
        # window that covers all 60 blocks (~24s span) and stays inside
        # the 30-day API cap.
        base = 1776_000_000_000
        r = await c.get(
            f"/api/contracts/{sys_addr}",
            params={"from_ts_ms": base, "to_ts_ms": base + 3_600_000},
        )
        assert r.status_code == 200
        body = r.json()
        assert body["category"] == "system"
        # Raw storage would flag pattern=uniform here; API suppressed it.
        assert body["pattern"] is None


@pytest.mark.asyncio
async def test_api_detail_endpoints_set_cache_header(
    state_with_storage, client: httpx.AsyncClient,
) -> None:
    # iter-11 audit G1-4: Cache-Control on the two detail endpoints so
    # repeat opens within 30s are served by the browser cache.
    from monad_ops.parser import ExecBlock
    b = ExecBlock(
        block_number=1234, block_id="0x" + "a" * 64,
        timestamp_ms=1776_000_000_000, tx_count=1, retried=0,
        retry_pct=0.0, state_reset_us=1, tx_exec_us=1, commit_us=1,
        total_us=3, tps_effective=1, tps_avg=1, gas_used=1,
        gas_per_sec_effective=1, gas_per_sec_avg=1, active_chunks=0,
        slow_chunks=0,
    )
    state_with_storage.storage.write_block(b)
    r = await client.get("/api/blocks/1234")
    assert r.status_code == 200
    assert r.headers.get("cache-control") == "private, max-age=30"

    r = await client.get("/api/contracts/0x" + "0" * 40, params={"hours": 1})
    assert r.status_code == 200
    assert r.headers.get("cache-control") == "private, max-age=30"


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
