"""Tests for the receipts-enrichment pipeline."""

from __future__ import annotations

from pathlib import Path

import pytest

from monad_ops.enricher.receipts import (
    receipts_to_contract_block_rows,
    receipts_to_enrichment_rows,
)
from monad_ops.parser import ExecBlock
from monad_ops.storage import ContractBlockAgg, Storage, TxEnrichment


def _agg_from_tx(rows: list[TxEnrichment]) -> list[ContractBlockAgg]:
    """Helper: roll a list of per-tx rows up into (block, to_addr) aggregates.

    Lets the legacy per-tx test fixtures drive the new aggregate schema
    without rewriting every dict literal.
    """
    buckets: dict[tuple[int, str], list[int]] = {}
    for r in rows:
        if r.to_addr is None:
            continue
        k = (r.block_number, r.to_addr)
        if k in buckets:
            buckets[k][0] += 1
            buckets[k][1] += r.gas_used
        else:
            buckets[k] = [1, r.gas_used]
    return [
        ContractBlockAgg(block_number=bn, to_addr=a, tx_count=tx, total_gas=gas)
        for (bn, a), (tx, gas) in buckets.items()
    ]


def _mk_block(n: int, rtp: float = 0.0, tx: int = 2) -> ExecBlock:
    return ExecBlock(
        block_number=n, block_id=f"0x{n:064x}", timestamp_ms=1776_000_000_000 + n * 400,
        tx_count=tx, retried=int(tx * rtp / 100), retry_pct=rtp,
        state_reset_us=73, tx_exec_us=653, commit_us=448, total_us=1232,
        tps_effective=4594, tps_avg=2435, gas_used=450276,
        gas_per_sec_effective=689, gas_per_sec_avg=365,
        active_chunks=1085212, slow_chunks=10000000,
    )


def test_receipts_to_enrichment_rows_basic() -> None:
    raw = [
        {
            "transactionIndex": "0x0",
            "transactionHash": "0xaaa",
            "from": "0x1111111111111111111111111111111111111111",
            "to": "0x2222222222222222222222222222222222222222",
            "contractAddress": None,
            "status": "0x1",
            "gasUsed": "0x5208",
        },
        {
            "transactionIndex": "0x1",
            "transactionHash": "0xbbb",
            "from": "0x3333333333333333333333333333333333333333",
            "to": None,
            "contractAddress": "0x4444444444444444444444444444444444444444",
            "status": "0x1",
            "gasUsed": "0x186A0",
        },
    ]
    rows = receipts_to_enrichment_rows(block_number=42, receipts=raw)
    assert len(rows) == 2
    assert rows[0].tx_index == 0
    assert rows[0].to_addr == "0x2222222222222222222222222222222222222222"
    assert rows[0].contract_created is None
    assert rows[0].gas_used == 0x5208
    assert rows[1].to_addr is None
    assert rows[1].contract_created == "0x4444444444444444444444444444444444444444"


def test_receipts_to_enrichment_rows_skips_malformed() -> None:
    raw = [
        {
            "transactionIndex": "0x0",
            "transactionHash": "0xaaa",
            "from": "0x1",
            "to": "0x2",
            "status": "0x1",
            "gasUsed": "0x5208",
        },
        {"transactionIndex": "bogus"},  # malformed — should be skipped
    ]
    rows = receipts_to_enrichment_rows(42, raw)
    assert len(rows) == 1
    assert rows[0].tx_hash == "0xaaa"


def test_receipts_to_contract_block_rows_groups_by_to_addr() -> None:
    """Same-contract txs in one block collapse into a single aggregate row."""
    raw = [
        {"to": "0xAaaa", "gasUsed": "0x5208"},           # tx → A, 21000
        {"to": "0xaaaa", "gasUsed": "0x5208"},           # tx → A (case-fold), 21000
        {"to": "0xbbbb", "gasUsed": "0x2710"},           # tx → B, 10000
        {"to": None, "contractAddress": "0xccc",         # contract creation — dropped
         "gasUsed": "0x4e20"},
        {"to": "   ", "gasUsed": "0x1"},                 # empty after strip — dropped
    ]
    rows = receipts_to_contract_block_rows(block_number=7, receipts=raw)
    by_addr = {r.to_addr: r for r in rows}
    assert set(by_addr) == {"0xaaaa", "0xbbbb"}
    a = by_addr["0xaaaa"]
    assert a.tx_count == 2
    assert a.total_gas == 0x5208 * 2
    assert by_addr["0xbbbb"].tx_count == 1
    assert by_addr["0xbbbb"].total_gas == 0x2710


def test_receipts_to_contract_block_rows_skips_bad_gas() -> None:
    raw = [
        {"to": "0xaaaa", "gasUsed": "0x5208"},           # valid
        {"to": "0xbbbb", "gasUsed": "not-hex"},          # malformed — dropped
    ]
    rows = receipts_to_contract_block_rows(block_number=1, receipts=raw)
    assert {r.to_addr for r in rows} == {"0xaaaa"}


def test_tx_enrichment_round_trip(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "state.db")
    storage.write_block(_mk_block(100))
    rows = [
        TxEnrichment(100, 0, "0xaaa", "0x1", "0xabc", None, 1, 21000),
        TxEnrichment(100, 1, "0xbbb", "0x2", "0xabc", None, 1, 30000),
    ]
    n = storage.write_tx_enrichment(rows)
    assert n == 2
    assert storage.tx_enrichment_count() == 2
    assert storage.enrichment_has_block(100) is True
    assert storage.enrichment_has_block(999) is False

    # idempotent re-insert
    storage.write_tx_enrichment(rows)
    assert storage.tx_enrichment_count() == 2
    storage.close()


def test_top_retried_contracts_aggregation(tmp_path: Path) -> None:
    """contract A appears in 2 clean and 3 retried blocks (score: 3/5).
    contract B appears in 3 clean and 0 retried (score: 0/3).
    contract C appears once only (below min_appearances)."""
    storage = Storage(tmp_path / "state.db")

    # five clean blocks: 100, 200 (A); 500, 600, 700 (B)
    # three retried blocks: 300, 400, 800 (A in each; also 400 has B)
    clean = [(100, "A"), (200, "A"), (500, "B"), (600, "B"), (700, "B")]
    retried = [(300, "A"), (400, "A"), (800, "A")]
    also_in_400 = ("400", "B")

    blocks_seen: dict[int, float] = {}
    for bn, _ in clean:
        blocks_seen[bn] = 0.0
    for bn, _ in retried:
        blocks_seen[bn] = 40.0

    for bn, rtp in blocks_seen.items():
        storage.write_block(_mk_block(bn, rtp=rtp, tx=2))

    addr_map = {
        "A": "0xaaaa000000000000000000000000000000000000",
        "B": "0xbbbb000000000000000000000000000000000000",
        "C": "0xcccc000000000000000000000000000000000000",
    }

    rows: list[TxEnrichment] = []
    for bn, a in clean + retried:
        rows.append(TxEnrichment(bn, 0, f"0x{bn:x}00", "0xff", addr_map[a], None, 1, 21000))
    # B also appears in block 400 (tx_index 1)
    rows.append(TxEnrichment(400, 1, "0x400b", "0xff", addr_map["B"], None, 1, 21000))
    # C appears once in block 100
    rows.append(TxEnrichment(100, 1, "0x100c", "0xff", addr_map["C"], None, 1, 21000))

    storage.write_tx_contract_block(_agg_from_tx(rows))

    stats = storage.top_retried_contracts(min_appearances=3, limit=10)
    by_addr = {s.to_addr: s for s in stats}

    a = by_addr[addr_map["A"]]
    b = by_addr[addr_map["B"]]
    assert addr_map["C"] not in by_addr  # below min_appearances

    assert a.blocks_appeared == 5
    assert a.retried_blocks == 3
    assert abs(a.avg_rtp_of_blocks - (40 * 3 / 5)) < 0.01
    assert b.blocks_appeared == 4   # 500, 600, 700, and 400
    assert b.retried_blocks == 1    # only 400 has rtp > 0

    # ordering: retried_blocks DESC
    assert stats[0].to_addr == addr_map["A"]
    storage.close()


def test_no_double_counting_when_contract_has_many_txs_per_block(tmp_path: Path) -> None:
    """Regression: a contract with multiple txs in the same retried block
    must not inflate retried_blocks or avg_rtp_of_blocks."""
    storage = Storage(tmp_path / "state.db")
    # block 100 rtp=40, block 200 rtp=0
    storage.write_block(_mk_block(100, rtp=40.0, tx=4))
    storage.write_block(_mk_block(200, rtp=0.0, tx=2))
    addr = "0xdead000000000000000000000000000000000000"
    # contract appears 3 times in block 100 and 2 times in block 200
    rows = [
        TxEnrichment(100, 0, "0x100a", "0xff", addr, None, 1, 1000),
        TxEnrichment(100, 1, "0x100b", "0xff", addr, None, 1, 2000),
        TxEnrichment(100, 2, "0x100c", "0xff", addr, None, 1, 3000),
        TxEnrichment(200, 0, "0x200a", "0xff", addr, None, 1, 500),
        TxEnrichment(200, 1, "0x200b", "0xff", addr, None, 1, 500),
    ]
    storage.write_tx_contract_block(_agg_from_tx(rows))

    stats = storage.top_retried_contracts(min_appearances=1, limit=10)
    assert len(stats) == 1
    s = stats[0]
    assert s.blocks_appeared == 2
    assert s.retried_blocks == 1                  # NOT 3 (same block)
    assert abs(s.avg_rtp_of_blocks - 20.0) < 0.01  # (40+0)/2, NOT weighted by tx count
    assert s.tx_count == 5
    assert s.total_gas == 7000
    storage.close()


def test_since_block_filter(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "state.db")
    for bn in [10, 20, 30, 40, 50]:
        storage.write_block(_mk_block(bn, rtp=50.0 if bn >= 30 else 0.0))
    addr = "0xdead000000000000000000000000000000000000"
    storage.write_tx_contract_block(_agg_from_tx([
        TxEnrichment(bn, 0, f"0x{bn:x}", "0xff", addr, None, 1, 1)
        for bn in [10, 20, 30, 40, 50]
    ]))

    stats = storage.top_retried_contracts(since_block=30, min_appearances=1, limit=10)
    assert len(stats) == 1
    assert stats[0].blocks_appeared == 3    # 30, 40, 50
    assert stats[0].retried_blocks == 3
    storage.close()


def test_since_ts_ms_filter(tmp_path: Path) -> None:
    """ts-based window for dashboard 'last hour' filter."""
    storage = Storage(tmp_path / "state.db")
    for bn in [10, 20, 30, 40, 50]:
        storage.write_block(_mk_block(bn, rtp=50.0 if bn >= 30 else 0.0))
    addr = "0xdead000000000000000000000000000000000000"
    storage.write_tx_contract_block(_agg_from_tx([
        TxEnrichment(bn, 0, f"0x{bn:x}", "0xff", addr, None, 1, 1)
        for bn in [10, 20, 30, 40, 50]
    ]))

    # _mk_block sets timestamp_ms = 1776_000_000_000 + bn * 400
    cutoff_ms = 1776_000_000_000 + 30 * 400   # inclusive
    stats = storage.top_retried_contracts(since_ts_ms=cutoff_ms, min_appearances=1, limit=10)
    assert len(stats) == 1
    assert stats[0].blocks_appeared == 3     # 30, 40, 50
    storage.close()


def test_until_ts_ms_and_until_block_upper_bounds(tmp_path: Path) -> None:
    """Upper bounds let us scope to a stress-test window [from, to]."""
    storage = Storage(tmp_path / "state.db")
    for bn in range(10, 70, 10):   # 10, 20, 30, 40, 50, 60
        storage.write_block(_mk_block(bn, rtp=50.0 if bn >= 30 else 0.0))
    addr = "0xdead000000000000000000000000000000000000"
    storage.write_tx_contract_block(_agg_from_tx([
        TxEnrichment(bn, 0, f"0x{bn:x}", "0xff", addr, None, 1, 1)
        for bn in range(10, 70, 10)
    ]))

    # bounded by block: [20, 50]
    stats = storage.top_retried_contracts(
        since_block=20, until_block=50, min_appearances=1, limit=10,
    )
    assert len(stats) == 1
    assert stats[0].blocks_appeared == 4   # 20, 30, 40, 50

    # bounded by ts: same window
    lo = 1776_000_000_000 + 20 * 400
    hi = 1776_000_000_000 + 50 * 400
    stats2 = storage.top_retried_contracts(
        since_ts_ms=lo, until_ts_ms=hi, min_appearances=1, limit=10,
    )
    assert len(stats2) == 1
    assert stats2[0].blocks_appeared == 4
    storage.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
