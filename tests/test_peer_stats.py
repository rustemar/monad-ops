"""PeerTracker — in-memory rolling per-peer error counters."""

from __future__ import annotations

from monad_ops.collector.peer_stats import PeerTracker
from monad_ops.parser import ConsensusEvent, ConsensusEventKind


def _ev(kind: ConsensusEventKind, ts_ms: int, addr: str | None) -> ConsensusEvent:
    return ConsensusEvent(kind=kind, round=0, epoch=None, ts_ms=ts_ms, peer_addr=addr)


def test_counts_separate_decrypt_and_session() -> None:
    t = PeerTracker()
    base = 1_777_000_000_000
    t.on_event(_ev(ConsensusEventKind.NETWORK_DECRYPT_FAIL, base, "1.2.3.4:8001"))
    t.on_event(_ev(ConsensusEventKind.NETWORK_DECRYPT_FAIL, base + 1000, "1.2.3.4:8001"))
    t.on_event(_ev(ConsensusEventKind.NETWORK_SESSION_TIMEOUT, base + 2000, "1.2.3.4:8001"))
    rows = t.snapshot(window_min=60, now_ms=base + 60_000)
    assert len(rows) == 1
    assert rows[0]["decrypt"] == 2
    assert rows[0]["session"] == 1
    assert rows[0]["total"] == 3


def test_event_without_addr_is_ignored() -> None:
    t = PeerTracker()
    t.on_event(_ev(ConsensusEventKind.NETWORK_DECRYPT_FAIL, 1_000_000_000_000, None))
    assert t.snapshot() == []


def test_window_filters_stale_events() -> None:
    t = PeerTracker()
    base = 1_777_000_000_000
    t.on_event(_ev(ConsensusEventKind.NETWORK_DECRYPT_FAIL, base, "9.9.9.9:8001"))
    rows = t.snapshot(window_min=60, now_ms=base + 90 * 60_000)
    assert rows == []


def test_enrichment_merges_into_snapshot() -> None:
    t = PeerTracker()
    base = 1_777_000_000_000
    t.on_event(_ev(ConsensusEventKind.NETWORK_DECRYPT_FAIL, base, "1.2.3.4:8001"))
    t.set_enrichment("1.2.3.4", country="DE", latency_ms=20)
    rows = t.snapshot(window_min=60, now_ms=base + 60_000)
    assert rows[0]["country"] == "DE"
    assert rows[0]["latency_ms"] == 20


def test_sorted_by_total_desc() -> None:
    t = PeerTracker()
    base = 1_777_000_000_000
    t.on_event(_ev(ConsensusEventKind.NETWORK_DECRYPT_FAIL, base, "1.1.1.1:8001"))
    for i in range(5):
        t.on_event(_ev(ConsensusEventKind.NETWORK_DECRYPT_FAIL, base + i, "2.2.2.2:8001"))
    rows = t.snapshot(window_min=60, now_ms=base + 60_000)
    assert [r["ip"] for r in rows] == ["2.2.2.2", "1.1.1.1"]
