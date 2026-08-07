"""Tests for the severity policy of ``AssertionRule``.

The parser's job is recognising a critical line; this rule's job is
deciding whether that line pages an operator at 3am. Those are the two
halves of the same alert path and only the first half was covered, so a
severity regression here would have been silent.

Two contracts matter downstream and are pinned below: the rule passes
``AssertionEvent.key`` through untouched (``DedupingSink`` keys off it),
and any kind missing from the severity map still comes out CRITICAL.
"""

from __future__ import annotations

import dataclasses

import pytest

from monad_ops.parser.assertion import AssertionEvent, AssertionKind, parse_assertion
from monad_ops.rules import AssertionRule, CodeColor, Severity, code_color_for


def _event(kind: AssertionKind, summary: str = "something broke", location: str | None = None):
    return AssertionEvent(
        kind=kind,
        raw="raw log line",
        key=f"{kind.value}:test",
        location=location,
        summary=summary,
    )


# ── severity policy ───────────────────────────────────────────────────

@pytest.mark.parametrize(
    "kind",
    [
        AssertionKind.CXX_ASSERT,
        AssertionKind.RUST_PANIC,
        AssertionKind.QC_OVERSHOOT,
        AssertionKind.IO_URING_INIT,
        AssertionKind.EVENT_RING_MMAP,
        AssertionKind.GENERIC_FATAL,
    ],
)
def test_every_non_chunk_kind_is_critical(kind: AssertionKind) -> None:
    alert = AssertionRule().on_event(_event(kind))
    assert alert.severity is Severity.CRITICAL
    assert code_color_for(alert.severity) is CodeColor.RED


def test_unmapped_kind_still_pages() -> None:
    """A kind added to the parser but forgotten in the severity map must
    fail loud, not silently downgrade to WARN."""
    ev = dataclasses.replace(_event(AssertionKind.CXX_ASSERT), kind="brand_new_kind")
    assert AssertionRule().on_event(ev).severity is Severity.CRITICAL


# ── chunk exhaustion: the one graded kind ─────────────────────────────
# The parser only emits this class at >= 0.90 (_CHUNK_CRITICAL_RATIO),
# so the whole live range is WARN below 0.95 and CRITICAL at or above it.

@pytest.mark.parametrize(
    ("ratio", "expected"),
    [
        (0.90, Severity.WARN),
        (0.94, Severity.WARN),
        (0.9499, Severity.WARN),
        (0.95, Severity.CRITICAL),  # boundary is inclusive
        (0.9754, Severity.CRITICAL),
        (0.99, Severity.CRITICAL),
    ],
)
def test_chunk_exhaustion_escalates_at_95_percent(ratio: float, expected: Severity) -> None:
    summary = f"TrieDB chunk exhaustion: {ratio:.4f} used, 3158 fast chunks"
    alert = AssertionRule().on_event(_event(AssertionKind.CHUNK_EXHAUSTION, summary=summary))
    assert alert.severity is expected


def test_chunk_exhaustion_without_a_readable_ratio_stays_warn() -> None:
    """No ratio in the text means we can't prove it's past 0.95 — WARN
    rather than paging on an unparseable line."""
    ev = _event(AssertionKind.CHUNK_EXHAUSTION, summary="TrieDB chunk exhaustion")
    assert AssertionRule().on_event(ev).severity is Severity.WARN


# ── alert shape ───────────────────────────────────────────────────────

def test_dedup_key_passes_through_unchanged() -> None:
    ev = _event(AssertionKind.RUST_PANIC)
    alert = AssertionRule().on_event(ev)
    assert alert.key == ev.key
    assert alert.rule == "assertion"


def test_location_is_appended_to_detail_when_captured() -> None:
    ev = _event(
        AssertionKind.CXX_ASSERT,
        summary="C++ assertion failed: x > 0",
        location="ring.cpp:45",
    )
    detail = AssertionRule().on_event(ev).detail
    assert detail.startswith("C++ assertion failed: x > 0")
    assert "ring.cpp:45" in detail


def test_detail_is_just_the_summary_when_location_is_missing() -> None:
    ev = _event(AssertionKind.RUST_PANIC, summary="panic: boom")
    assert AssertionRule().on_event(ev).detail == "panic: boom"


def test_each_kind_gets_a_distinct_title() -> None:
    rule = AssertionRule()
    titles = {rule.on_event(_event(k)).title for k in AssertionKind}
    assert len(titles) == len(list(AssertionKind))
    assert all(t for t in titles)


# ── end to end, on lines the parser actually produced ─────────────────

def test_livelock_assertion_line_pages_red() -> None:
    """The 2026-07-19 BeeHive report: restarting monad-bft alone aborts
    monad-execution. That line must reach an operator as RED."""
    line = (
        "runloop_monad.cpp:215: propose_block(...): "
        "Assertion 'block_cache.emplace(block_id, BlockCacheEntry{...}).second' failed"
    )
    ev = parse_assertion(line)
    assert ev is not None

    alert = AssertionRule().on_event(ev)
    assert alert.severity is Severity.CRITICAL
    assert alert.title == "monad-execution assertion failed"
    assert "runloop_monad.cpp:215" in alert.detail


def test_chunk_exhaustion_line_round_trips_from_parser_to_warn() -> None:
    ev = parse_assertion("Disk usage: 0.9210. Chunks: 3158 fast")
    assert ev is not None
    assert ev.kind is AssertionKind.CHUNK_EXHAUSTION

    alert = AssertionRule().on_event(ev)
    assert alert.severity is Severity.WARN
    assert code_color_for(alert.severity) is CodeColor.ORANGE
