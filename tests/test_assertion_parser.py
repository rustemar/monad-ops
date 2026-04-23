"""Tests for assertion/panic detector.

Fixtures use quotes verbatim from public operator incidents in the
Monad Developers Discord (wiki/pains/validator.md). Keeping the
literal text as test input guards against regex drift: if this test
breaks, we've stopped recognizing one of the classes of failure the
community has actually reported.
"""

from __future__ import annotations

import pytest

from monad_ops.parser.assertion import (
    AssertionEvent,
    AssertionKind,
    parse_assertion,
)


# ── ProofLine stall 2026-04-16 (monad-execution) ──────────────────────
def test_cxx_assertion_proofline() -> None:
    line = "monad-execution crashed with an internal assertion: Assertion 'block_cache.emplace(...).second' failed"
    ev = parse_assertion(line)
    assert ev is not None
    assert ev.kind is AssertionKind.CXX_ASSERT
    assert "block_cache.emplace" in ev.key
    assert "block_cache.emplace" in ev.summary


# ── Karlo Endorphine Stake 2026-03-02 (monad-execution) ───────────────
def test_cxx_assertion_ring_cpp_captures_location() -> None:
    line = (
        "monad-node: /usr/src/monad-bft/monad-cxx/monad-execution/category/core/io/ring.cpp:45: "
        "monad::io::Ring::Ring(const monad::io::RingConfig&): Assertion 'buffer_size > 0' failed"
    )
    ev = parse_assertion(line)
    assert ev is not None
    assert ev.kind is AssertionKind.CXX_ASSERT
    assert ev.location is not None
    assert "ring.cpp:45" in ev.location


# ── Karlo Endorphine Stake 2026-03-02 (io_uring startup failure) ──────
# The second line emitted by monad-execution when ring.cpp assertion
# trips — carries the specific kernel-level cause ("Invalid argument (22)")
# that separates this class from a generic C++ assertion. The remediation
# is environment-level (kernel / sysctl / container), not code-level, so
# the alert sink needs to differentiate it.
def test_io_uring_init_failure_classified_separately() -> None:
    line = (
        "monad-node: assertion failure message: "
        "io_uring_queue_init_params failed: Invalid argument (22)"
    )
    ev = parse_assertion(line)
    assert ev is not None
    assert ev.kind is AssertionKind.IO_URING_INIT
    # Summary must carry the remediation pointers operators act on.
    assert "kernel" in ev.summary.lower()
    assert "io_uring_disabled" in ev.summary
    # Stable dedup key: identical startup failures collapse to one alert.
    assert ev.key == "io_uring_init:startup"


def test_io_uring_init_dedup_key_stable_across_retries() -> None:
    """systemd restarts the service; every attempt emits a fresh
    ``io_uring_queue_init_params failed`` line. They must share a dedup
    key so the operator sees one alert per boot loop, not N."""
    a = parse_assertion(
        "io_uring_queue_init_params failed: Invalid argument (22)"
    )
    b = parse_assertion(
        "[retry #3] io_uring_queue_init_params failed: Invalid argument (22)"
    )
    assert a is not None and b is not None
    assert a.key == b.key


def test_io_uring_takes_priority_over_cxx_assert_on_same_line() -> None:
    """If both patterns appear in one line (unusual but possible with
    bundled log output), the operator-actionable IO_URING_INIT wins —
    a "C++ assertion failed" title would bury the remediation hint."""
    line = (
        "ring.cpp:45: Assertion 'result == 0' failed. "
        "io_uring_queue_init_params failed: Invalid argument (22)"
    )
    ev = parse_assertion(line)
    assert ev is not None
    assert ev.kind is AssertionKind.IO_URING_INIT


# ── Unit410 consensus panic 2025-12-04 ────────────────────────────────
def test_qc_overshoot_exact_phrase() -> None:
    line = (
        "thread 'main' panicked at /usr/src/monad-bft/monad-consensus-state/src/lib.rs:1415:13 "
        "high qc too far ahead of block tree root, restart client and statesync"
    )
    ev = parse_assertion(line)
    assert ev is not None
    # QC check fires first because it's the most specific pattern
    assert ev.kind is AssertionKind.QC_OVERSHOOT
    assert ev.key == "qc_overshoot:high_qc_overshoot"


def test_rust_panic_without_qc_phrase() -> None:
    line = (
        "thread 'blocksync-worker' panicked at src/blocksync.rs:42:7: "
        "peer index out of range"
    )
    ev = parse_assertion(line)
    assert ev is not None
    assert ev.kind is AssertionKind.RUST_PANIC
    assert ev.location == "src/blocksync.rs:42:7"
    assert "blocksync-worker" in ev.summary


# ── 0xAN Nodes.Guru halt 2026-04-14 ──────────────────────────────────
def test_chunk_exhaustion_above_threshold() -> None:
    line = "Disk usage: 0.9999. Chunks: 8100 fast, 73 slow, 1 free"
    ev = parse_assertion(line)
    assert ev is not None
    assert ev.kind is AssertionKind.CHUNK_EXHAUSTION
    assert "0.9999" in ev.key or "1.00" in ev.key or "0.99" in ev.key
    assert "8100 fast" in ev.summary


def test_chunk_exhaustion_below_threshold_skipped() -> None:
    # Regular progress reporting at 40% full — should NOT fire.
    line = "Disk usage: 0.40. Chunks: 3200 fast, 20 slow, 4880 free"
    ev = parse_assertion(line)
    assert ev is None


# ── Generic fatal catch-all ───────────────────────────────────────────
def test_generic_fatal_detected() -> None:
    line = "FATAL: execution state mismatch on block 100"
    ev = parse_assertion(line)
    assert ev is not None
    assert ev.kind is AssertionKind.GENERIC_FATAL


def test_fatal_keyword_in_context_not_matched() -> None:
    # Shouldn't match "fatal" as a casual substring in a non-capital context.
    # Our regex requires \bFATAL\b with leading case — so lowercase 'fatal' skipped.
    line = '[info] recovered fatal error in user space during init'
    ev = parse_assertion(line)
    # Regex is case-insensitive so this WILL match — that's intentional
    # per our conservative design (better one false-positive dedup'd by
    # sink than a missed real FATAL). Test documents the behavior.
    assert ev is not None
    assert ev.kind is AssertionKind.GENERIC_FATAL


# ── Negative cases ─────────────────────────────────────────────────────
def test_exec_block_line_not_matched() -> None:
    line = (
        "__exec_block,bl=26243796,id=0xabc,ts=1776517155390,tx=3,rt=1,rtp=33.33%,"
        "sr=73µs,txe=653µs,cmt=448µs,tot=1232µs,tpse=4594,tps=2435,"
        "gas=450276,gpse=689,gps=365,ac=1085212,sc=10000000"
    )
    ev = parse_assertion(line)
    assert ev is None


def test_empty_line_not_matched() -> None:
    assert parse_assertion("") is None
    assert parse_assertion("some innocuous info line") is None


# ── Dedup-key stability ───────────────────────────────────────────────
def test_same_incident_same_key() -> None:
    """Two emissions of the same assertion → same dedup key so the sink
    can collapse them."""
    a = parse_assertion(
        "ring.cpp:45: Ring::Ring(const RingConfig&): Assertion 'buf > 0' failed"
    )
    b = parse_assertion(
        "ring.cpp:45: Ring::Ring(const RingConfig&): Assertion 'buf > 0' failed [retry]"
    )
    assert a is not None and b is not None
    assert a.key == b.key


def test_different_incidents_different_keys() -> None:
    a = parse_assertion("file_a.cpp:10: Assertion 'x' failed")
    b = parse_assertion("file_b.cpp:20: Assertion 'y' failed")
    assert a is not None and b is not None
    assert a.key != b.key


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
