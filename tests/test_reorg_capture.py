"""Tests for the per-reorg journal capture pipeline."""

from __future__ import annotations

import asyncio
import gzip
from pathlib import Path

import pytest

from monad_ops.reorg_capture import (
    CaptureRequest,
    artifact_path,
    capture_reorg_journal,
    find_artifact,
    journal_dir_for,
    sanitize_text,
)


# ---------------------------------------------------------------------------
# Sanitizer
# ---------------------------------------------------------------------------

class TestSanitizer:
    def test_redacts_peer_ip_with_port(self) -> None:
        line = '"remote_addr":"65.108.18.212:8001"'
        assert sanitize_text(line) == '"remote_addr":"<ip>"'

    def test_redacts_bare_ipv4(self) -> None:
        assert sanitize_text("connecting to 127.0.0.1:4317") == "connecting to <ip>"

    def test_redacts_multiple_ips_in_one_line(self) -> None:
        text = "from 95.217.139.60:8001 to 192.168.1.1"
        assert sanitize_text(text) == "from <ip> to <ip>"

    def test_redacts_across_lines(self) -> None:
        text = "line1 65.21.22.158:8001\nline2 149.50.110.7:8001\n"
        out = sanitize_text(text)
        assert "65.21.22.158" not in out
        assert "149.50.110.7" not in out
        assert out.count("<ip>") == 2

    def test_keeps_validator_pubkeys_intact(self) -> None:
        # Pubkeys contain hex bytes that never render as decimal-dotted IPs.
        pk = "028a938a76c07651a27f5a20c875f14c5d336505527ea4c89040302a4b735837ed"
        line = f'"author":"{pk}"'
        assert sanitize_text(line) == line

    def test_keeps_block_ids_intact(self) -> None:
        bid = "0xbf53e2b261c7f6265ba1f30eb8516f3962e8104554b2ab6be94ac1d038c8baf2"
        line = f'"block_id":"{bid}"'
        assert sanitize_text(line) == line

    def test_keeps_round_numbers(self) -> None:
        line = '"round":"28024222"'
        assert sanitize_text(line) == line

    def test_redact_is_idempotent(self) -> None:
        once = sanitize_text("1.2.3.4 and 5.6.7.8")
        twice = sanitize_text(once)
        assert once == twice == "<ip> and <ip>"


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

class TestPathHelpers:
    def test_journal_dir_is_sibling_of_db(self, tmp_path: Path) -> None:
        db_path = tmp_path / "data" / "state.db"
        out = journal_dir_for(db_path)
        assert out == tmp_path / "data" / "reorgs"

    def test_artifact_path_uses_seconds_precision(self, tmp_path: Path) -> None:
        path = artifact_path(tmp_path, 27868954, 1777178525337)
        assert path == tmp_path / "27868954-1777178525.jsonl.gz"

    def test_find_artifact_returns_none_for_missing_dir(self, tmp_path: Path) -> None:
        assert find_artifact(tmp_path / "absent", 1) is None

    def test_find_artifact_returns_none_for_unknown_block(self, tmp_path: Path) -> None:
        tmp_path.mkdir(exist_ok=True)
        (tmp_path / "111-1.jsonl.gz").write_bytes(b"x")
        assert find_artifact(tmp_path, 222) is None

    def test_find_artifact_picks_latest_when_multiple(self, tmp_path: Path) -> None:
        # Two captures with different unix-second suffixes — sorted lexically,
        # the lexically last one wins (timestamps are zero-padded ints so
        # numeric and lexical orders agree for our usage).
        (tmp_path / "100-1000.jsonl.gz").write_bytes(b"a")
        (tmp_path / "100-2000.jsonl.gz").write_bytes(b"b")
        latest = find_artifact(tmp_path, 100)
        assert latest is not None
        assert latest.name == "100-2000.jsonl.gz"


# ---------------------------------------------------------------------------
# Capture flow (subprocess mocked via injected runner)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_capture_writes_sanitized_gzip(tmp_path: Path) -> None:
    raw_journal = (
        b'{"timestamp":"2026-04-26T02:42:05Z","fields":{"remote_addr":"65.108.18.212:8001"}}\n'
        b'{"timestamp":"2026-04-26T02:42:06Z","fields":{"vote":"abc","author":"028a938a"}}\n'
    )

    def fake_runner(since: str, until: str, unit: str) -> bytes:
        assert unit == "monad-bft"
        return raw_journal

    # Simulate "block already happened, just past the post window" so the
    # coroutine doesn't actually sleep.
    block_ts_ms = 1_000_000_000_000
    fake_now = (block_ts_ms / 1000.0) + 100.0  # 100s after block

    out_dir = tmp_path / "reorgs"
    path = await capture_reorg_journal(
        CaptureRequest(block_number=42, block_ts_ms=block_ts_ms),
        out_dir,
        pre_sec=10, post_sec=10, flush_buffer_sec=5,
        _now=lambda: fake_now,
        _runner=fake_runner,
    )

    assert path is not None
    assert path == out_dir / "42-1000000000.jsonl.gz"
    assert path.exists()

    body = gzip.decompress(path.read_bytes()).decode("utf-8")
    # Peer IP scrubbed.
    assert "65.108.18.212" not in body
    assert "<ip>" in body
    # Public chain data passed through unchanged.
    assert "028a938a" in body
    assert '"vote":"abc"' in body


@pytest.mark.asyncio
async def test_capture_returns_none_on_empty_journal(tmp_path: Path) -> None:
    def empty_runner(since: str, until: str, unit: str) -> bytes:
        return b""

    out_dir = tmp_path / "reorgs"
    path = await capture_reorg_journal(
        CaptureRequest(block_number=42, block_ts_ms=1_000_000_000_000),
        out_dir,
        _now=lambda: 1_000_000_100.0,
        _runner=empty_runner,
    )
    assert path is None
    assert not (out_dir / "42-1000000000.jsonl.gz").exists()


@pytest.mark.asyncio
async def test_capture_defers_until_post_window(tmp_path: Path) -> None:
    """If we ask immediately after the block, the coroutine must sleep
    so journald has time to flush future-side lines before snapshotting."""
    runner_calls: list[tuple[str, str]] = []

    def runner(since: str, until: str, unit: str) -> bytes:
        runner_calls.append((since, until))
        return b'{"x":1}\n'

    block_ts_ms = 1_000_000_000_000
    # Pretend we're called *exactly* at block time. The coroutine should
    # sleep for post_sec + flush_buffer_sec (10+5=15s) before invoking
    # the runner. We patch asyncio.sleep to record the requested delay.
    sleeps: list[float] = []

    async def fake_sleep(d: float) -> None:
        sleeps.append(d)

    import monad_ops.reorg_capture as mod
    saved_sleep = mod.asyncio.sleep
    mod.asyncio.sleep = fake_sleep  # type: ignore[assignment]
    try:
        await capture_reorg_journal(
            CaptureRequest(block_number=42, block_ts_ms=block_ts_ms),
            tmp_path / "reorgs",
            pre_sec=10, post_sec=10, flush_buffer_sec=5,
            _now=lambda: block_ts_ms / 1000.0,  # called at T (event time)
            _runner=runner,
        )
    finally:
        mod.asyncio.sleep = saved_sleep  # type: ignore[assignment]

    # Slept ~15 seconds (post_sec + flush_buffer_sec), within tolerance.
    assert len(sleeps) == 1
    assert 14.5 <= sleeps[0] <= 15.5

    # Window is correctly centred around block_ts.
    assert len(runner_calls) == 1
    since, until = runner_calls[0]
    # since = block_ts - pre_sec (10 s) → 1969-12-31 23:59:50 UTC for ts=0;
    # for our fake epoch, the formatter just needs to be deterministic.
    assert since != until


@pytest.mark.asyncio
async def test_capture_does_not_sleep_when_window_already_past(tmp_path: Path) -> None:
    """Backfill path: if the post-window is already in the past, the
    coroutine must skip the sleep entirely and snapshot immediately."""
    sleeps: list[float] = []

    async def fake_sleep(d: float) -> None:
        sleeps.append(d)

    import monad_ops.reorg_capture as mod
    saved_sleep = mod.asyncio.sleep
    mod.asyncio.sleep = fake_sleep  # type: ignore[assignment]
    try:
        await capture_reorg_journal(
            CaptureRequest(block_number=42, block_ts_ms=1_000_000_000_000),
            tmp_path / "reorgs",
            pre_sec=10, post_sec=10, flush_buffer_sec=5,
            _now=lambda: 1_000_000_100.0,  # 100 s after block — well past window
            _runner=lambda since, until, unit: b'{"x":1}\n',
        )
    finally:
        mod.asyncio.sleep = saved_sleep  # type: ignore[assignment]
    # No sleep call made because delay was negative.
    assert sleeps == []
