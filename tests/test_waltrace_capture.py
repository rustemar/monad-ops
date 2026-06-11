"""Tests for the waltrace-flood evidence capture."""

from __future__ import annotations

import asyncio
import gzip
from pathlib import Path

from monad_ops.waltrace_capture import (
    capture_waltrace_evidence,
    list_wal_dir,
    waltrace_dir_for,
)


def _read_gz(path: Path) -> str:
    with gzip.open(path, "rb") as fh:
        return fh.read().decode("utf-8")


class TestListWalDir:
    def test_lists_sizes_and_timestamps(self, tmp_path: Path) -> None:
        (tmp_path / "wal_1781074628443.7").write_bytes(b"x" * 100)
        (tmp_path / "wal_1781074628443.8").write_bytes(b"")
        out = list_wal_dir(tmp_path)
        assert "wal_1781074628443.7\tsize=100" in out
        assert "wal_1781074628443.8\tsize=0" in out
        assert "dir-mtime=" in out
        assert "mtime=" in out and "ctime=" in out

    def test_unreadable_dir_is_a_recorded_finding(self, tmp_path: Path) -> None:
        out = list_wal_dir(tmp_path / "does-not-exist")
        assert out.startswith("# wal dir unreadable:")


class TestCapture:
    def test_writes_listing_and_sanitized_journal(self, tmp_path: Path) -> None:
        wal = tmp_path / "wal"
        wal.mkdir()
        (wal / "wal_123.0").write_bytes(b"y" * 10)

        def runner(since: str, until: str, unit: str) -> bytes:
            assert unit == "monad-bft"
            return (
                b'{"message":"local timeout"}\n'
                b'"remote_addr":"65.108.18.212:8001"\n'
                b'{"message":"waltrace thread stopped"}\n'
            )

        out_dir = tmp_path / "captures"
        path = asyncio.run(capture_waltrace_evidence(
            1781152000.0, out_dir, wal_dir=wal, _runner=runner,
        ))
        assert path is not None and path.exists()
        text = _read_gz(path)
        assert "wal_123.0\tsize=10" in text
        assert "waltrace thread stopped" in text
        assert "65.108.18.212" not in text  # peer IP sanitized
        assert "<ip>" in text
        assert "first error line at 2026-06-" in text

    def test_journalctl_failure_still_writes_listing(self, tmp_path: Path) -> None:
        wal = tmp_path / "wal"
        wal.mkdir()
        (wal / "wal_9.0").write_bytes(b"")

        def runner(since: str, until: str, unit: str) -> bytes:
            raise OSError("journalctl missing")

        path = asyncio.run(capture_waltrace_evidence(
            1781152000.0, tmp_path / "captures", wal_dir=wal, _runner=runner,
        ))
        assert path is not None
        text = _read_gz(path)
        assert "wal_9.0\tsize=0" in text
        assert "# journalctl failed:" in text

    def test_artifact_dir_sits_next_to_db(self) -> None:
        assert waltrace_dir_for("data/state.db") == Path("data/waltrace")
