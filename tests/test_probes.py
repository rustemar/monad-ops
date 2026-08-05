"""Host probes.

``probe_key_backups`` first: it is the one probe that renders on the
public dashboard without an alert path behind it, so both its status
and the wording of its summary matter.
"""

from __future__ import annotations

import os
import time
from pathlib import Path
from unittest import mock

import pytest

from monad_ops.collector.probes import probe_key_backups


def _backup(dir_: Path, name: str, *, mode: int = 0o600, age_days: float = 0.0) -> Path:
    p = dir_ / name
    p.write_text("key material")
    p.chmod(mode)
    if age_days:
        when = time.time() - age_days * 86400
        os.utime(p, (when, when))
    return p


@pytest.mark.asyncio
async def test_owner_only_backups_are_ok(tmp_path: Path) -> None:
    paths = [_backup(tmp_path, "secp-backup"), _backup(tmp_path, "bls-backup")]
    r = await probe_key_backups(paths)
    assert r.status == "ok"
    assert r.summary == "2 backup(s) present, owner-only"
    assert r.details[str(paths[0])]["mode"] == "0600"


@pytest.mark.asyncio
async def test_never_rotated_backup_is_not_a_warning(tmp_path: Path) -> None:
    # The 113-day false WARN: a keystore that never rotates keeps its
    # original mtime, and age alone must not arm the probe.
    paths = [_backup(tmp_path, "secp-backup", age_days=113)]
    r = await probe_key_backups(paths)
    assert r.status == "ok"


@pytest.mark.asyncio
async def test_age_still_warns_when_the_operator_opts_in(tmp_path: Path) -> None:
    paths = [_backup(tmp_path, "secp-backup", age_days=113)]
    r = await probe_key_backups(paths, warn_after_days=30)
    assert r.status == "warn"
    assert "stale" in r.summary


@pytest.mark.asyncio
async def test_world_readable_backup_warns_without_naming_it(tmp_path: Path) -> None:
    paths = [
        _backup(tmp_path, "secp-backup", mode=0o644),
        _backup(tmp_path, "bls-backup"),
    ]
    r = await probe_key_backups(paths)
    assert r.status == "warn"
    assert r.summary == "1 of 2 backup(s) readable beyond owner"
    # The public summary must not point at the exposed file.
    assert "secp-backup" not in r.summary
    assert r.details[str(paths[0])]["mode"] == "0644"


@pytest.mark.asyncio
async def test_group_readable_counts_as_exposed(tmp_path: Path) -> None:
    paths = [_backup(tmp_path, "secp-backup", mode=0o640)]
    r = await probe_key_backups(paths)
    assert r.status == "warn"


@pytest.mark.asyncio
async def test_missing_backup_is_critical_and_named(tmp_path: Path) -> None:
    paths = [_backup(tmp_path, "secp-backup"), tmp_path / "bls-backup"]
    r = await probe_key_backups(paths)
    assert r.status == "critical"
    assert r.summary == "missing: bls-backup"
    assert r.details[str(paths[1])] == {"exists": False}


@pytest.mark.asyncio
async def test_empty_backup_is_critical(tmp_path: Path) -> None:
    p = _backup(tmp_path, "secp-backup")
    p.write_text("")
    r = await probe_key_backups([p])
    assert r.status == "critical"
    assert r.summary == "empty: secp-backup"


@pytest.mark.asyncio
async def test_unstattable_backup_reports_unknown_not_ok(tmp_path: Path) -> None:
    paths = [_backup(tmp_path, "secp-backup")]
    with mock.patch.object(Path, "stat", side_effect=PermissionError("denied")):
        r = await probe_key_backups(paths)
    assert r.status == "unknown"
    assert r.summary == "cannot stat 1 of 1 backup(s)"
