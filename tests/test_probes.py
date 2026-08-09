"""Host probes.

``probe_key_backups`` first: it is the one probe that renders on the
public dashboard without an alert path behind it, so both its status
and the wording of its summary matter.

``probe_fd_limits`` second: it drives a CRITICAL alert off two numbers
scraped out of ``/proc/<pid>/limits``, and every way that scrape can
fail has to land on ``unknown`` rather than on ``ok``.
"""

from __future__ import annotations

import os
import time
from pathlib import Path
from unittest import mock

import pytest

from monad_ops.collector.probes import probe_fd_limits, probe_key_backups


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


# ── probe_fd_limits ───────────────────────────────────────────────────
# The node hit "Too many open files" on TN1 2025-11-25 with the 1024
# default, which is what this probe exists to catch before it happens.

_LIMITS_TEMPLATE = """\
Limit                     Soft Limit           Hard Limit           Units
Max cpu time              unlimited            unlimited            seconds
Max file size             unlimited            unlimited            bytes
Max open files            {soft}               {hard}               files
Max locked memory         unlimited            unlimited            bytes
"""


def _fd_probe(*, soft: int = 1_048_576, hard: int = 1_048_576, pid: str = "4242"):
    """Drive probe_fd_limits against a synthetic /proc/<pid>/limits."""
    return _fd_probe_raw(_LIMITS_TEMPLATE.format(soft=soft, hard=hard), pid=pid)


def _fd_probe_raw(limits_text: str, *, pid: str = "4242", rc: int = 0):
    async def fake_run(cmd, timeout=5.0):
        return rc, pid, ""

    return mock.patch("monad_ops.collector.probes._run", fake_run), mock.patch.object(
        Path, "exists", return_value=True
    ), mock.patch.object(Path, "read_text", return_value=limits_text)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("soft", "expected"),
    [
        (1_048_576, "ok"),
        (16_384, "ok"),      # threshold is exclusive: at the recommended value we are fine
        (16_383, "warn"),
        (8_192, "warn"),
        (4_096, "warn"),     # the critical threshold is exclusive too
        (4_095, "critical"),
        (1_024, "critical"),  # the systemd default that broke the node
    ],
)
async def test_soft_limit_bands(soft: int, expected: str) -> None:
    run, exists, read = _fd_probe(soft=soft)
    with run, exists, read:
        r = await probe_fd_limits()
    assert r.status == expected
    assert r.details["soft"] == soft


@pytest.mark.asyncio
async def test_ok_result_carries_both_numbers() -> None:
    run, exists, read = _fd_probe(soft=1_048_576, hard=1_048_576, pid="1234")
    with run, exists, read:
        r = await probe_fd_limits()
    assert r.name == "fd_limits"
    assert r.status == "ok"
    assert r.details == {"pid": "1234", "soft": 1_048_576, "hard": 1_048_576}
    assert "monad-execution" in r.summary


@pytest.mark.asyncio
async def test_the_probed_service_is_named_in_the_summary() -> None:
    run, exists, read = _fd_probe(soft=1_024)
    with run, exists, read:
        r = await probe_fd_limits(service="monad-bft")
    assert r.status == "critical"
    assert "monad-bft" in r.summary


# Every failure below must be "unknown". A probe that could not measure
# anything reporting "ok" would quietly retire a CRITICAL alert path.

@pytest.mark.asyncio
async def test_stopped_service_is_unknown_not_ok() -> None:
    # systemd reports MainPID=0 for a stopped unit.
    run, exists, read = _fd_probe_raw("", pid="0")
    with run, exists, read:
        r = await probe_fd_limits()
    assert r.status == "unknown"
    assert "MainPID not resolvable" in r.summary


@pytest.mark.asyncio
async def test_systemctl_failure_is_unknown() -> None:
    run, exists, read = _fd_probe_raw("", pid="4242", rc=1)
    with run, exists, read:
        r = await probe_fd_limits()
    assert r.status == "unknown"


@pytest.mark.asyncio
async def test_missing_limits_file_is_unknown() -> None:
    # The process can exit between reading MainPID and opening /proc.
    async def fake_run(cmd, timeout=5.0):
        return 0, "4242", ""

    with mock.patch("monad_ops.collector.probes._run", fake_run), mock.patch.object(
        Path, "exists", return_value=False
    ):
        r = await probe_fd_limits()
    assert r.status == "unknown"
    assert "not readable" in r.summary


@pytest.mark.asyncio
async def test_unreadable_limits_file_is_unknown() -> None:
    async def fake_run(cmd, timeout=5.0):
        return 0, "4242", ""

    with mock.patch("monad_ops.collector.probes._run", fake_run), mock.patch.object(
        Path, "exists", return_value=True
    ), mock.patch.object(Path, "read_text", side_effect=PermissionError("denied")):
        r = await probe_fd_limits()
    assert r.status == "unknown"
    assert "cannot read" in r.summary


@pytest.mark.asyncio
async def test_unparseable_limits_file_is_unknown() -> None:
    # An "unlimited" nofile row, or a reformatted /proc, must not be read
    # as a low limit — there is no number to compare.
    run, exists, read = _fd_probe_raw(
        "Limit                     Soft Limit           Hard Limit\n"
        "Max open files            unlimited            unlimited\n"
    )
    with run, exists, read:
        r = await probe_fd_limits()
    assert r.status == "unknown"
    assert "Max open files" in r.summary
