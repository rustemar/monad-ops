"""Host probes.

``probe_key_backups`` first: it is the one probe that renders on the
public dashboard without an alert path behind it, so both its status
and the wording of its summary matter.

``probe_fd_limits`` second: it drives a CRITICAL alert off two numbers
scraped out of ``/proc/<pid>/limits``, and every way that scrape can
fail has to land on ``unknown`` rather than on ``ok``.
"""

from __future__ import annotations

import asyncio
import os
import time
from pathlib import Path
from unittest import mock

import httpx
import pytest

from monad_ops.collector.probes import (
    probe_fd_limits,
    probe_key_backups,
    probe_stale_deploy,
    probe_triedb_migration,
)


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


# ── probe_triedb_migration ────────────────────────────────────────────
# Reports which TrieDB encoding a RUNNING node is writing. There is no
# other way to ask: monad-mpt takes the storage pool exclusively, so the
# alternative is stopping the node. Informational only — the probe alert
# path in cli.py fires on "warn"/"critical", and this probe emits
# neither, so it can never page anyone.

_METRICS_BODY = """\
# HELP monad_triedb_migration_phase Dual-DB migration phase: 0=legacy, 1=dual, 2=page
# TYPE monad_triedb_migration_phase gauge
monad_triedb_migration_phase{network="testnet",service_version="0.16.0"} 1
monad_bft_round_total{network="testnet"} 53277653
"""


def _metrics_client(body: str = _METRICS_BODY, status: int = 200):
    def handler(request):
        return httpx.Response(status, text=body)

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("phase", "word"),
    [(0, "legacy"), (1, "dual"), (2, "page")],
)
async def test_reports_each_migration_phase(phase: int, word: str) -> None:
    body = _METRICS_BODY.replace(
        'service_version="0.16.0"} 1', f'service_version="0.16.0"}} {phase}'
    )
    async with _metrics_client(body) as c:
        r = await probe_triedb_migration(client=c)
    assert r.name == "triedb_migration"
    assert r.status == "ok"
    assert r.details["phase"] == phase
    assert r.summary.startswith(word)


@pytest.mark.asyncio
async def test_unreachable_endpoint_is_unknown_not_a_fault() -> None:
    # The normal reading on any release before v0.16.0, where the
    # metrics endpoint did not exist or was off by default.
    def boom(request):
        raise httpx.ConnectError("connection refused")

    async with httpx.AsyncClient(transport=httpx.MockTransport(boom)) as c:
        r = await probe_triedb_migration(client=c)
    assert r.status == "unknown"
    assert "unreadable" in r.summary


@pytest.mark.asyncio
async def test_http_error_status_is_unknown() -> None:
    async with _metrics_client(status=503) as c:
        r = await probe_triedb_migration(client=c)
    assert r.status == "unknown"


@pytest.mark.asyncio
async def test_missing_metric_is_unknown() -> None:
    async with _metrics_client("monad_bft_round_total{a=\"b\"} 5\n") as c:
        r = await probe_triedb_migration(client=c)
    assert r.status == "unknown"
    assert "not exported" in r.summary


@pytest.mark.asyncio
async def test_unrecognised_phase_is_surfaced_not_swallowed() -> None:
    body = _METRICS_BODY.replace('service_version="0.16.0"} 1', 'service_version="0.16.0"} 7')
    async with _metrics_client(body) as c:
        r = await probe_triedb_migration(client=c)
    assert r.status == "unknown"
    assert "7" in r.summary
    assert r.details["phase"] == 7


@pytest.mark.asyncio
async def test_metric_with_a_shared_prefix_is_not_matched() -> None:
    # A future monad_triedb_migration_phase_total counter must not be
    # read as the gauge.
    body = 'monad_triedb_migration_phase_total{network="testnet"} 42\n'
    async with _metrics_client(body) as c:
        r = await probe_triedb_migration(client=c)
    assert r.status == "unknown"
    assert "not exported" in r.summary


@pytest.mark.asyncio
async def test_help_and_type_comment_lines_do_not_become_the_value() -> None:
    # The HELP line mentions the metric name and the digits 0, 1 and 2,
    # and the TYPE line ends in the word "gauge". Neither may be read as
    # the sample.
    async with _metrics_client(_METRICS_BODY) as c:
        r = await probe_triedb_migration(client=c)
    assert r.details["phase"] == 1


@pytest.mark.asyncio
async def test_infinite_value_does_not_escape_the_probe() -> None:
    # int(float("+Inf")) raises OverflowError, which is not a ValueError.
    # This path sits outside the fetch guard, so an escape here discards
    # every other probe's result for the cycle.
    body = _METRICS_BODY.replace('service_version="0.16.0"} 1', 'service_version="0.16.0"} +Inf')
    async with _metrics_client(body) as c:
        r = await probe_triedb_migration(client=c)
    assert r.status == "unknown"


@pytest.mark.asyncio
async def test_no_exception_escapes_to_poison_the_probe_batch() -> None:
    """run_all_probes gathers without return_exceptions.

    Anything this probe raises costs every other probe's result for that
    cycle, so the fetch must swallow even the exceptions that are not
    httpx.HTTPError — httpx.InvalidURL is neither that nor an OSError.
    """
    def boom(request):
        raise httpx.InvalidURL("not a url")

    async with httpx.AsyncClient(transport=httpx.MockTransport(boom)) as c:
        r = await probe_triedb_migration(client=c)
    assert r.status == "unknown"
    assert "InvalidURL" in r.summary


@pytest.mark.asyncio
async def test_cancellation_still_propagates() -> None:
    # CancelledError is a BaseException; swallowing it would make the
    # probe loop unkillable on shutdown.
    def cancel(request):
        raise asyncio.CancelledError()

    async with httpx.AsyncClient(transport=httpx.MockTransport(cancel)) as c:
        with pytest.raises(asyncio.CancelledError):
            await probe_triedb_migration(client=c)


# ---------------------------------------------------------------------------
# stale_deploy — "you pulled but did not restart"
# ---------------------------------------------------------------------------

def _mk_checkout(root: Path, sha: str, *, packed: bool = False) -> Path:
    """Minimal .git that _resolve_git_head can read."""
    git = root / ".git"
    (git / "refs" / "heads").mkdir(parents=True, exist_ok=True)
    (git / "HEAD").write_text("ref: refs/heads/main\n")
    if packed:
        (git / "packed-refs").write_text(
            f"# pack-refs with: peeled fully-peeled sorted\n{sha} refs/heads/main\n"
        )
    else:
        (git / "refs" / "heads" / "main").write_text(sha + "\n")
    return root


@pytest.mark.asyncio
async def test_stale_deploy_ok_when_checkout_matches(tmp_path: Path) -> None:
    sha = "a" * 40
    repo = _mk_checkout(tmp_path, sha)
    r = await probe_stale_deploy(repo_dir=repo, startup_head=sha)
    assert r.status == "ok"
    assert sha[:12] in r.summary


@pytest.mark.asyncio
async def test_stale_deploy_warns_after_a_pull(tmp_path: Path) -> None:
    """The case this exists for: files moved, process did not."""
    old, new = "a" * 40, "b" * 40
    repo = _mk_checkout(tmp_path, new)
    r = await probe_stale_deploy(repo_dir=repo, startup_head=old)
    assert r.status == "warn"          # never critical — stale is not an outage
    assert new[:12] in r.summary and old[:12] in r.summary
    assert r.details == {"running": old, "checkout": new}


@pytest.mark.asyncio
async def test_stale_deploy_reads_packed_refs(tmp_path: Path) -> None:
    # A fresh clone keeps refs packed, so a loose-file-only reader would
    # report "unknown" on exactly the deployments most likely to be new.
    sha = "c" * 40
    repo = _mk_checkout(tmp_path, sha, packed=True)
    assert not (repo / ".git" / "refs" / "heads" / "main").exists()
    r = await probe_stale_deploy(repo_dir=repo, startup_head=sha)
    assert r.status == "ok"


@pytest.mark.asyncio
async def test_stale_deploy_follows_a_worktree_pointer(tmp_path: Path) -> None:
    # In a worktree .git is a FILE pointing at the real gitdir.
    sha = "d" * 40
    real = _mk_checkout(tmp_path / "real", sha)
    wt = tmp_path / "wt"
    wt.mkdir()
    (wt / ".git").write_text(f"gitdir: {real / '.git'}\n")
    r = await probe_stale_deploy(repo_dir=wt, startup_head=sha)
    assert r.status == "ok"


@pytest.mark.asyncio
async def test_stale_deploy_unknown_outside_a_checkout(tmp_path: Path) -> None:
    """Installed from a wheel is a normal way to run, not a fault.

    'I cannot tell' must never render as 'nothing changed'.
    """
    r = await probe_stale_deploy(repo_dir=tmp_path, startup_head="e" * 40)
    assert r.status == "unknown"
    r2 = await probe_stale_deploy(repo_dir=tmp_path, startup_head=None)
    assert r2.status == "unknown"


@pytest.mark.asyncio
async def test_stale_deploy_never_raises_on_a_broken_git_dir(tmp_path: Path) -> None:
    # run_all_probes gathers without return_exceptions, so an escape here
    # would discard every other probe's result for the cycle.
    (tmp_path / ".git").mkdir()
    (tmp_path / ".git" / "HEAD").write_text("ref: refs/heads/gone\n")
    r = await probe_stale_deploy(repo_dir=tmp_path, startup_head="f" * 40)
    assert r.status == "unknown"
