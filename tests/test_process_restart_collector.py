"""Unit tests for collector/process_restart.py — systemctl output parsing.

The async subprocess shell-out is patched so the suite runs without
systemd available (CI / macOS / containers). Real-system integration is
covered by the live restart that exercises this code path each
deploy — we don't test against actual systemctl in CI.
"""

from __future__ import annotations

import pytest

from monad_ops.collector import process_restart as pr_mod
from monad_ops.collector.process_restart import (
    InvocationSnapshot,
    poll_invocation,
    poll_invocations,
)


def _patch_run(monkeypatch, output: str, rc: int = 0, err: str = ""):
    """Replace the module-level ``_run`` with a stub that returns
    a single canned subprocess result."""

    async def fake_run(cmd, timeout=5.0):
        return rc, output, err

    monkeypatch.setattr(pr_mod, "_run", fake_run)


@pytest.mark.asyncio
async def test_parses_systemctl_show_output(monkeypatch):
    """Standard `systemctl show … --property=…` output: KEY=VALUE per
    line. Empty values become None."""
    output = (
        "ActiveState=active\n"
        "SubState=running\n"
        "InvocationID=f60018c6ddcb4c64bc703e8ecde3b7f6\n"
    )
    _patch_run(monkeypatch, output)
    snap = await poll_invocation("monad-bft")
    assert snap.service == "monad-bft"
    assert snap.invocation_id == "f60018c6ddcb4c64bc703e8ecde3b7f6"
    assert snap.sub_state == "running"
    assert snap.active_state == "active"
    assert snap.error is None


@pytest.mark.asyncio
async def test_subprocess_timeout_returns_error_snapshot(monkeypatch):
    """rc=127 from _run signals timeout / FileNotFoundError. The
    snapshot must surface as error, not as a bogus all-None state
    (which the rule would mistake for "missing InvocationID — schema
    drift")."""
    _patch_run(monkeypatch, "", rc=127, err="timed out")
    snap = await poll_invocation("monad-bft")
    assert snap.invocation_id is None
    assert snap.error == "timed out"


@pytest.mark.asyncio
async def test_nonzero_rc_returns_error_snapshot(monkeypatch):
    """systemctl returns non-zero on unknown unit / load error. Don't
    let the rule see an empty-but-error-free snapshot — that would
    silently overwrite the last-seen InvocationID with None and never
    re-arm."""
    _patch_run(monkeypatch, "", rc=4, err="Unit not loaded.")
    snap = await poll_invocation("does-not-exist")
    assert snap.invocation_id is None
    assert snap.error is not None
    assert "rc=4" in snap.error


@pytest.mark.asyncio
async def test_unknown_keys_in_output_ignored(monkeypatch):
    """Future systemd versions might add new properties or change
    capitalization. Parser must skip unrecognized lines silently."""
    output = (
        "ActiveState=active\n"
        "SubState=running\n"
        "SomeNewProperty=42\n"
        "InvocationID=abc\n"
    )
    _patch_run(monkeypatch, output)
    snap = await poll_invocation("monad-bft")
    assert snap.invocation_id == "abc"
    assert snap.sub_state == "running"


@pytest.mark.asyncio
async def test_blank_value_becomes_none(monkeypatch):
    """A unit that has been stopped briefly may render
    `InvocationID=` (blank value). Treat that as missing — let the rule
    soft-ignore."""
    output = "ActiveState=inactive\nSubState=dead\nInvocationID=\n"
    _patch_run(monkeypatch, output)
    snap = await poll_invocation("monad-rpc")
    assert snap.invocation_id is None
    assert snap.sub_state == "dead"


@pytest.mark.asyncio
async def test_poll_invocations_parallel_preserves_order(monkeypatch):
    """When polling multiple services, the result list order must
    match the input order so callers can pair-zip without lookups."""
    services = ["monad-bft", "monad-execution", "monad-rpc"]
    outputs = {
        "monad-bft": "InvocationID=bft-id\n",
        "monad-execution": "InvocationID=exec-id\n",
        "monad-rpc": "InvocationID=rpc-id\n",
    }

    async def fake_run(cmd, timeout=5.0):
        # cmd looks like ["systemctl", "show", "<unit>", "--property=..."]
        unit = cmd[2]
        return 0, outputs[unit], ""

    monkeypatch.setattr(pr_mod, "_run", fake_run)
    snaps = await poll_invocations(services)
    assert [s.service for s in snaps] == services
    assert snaps[0].invocation_id == "bft-id"
    assert snaps[2].invocation_id == "rpc-id"
