"""Exercise the real run supervisor with isolated worker I/O."""

import argparse
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from monad_ops import cli
from monad_ops.collector.epoch_probe import EpochHistoryScan
from monad_ops.config import Config, NodeConfig
from monad_ops.state import State


@pytest.mark.parametrize("disabled", [
    pytest.param(set(), id="all-enabled"),
    pytest.param({"reference"}, id="no-reference"),
    pytest.param({"version_watch"}, id="no-version-watch"),
    pytest.param({"validator_set"}, id="no-validator-set"),
    pytest.param({"persistence"}, id="in-memory"),
    pytest.param({"reference", "version_watch", "validator_set", "persistence"}, id="minimal"),
])
@pytest.mark.asyncio
async def test_optional_workers_do_not_stop_the_monitor(monkeypatch, disabled):
    config = Config(node=NodeConfig(name="test"))
    config.node.reference_rpc_url = "" if "reference" in disabled else "http://reference.invalid"
    config.version_watch.enabled = "version_watch" not in disabled
    config.validator_set.enabled = "validator_set" not in disabled
    config.persistence.enabled = "persistence" not in disabled
    config.enrichment.enabled = False
    storage = Mock() if config.persistence.enabled else None
    if storage is not None:
        storage.get_meta.return_value = None
        storage.contract_hour_count.return_value = 1
    monkeypatch.setattr(cli, "load_config", lambda _: config)
    monkeypatch.setattr(cli, "_build_runtime_state", lambda _: (storage, State()))

    async def park(*args, **kwargs):
        await asyncio.Future()

    async def empty_tailer(**kwargs):
        await park()
        yield  # Keep the journal stream open until the supervisor cancels it.

    for function in ("_collector_loop", "_probe_loop", "fetch_reference_block", "poll_invocations"):
        monkeypatch.setattr(cli, function, park)
    monkeypatch.setattr(cli, "tail_consensus_events", empty_tailer)
    history = EpochHistoryScan([], "completed", 0, "test", "now", 1, returncode=0)
    monkeypatch.setattr(cli, "scan_epoch_history", AsyncMock(return_value=history))
    http_exit = asyncio.Event()
    server = SimpleNamespace(serve=http_exit.wait)
    monkeypatch.setattr(cli, "_build_http_server", lambda *args: (server, None))

    # Let each worker run through its startup guard before the HTTP server exits.
    real_wait = asyncio.wait
    supervised = set()
    premature = []

    async def observe_wait(tasks, *, return_when):
        assert return_when == asyncio.FIRST_COMPLETED
        supervised.update(tasks)
        await asyncio.sleep(0)
        premature.extend(task.get_coro().__name__ for task in tasks if task.done())
        http_exit.set()
        return await real_wait(tasks, return_when=return_when)

    monkeypatch.setattr(cli.asyncio, "wait", observe_wait)
    args = argparse.Namespace(config=None, host="localhost", port=0)
    assert await asyncio.wait_for(cli._cmd_run(args), timeout=2) == 0
    await asyncio.gather(*supervised, return_exceptions=True)
    assert not premature, f"workers exited before HTTP shutdown: {premature}"
    by_name = {task.get_name(): task for task in supervised}
    assert {"consensus_tailer", "bft_flush", "process_restart"} <= by_name.keys()
    assert by_name["consensus_tailer"].cancelled()
    workers = {task.get_coro().__name__ for task in supervised}
    assert ("reference_loop" in workers) == ("reference" not in disabled)
    assert ("version_loop" in workers) == ("version_watch" not in disabled)
    assert ("validator_set_loop" in workers) == ("validator_set" not in disabled)
