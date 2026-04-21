"""monad-ops CLI entry point.

    monad-ops run       — start the live collector loop (+ dashboard HTTP server)
    monad-ops ping      — send a test message to the configured alert channel
    monad-ops replay    — parse the last N minutes of journal as a dry-run
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import time
from pathlib import Path

import structlog
import uvicorn

from monad_ops.alerts import DedupingSink, StdoutSink, TelegramSink
from monad_ops.alerts.sink import AlertSink
from monad_ops.api import build_app
from monad_ops.collector.journal import TailError, tail_execution_blocks
from monad_ops.collector.probes import run_all_probes
from monad_ops.collector.epoch_probe import probe_epoch, scan_epoch_history
from monad_ops.collector.reference_rpc import fetch_reference_block
from monad_ops.config import Config, load_config
from monad_ops.enricher import EnrichmentWorker, ReceiptsClient
from monad_ops.labels import ContractLabels
from monad_ops.parser import AssertionEvent, ExecBlock
from monad_ops.rules import (
    AlertEvent,
    AssertionRule,
    ReferenceLagRule,
    ReorgRule,
    RetrySpikeRule,
    Severity,
    StallRule,
)
from monad_ops.state import State
from monad_ops.storage import Storage


log = structlog.stdlib.get_logger()


def _build_sink(config: Config) -> AlertSink:
    tg_cfg = config.alerts.telegram
    if tg_cfg is None:
        log.warning("telegram.not_configured", falling_back="stdout")
        inner: AlertSink = StdoutSink()
    else:
        inner = TelegramSink(
            bot_token=tg_cfg.bot_token,
            chat_id=tg_cfg.chat_id,
            topic_id=tg_cfg.topic_id,
            source_tag=tg_cfg.source_tag,
        )
    return DedupingSink(inner, cooldown_sec=config.rules.dedup.cooldown_sec)


class _RecordingSink:
    """Wraps a sink and records every delivered event into shared State."""

    def __init__(self, inner: AlertSink, state: State) -> None:
        self._inner = inner
        self._state = state

    async def deliver(self, event: AlertEvent) -> None:
        await self._state.add_alert_async(event)
        await self._inner.deliver(event)


async def _collector_loop(
    config: Config,
    sink: AlertSink,
    state: State,
    enricher: EnrichmentWorker | None = None,
) -> None:
    stall = StallRule(
        warn_after_sec=config.rules.stall.warn_after_sec,
        critical_after_sec=config.rules.stall.critical_after_sec,
    )
    retry = RetrySpikeRule(
        window=config.rules.retry_spike.window,
        warn_pct=config.rules.retry_spike.warn_pct,
        critical_pct=config.rules.retry_spike.critical_pct,
        min_window_tx_avg=config.rules.retry_spike.min_window_tx_avg,
    )
    assertion = AssertionRule()
    reorg = ReorgRule()
    # Rule is observable from the API layer for the integrity panel
    # (reorg_count + last-reorg details) even when it has fired zero
    # events — "0 reorgs over N blocks" is itself the signal we want
    # to display.
    state.attach_reorg_rule(reorg)
    # Bootstrap the counter from sqlite so restart-induced reset doesn't
    # under-report. See rules/reorg.py ReorgRule.bootstrap() docstring.
    if state.storage is not None:
        count, last = state.storage.load_reorg_history()
        if count > 0:
            # Alert detail format produced by _make_event is:
            #   "Block #NNN id changed: 0xAAAA…BBBB → 0xCCCC…DDDD. …"
            # We extract the short-form IDs — the full 64-hex pre-image
            # is only available on the *new* id via block_record, and
            # the UI renders short anyway, so preserving truncation is
            # good enough for the integrity panel.
            bn = last_old = last_new = None
            last_ts_ms = None
            if last is not None:
                import re as _re
                m = _re.match(
                    r"Block #(\d+) id changed: (\S+) → (\S+)\.",
                    last.detail,
                )
                if m:
                    bn = int(m.group(1))
                    last_old = m.group(2)
                    last_new = m.group(3)
                last_ts_ms = int(last.ts * 1000)
            reorg.bootstrap(
                count=count,
                last_block_number=bn,
                last_old_id=last_old,
                last_new_id=last_new,
                last_ts_ms=last_ts_ms,
            )
            log.info("reorg.bootstrap", count=count, last_block=bn)

    async def tick_task() -> None:
        while True:
            await asyncio.sleep(1)
            ev = stall.on_tick()
            if ev is not None:
                await sink.deliver(ev)

    tick_handle = asyncio.create_task(tick_task())

    try:
        async for item in tail_execution_blocks():
            if isinstance(item, TailError):
                if item.graceful:
                    # Signal-initiated shutdown (our own restart / systemd
                    # stop). Log it but do not alert — firing CRITICAL on
                    # every deploy pollutes the alert history and, post
                    # public launch, would read as a node incident to
                    # dashboard viewers.
                    log.info("tailer.graceful_exit", detail=item.message)
                else:
                    log.error("tailer.exit", detail=item.message)
                    await sink.deliver(
                        AlertEvent(
                            rule="monitor",
                            severity=Severity.CRITICAL,
                            key="monitor:tailer-dead",
                            title="monad-ops tailer exited",
                            detail=item.message,
                        )
                    )
                return

            if isinstance(item, AssertionEvent):
                log.warning("assertion.detected", kind=item.kind.value,
                            key=item.key, location=item.location)
                await sink.deliver(assertion.on_event(item))
                continue

            block: ExecBlock = item
            await state.add_block_async(block)
            log.debug("block", n=block.block_number, tx=block.tx_count,
                      rt=block.retried, rtp=block.retry_pct)
            if enricher is not None and block.tx_count > 0:
                enricher.submit(block.block_number)

            for ev in _filter_none([
                stall.on_block(block),
                retry.on_block(block),
                reorg.on_block(block),
            ]):
                await sink.deliver(ev)
    finally:
        tick_handle.cancel()


def _filter_none(items):
    return [it for it in items if it is not None]


async def _cmd_run(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    storage: Storage | None = None
    if config.persistence.enabled:
        storage = Storage(config.persistence.path)
        log.info("persistence.ready", path=config.persistence.path,
                 blocks_on_disk=storage.block_count())
    state = State(storage=storage)
    if storage is not None:
        loaded = state.bootstrap_from_storage(limit=config.persistence.bootstrap_blocks)
        if loaded:
            log.info("bootstrap.loaded", blocks=loaded)
        # Hydrate the recent-alerts buffer so the dashboard panel keeps
        # showing events through a restart. Limit matches the in-memory
        # buffer size (200) — anything older isn't retained anyway.
        alerts_loaded = state.bootstrap_alerts_from_storage(limit=200)
        if alerts_loaded:
            log.info("bootstrap.alerts_loaded", alerts=alerts_loaded)
    base_sink = _build_sink(config)
    sink: AlertSink = _RecordingSink(base_sink, state)

    enricher: EnrichmentWorker | None = None
    receipts_client: ReceiptsClient | None = None
    if config.enrichment.enabled and storage is not None:
        receipts_client = ReceiptsClient(
            rpc_url=config.node.rpc_url,
            timeout_sec=config.enrichment.rpc_timeout_sec,
        )
        enricher = EnrichmentWorker(
            client=receipts_client,
            storage=storage,
            queue_size=config.enrichment.queue_size,
        )
        log.info("enricher.ready",
                 rpc=config.node.rpc_url,
                 queue=config.enrichment.queue_size,
                 tx_rows_on_disk=storage.tx_enrichment_count())

    labels = ContractLabels.load(config.labels.path)
    app = build_app(state, config, enricher=enricher, labels=labels)
    server = uvicorn.Server(uvicorn.Config(
        app,
        host=args.host,
        port=args.port,
        log_level="warning",
        access_log=False,
        loop="asyncio",
    ))

    log.info("starting", node=config.node.name, rpc=config.node.rpc_url,
             http=f"{args.host}:{args.port}")

    async def probe_loop():
        # Run probes immediately, then every 60s.
        while True:
            try:
                results = await run_all_probes(config.node.services)
                state.set_probes(results)
                # Emit alerts on any critical probe.
                for r in results:
                    if r.status == "critical":
                        await sink.deliver(AlertEvent(
                            rule=f"probe:{r.name}",
                            severity=Severity.CRITICAL,
                            key=f"probe:{r.name}",
                            title=f"Probe {r.name} CRITICAL",
                            detail=r.summary,
                        ))
                    elif r.status == "warn":
                        await sink.deliver(AlertEvent(
                            rule=f"probe:{r.name}",
                            severity=Severity.WARN,
                            key=f"probe:{r.name}",
                            title=f"Probe {r.name} WARN",
                            detail=r.summary,
                        ))
            except Exception as e:  # noqa: BLE001
                log.error("probe_loop.error", exc=str(e))
            await asyncio.sleep(60)

    async def reference_loop():
        # Periodic probe of an external reference RPC so the dashboard
        # can show "is my node lagging or is the whole network halted?".
        # Empty url disables the probe entirely (snapshot() returns None
        # fields and the UI shows "— unreachable" — same as the
        # fail-closed state, by design).
        url = config.node.reference_rpc_url
        interval = max(5, int(config.node.reference_poll_sec))
        if not url:
            log.info("reference_rpc.disabled")
            return
        lag_rule = ReferenceLagRule(
            warn_blocks=config.rules.reference_lag.warn_blocks,
            critical_blocks=config.rules.reference_lag.critical_blocks,
            window=config.rules.reference_lag.window,
        )
        from dataclasses import replace
        local_rpc_url = config.node.rpc_url
        while True:
            try:
                # Poll both in parallel so the two samples are as close
                # in wall-clock time as possible (same reason we record
                # local_at_sample — staleness bias).
                sample, local_sample = await asyncio.gather(
                    fetch_reference_block(url),
                    fetch_reference_block(local_rpc_url),
                )
                # Use the LIVE RPC number as local tip, not
                # `state.snapshot().last_block`. On 2026-04-20 the
                # state-based source produced 5 false criticals during
                # event-loop freezes: our collector's state.last_block
                # fell behind by hundreds of blocks while the node RPC
                # was still returning the live tip. Asking the node
                # directly is the physical truth; state is a derived
                # view that lags under load.
                local_at_sample = local_sample.block_number
                sample = replace(sample, local_at_sample=local_at_sample)
                # Only overwrite the UI-visible reference state when both
                # probes succeeded. A single transient timeout used to
                # blank out the "reference" card for 15 seconds until the
                # next poll — jarring during a demo. Keeping the last
                # known sample means the UI stays steady; viewers get
                # fresh data on the next successful tick.
                if sample.error is None and local_sample.error is None:
                    state.set_reference(sample)
                # If either probe failed we treat it as reference_error
                # so the rule soft-ignores the sample (see ReferenceLagRule).
                probe_error = sample.error or local_sample.error
                ev = lag_rule.on_sample(
                    reference_block=sample.block_number,
                    local_block=local_at_sample,
                    reference_error=probe_error,
                )
                if ev is not None:
                    await sink.deliver(ev)
            except Exception as e:  # noqa: BLE001
                # fetch_reference_block swallows its own errors into the
                # sample.error field — anything reaching here is a bug.
                log.error("reference_loop.error", exc=str(e))
            await asyncio.sleep(interval)

    async def retention_loop():
        # Daily prune of historical rows older than keep_days. Fire-and-
        # forget: failures log and retry next tick, never fatal. Runs in
        # a worker thread so a large DELETE on a million rows doesn't
        # stall the event loop.
        keep_days = int(config.retention.keep_days)
        interval_sec = max(3600, int(config.retention.interval_hours) * 3600)
        log.info("retention.ready", keep_days=keep_days, interval_sec=interval_sec)
        # Sleep a little on startup so bootstrap/first metrics settle
        # before we start doing DB-heavy background writes.
        await asyncio.sleep(60)
        while True:
            try:
                deleted = await asyncio.to_thread(
                    storage.prune_older_than, keep_days=keep_days
                )
                log.info("retention.prune", **deleted)
            except Exception as e:  # noqa: BLE001
                log.error("retention.error", exc=str(e))
            await asyncio.sleep(interval_sec)

    async def epoch_backfill_task():
        # Fire-and-forget: scan 8h of monad-bft journal and seed the
        # tracker with closed epoch spans. Without this, the dashboard
        # shows "epoch length unknown" for ~5 hours (until the first
        # live rollover). Runs in parallel with epoch_loop, not in
        # series — the 60–120s scan shouldn't delay the live probe
        # that gives the dashboard its current-epoch number.
        try:
            history = await scan_epoch_history()
            for seq, ep in history:
                state.observe_epoch(ep, seq)
            log.info("epoch.backfill", observations=len(history))
        except Exception as e:  # noqa: BLE001
            log.warning("epoch.backfill_failed", exc=str(e))

    async def epoch_loop():
        # Light sidecar. Scans the last 30s of monad-bft every 15s for
        # the latest (block_seq_num, block_epoch) tuple. Each successful
        # reading feeds State.observe_epoch which accretes epoch spans
        # and lets snapshot() compute progress. Failures are soft — the
        # dashboard shows "—" until the next round.
        # Kick the backfill in a detached task so the live probe can
        # start filling in the current-epoch number immediately.
        asyncio.create_task(epoch_backfill_task(), name="epoch_backfill")
        await asyncio.sleep(2)
        while True:
            try:
                sample = await probe_epoch()
                if sample.error is None:
                    state.observe_epoch(sample.epoch, sample.seq_num)
                else:
                    log.debug("epoch_probe.skip", err=sample.error)
            except Exception as e:  # noqa: BLE001
                log.error("epoch_loop.error", exc=str(e))
            await asyncio.sleep(15)

    async def warm_sampled_windows():
        # Background pre-warm for the chart endpoint. Visitors who land
        # with the default 12h window otherwise pay the cold-SQL cost on
        # first fetch (~8s on a 24h bin + JOIN). We keep the popular
        # windows warm in the server-side cache so the first Cloudflare-
        # level hit returns instantly, not just the second one. Cheap —
        # one 100–300ms SQL per window per ~12s of cadence.
        if storage is None:
            return
        # Windows in seconds. Skip 5m/15m — their cache TTL is only 2s
        # and short windows are already fast enough cold.
        PREWARM_WINDOWS_SEC = (3600, 14400, 43200, 86400)
        # Sleep briefly on startup so the first SQLite queries don't
        # compete with bootstrap reads.
        await asyncio.sleep(10)
        while True:
            now_ms = int(time.time() * 1000)
            for w_sec in PREWARM_WINDOWS_SEC:
                try:
                    await asyncio.to_thread(
                        storage.sampled_blocks,
                        from_ts_ms=now_ms - w_sec * 1000,
                        to_ts_ms=now_ms,
                        target_points=300,
                    )
                except Exception as e:  # noqa: BLE001
                    log.warning("prewarm.error", window=w_sec, exc=str(e))
            # Wide-window queries (12h, 24h) are the expensive ones but
            # their cache TTL is 15 s. Running every 12 s kept the loop
            # always active. 60 s cadence frees CPU for the tailer and
            # viewer requests; cold hits on wide windows are rare enough.
            await asyncio.sleep(60)

    async def contract_hour_loop():
        """Keep the `contract_hour` rollup current.

        Backfills the whole history on startup if the table is empty
        (one-off for the tx_contract_block → contract_hour migration),
        then rebuilds the last two hour-buckets every 60 s so
        late-arriving enrichments are picked up without leaving stale
        aggregates in the open hour.

        Failure-safe: exceptions log and the loop continues — the
        dashboard keeps serving whatever was last successfully written.
        """
        if storage is None:
            return
        try:
            count = await asyncio.to_thread(storage.contract_hour_count)
            if count == 0:
                log.info("contract_hour.backfill.start")
                t0 = time.time()
                written = await asyncio.to_thread(storage.backfill_contract_hour)
                log.info(
                    "contract_hour.backfill.done",
                    rows=written,
                    elapsed_sec=round(time.time() - t0, 1),
                )
        except Exception as e:  # noqa: BLE001
            log.error("contract_hour.backfill.error", exc=str(e))

        while True:
            try:
                now_ms = int(time.time() * 1000)
                # Rebuild the current hour plus the previous one. The
                # previous hour might still receive late enrichments if
                # the enricher is catching up after a restart or an RPC
                # blip; rewriting it is cheap (~1 000 rollup rows) and
                # keeps the rollup authoritative.
                HOUR_MS = 3_600_000
                from_ms = ((now_ms // HOUR_MS) - 1) * HOUR_MS
                to_ms = ((now_ms // HOUR_MS) + 1) * HOUR_MS
                await asyncio.to_thread(
                    storage.rebuild_contract_hour, from_ms, to_ms
                )
            except Exception as e:  # noqa: BLE001
                log.warning("contract_hour.rebuild.error", exc=str(e))
            await asyncio.sleep(60)

    collector = asyncio.create_task(_collector_loop(config, sink, state, enricher=enricher))
    http = asyncio.create_task(server.serve())
    probes = asyncio.create_task(probe_loop())
    reference = asyncio.create_task(reference_loop())
    epoch = asyncio.create_task(epoch_loop(), name="epoch_probe")
    tasks: set[asyncio.Task] = {collector, http, probes, reference, epoch}
    if storage is not None:
        tasks.add(asyncio.create_task(warm_sampled_windows(), name="prewarm"))
        tasks.add(
            asyncio.create_task(contract_hour_loop(), name="contract_hour")
        )
    # Only spawn the retention task when actually enabled — otherwise a
    # no-op coroutine returns immediately, and `asyncio.wait(FIRST_COMPLETED)`
    # would treat that as the first finisher and cancel everything else.
    if config.retention.enabled and storage is not None:
        tasks.add(asyncio.create_task(retention_loop(), name="retention"))
    else:
        log.info("retention.disabled")
    enricher_task: asyncio.Task | None = None
    if enricher is not None:
        enricher_task = asyncio.create_task(enricher.run(), name="enricher")
        tasks.add(enricher_task)

    done, pending = await asyncio.wait(
        tasks,
        return_when=asyncio.FIRST_COMPLETED,
    )
    for t in pending:
        t.cancel()
    for t in done:
        # surface any exception
        exc = t.exception()
        if exc:
            log.error("task.exited", task=t.get_name(), exc=str(exc))
    if receipts_client is not None:
        await receipts_client.close()
    return 0


async def _cmd_ping(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    sink = _build_sink(config)
    await sink.deliver(
        AlertEvent(
            rule="ping",
            severity=Severity.INFO,
            key=f"ping:{time.monotonic()}",
            title="monad-ops ping",
            detail=(
                f"Alert channel alive. Node: {config.node.name}. "
                f"Stall thresholds: "
                f"{config.rules.stall.warn_after_sec}s / "
                f"{config.rules.stall.critical_after_sec}s."
            ),
        )
    )
    print("ping delivered", file=sys.stderr)
    return 0


async def _cmd_replay(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    retry = RetrySpikeRule(
        window=config.rules.retry_spike.window,
        warn_pct=config.rules.retry_spike.warn_pct,
        critical_pct=config.rules.retry_spike.critical_pct,
        min_window_tx_avg=config.rules.retry_spike.min_window_tx_avg,
    )
    count = 0
    total_tx = 0
    total_retried = 0
    max_rtp = 0.0
    first: ExecBlock | None = None
    last: ExecBlock | None = None

    async for item in tail_execution_blocks(lookback=args.since, follow=False):
        if isinstance(item, TailError):
            break
        count += 1
        total_tx += item.tx_count
        total_retried += item.retried
        max_rtp = max(max_rtp, item.retry_pct)
        first = first or item
        last = item
        ev = retry.on_block(item)
        if ev is not None:
            print(f"  ALERT: {ev.severity.value}  {ev.detail}")
        if count >= args.limit:
            break

    print(f"parsed {count} blocks")
    if first and last:
        print(f"range: {first.block_number} .. {last.block_number} "
              f"(span {last.block_number - first.block_number})")
        if total_tx:
            print(f"cumulative retry: {total_retried}/{total_tx} = "
                  f"{100 * total_retried / total_tx:.2f}%")
        print(f"peak per-block retry_pct: {max_rtp}%")
    return 0


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="monad-ops")
    p.add_argument("-c", "--config", type=Path, default=None,
                   help="path to config.toml (default: ./config.toml or $MONAD_OPS_CONFIG)")
    sub = p.add_subparsers(dest="cmd", required=True)

    run = sub.add_parser("run", help="run collector + dashboard HTTP server")
    run.add_argument("--host", default="127.0.0.1")
    run.add_argument("--port", type=int, default=8873)

    sub.add_parser("ping", help="send a test alert to the configured channel")
    r = sub.add_parser("replay", help="parse journal history without alerting live")
    r.add_argument("--since", default="10 minutes ago")
    r.add_argument("--limit", type=int, default=10_000)

    return p.parse_args()


def _configure_logging() -> None:
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(20),  # INFO
        processors=[
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.dev.ConsoleRenderer(colors=False),
        ],
    )


def main() -> int:
    _configure_logging()
    args = _parse_args()
    handlers = {
        "run": _cmd_run,
        "ping": _cmd_ping,
        "replay": _cmd_replay,
    }
    return asyncio.run(handlers[args.cmd](args))


if __name__ == "__main__":
    sys.exit(main())
