"""Pydantic-backed configuration loader.

Reads ``config.toml`` from the project root (or a path supplied via the
MONAD_OPS_CONFIG env var) and returns a typed ``Config`` instance.
"""

from __future__ import annotations

import os
import tomllib
from pathlib import Path

from pydantic import BaseModel, Field


class NodeConfig(BaseModel):
    name: str
    rpc_url: str = "http://127.0.0.1:8080"
    services: list[str] = Field(default_factory=lambda: ["monad-bft", "monad-execution", "monad-rpc"])
    # Public reference RPC used to answer "is the whole network halted
    # or just my node?" — compared against local tip in the dashboard.
    # Empty string disables the reference probe.
    reference_rpc_url: str = "https://testnet-rpc.monad.xyz"
    reference_poll_sec: int = 15


class OperatorConfig(BaseModel):
    """Public identity surface for the operator-readiness profile page.

    Powers ``/profile/<handle>`` — a transparency surface aggregating
    uptime, version compliance, reorg awareness, validator-timeout %,
    base-fee response and links to the public artefact set. ``enabled=
    false`` removes the route entirely; the rest of the dashboard is
    unaffected.

    All fields are optional with conservative defaults so a fresh
    operator forking monad-ops can ship the dashboard without filling
    them in — the profile page just won't render until they configure
    an identity.
    """
    enabled: bool = True
    handle: str = ""
    region: str = ""
    chain: str = "testnet"
    since_date: str = ""
    public_repo: str = ""


class TelegramConfig(BaseModel):
    bot_token: str
    chat_id: int
    topic_id: int = 0
    source_tag: str = "monad-ops"
    drop_severities: list[str] = ["info"]


class AlertsConfig(BaseModel):
    telegram: TelegramConfig | None = None


class StallRuleConfig(BaseModel):
    warn_after_sec: int = 10
    critical_after_sec: int = 30


class RetrySpikeRuleConfig(BaseModel):
    window: int = 60
    warn_pct: float = 65.0
    critical_pct: float = 75.0
    # Quiet-period gate: suppress arming WARN/CRITICAL when the
    # window's average tx_count per block is below this. retry_pct
    # over tiny blocks is statistical noise (one retried tx of three
    # is 33%). Gate applies only to arming — RECOVERED still fires.
    min_window_tx_avg: float = 50.0


class ReferenceLagRuleConfig(BaseModel):
    # Blocks behind the external reference RPC required to arm each
    # severity. Block time on testnet is ~400 ms, so 15 blocks ≈ 6 s
    # of lag, 60 blocks ≈ 24 s — both well above ordinary jitter.
    warn_blocks: int = 15
    critical_blocks: int = 60
    # Consecutive good samples needed to confirm a state change.
    # With 15 s reference-poll cadence, window=2 ≈ 30 s confirmation.
    window: int = 2


class ProcessRestartRuleConfig(BaseModel):
    """Service-restart detector via systemd InvocationID polling.

    Per-service tracking — fires WARN on any change between polls. Uses
    the same service list as ``probe_services`` (config.node.services).
    """
    poll_interval_sec: int = 60


class NetworkLayerSignalRuleConfig(BaseModel):
    """Aggregate rate of monad-bft network-layer error events.

    Watches three sparse-at-baseline event classes (RaptorCast decrypt
    fail + wireauth session timeout + consensus_state timestamp
    validation fail) over a rolling 5-min window. Defaults calibrated
    on this node 2026-05-03 — baseline ~0.07 events/min (calm hour),
    burst ~6.7 events/min (this morning's reorg storm). 5/15 thresholds
    give clean separation without firing on isolated stray events.
    """
    window_sec: int = 300  # 5 minutes
    warn_count: int = 10   # raised 2026-05-04: 5 was bouncing on healthy network noise
    critical_count: int = 15
    # Diversity gates. Single-peer storms (one chronically desynced
    # neighbour spamming RaptorCast) routinely cross the volume
    # thresholds — observed 2026-05-03 and 2026-05-06 — with zero
    # correlation to chain-side stress. Below the WARN floor we stay
    # silent entirely; between WARN and CRITICAL we hold at WARN with a
    # gate-hint. Set either to 1 to disable that tier's gate.
    warn_min_unique_peers: int = 2
    critical_min_unique_peers: int = 3


class BlockProcessingSlowdownRuleConfig(BaseModel):
    """Predictive rule on rolling median ``total_us``.

    Fires before a stall: while the node still produces blocks, but
    per-block processing time has shifted into stress territory.
    Defaults calibrated against the 2026-04-20 stress test (median
    1.2 ms quiet → 24.6 ms mid-stress → 81.3 ms peak):

      ``warn_us`` 10 ms  : ~5× quiet baseline, catches load shift early
      ``critical_us`` 50 ms : ~25× baseline, deep into stress territory
                              but well below 400 ms inter-block budget
      ``window`` 120     : ~48 s at 2.5 blk/s, robust to single outliers
    """
    window: int = 120
    warn_us: int = 10_000
    critical_us: int = 50_000


class ReorgRuleConfig(BaseModel):
    """Cluster-based severity for the reorg detector.

    Default behaviour: a single reorg is WARN. Three reorgs inside a
    30-minute window escalate to CRITICAL. Tuned against the 23-reorg
    multi-day window from 2026-04-19, where five clusters carried the
    operationally-interesting signal and isolated events were noise.
    """
    cluster_window_sec: int = 30 * 60
    cluster_threshold: int = 3
    # Window used by the dashboard to count "recent reorgs" alongside the
    # lifetime counter. Decoupled from cluster_window_sec on purpose: the
    # operator wants a 24h overview even when no cluster is active.
    recent_window_sec: int = 24 * 3600


class TailerConfig(BaseModel):
    """journalctl-follow self-heal.

    A live ``journalctl -f`` can silently stop delivering lines after a
    journald rotation: readline() then blocks forever — no EOF, no error
    — while the node keeps producing. That is the failure that froze
    ingestion for 30h on 2026-06-05 under a monad-bft log flood. The
    tailer bounds each read by ``idle_timeout_sec`` and respawns the
    child on silence; repeated respawns inside the window escalate to a
    CRITICAL alert instead of spinning quietly.
    """
    # Both monad units log every sub-second, so this much total silence
    # is an unambiguous broken-follow signal, not a quiet node.
    idle_timeout_sec: float = 30.0
    max_respawns: int = 10
    respawn_window_sec: float = 300.0


class DedupConfig(BaseModel):
    cooldown_sec: int = 300


class WaltraceFloodRuleConfig(BaseModel):
    """v0.14.5 ``waltrace thread stopped`` flood detector.

    Baseline is strictly zero; the flood runs ~250 lines/sec, so the
    thresholds need no calibration headroom. CRITICAL fires if the
    operator hasn't restarted within ``critical_after_sec``. The
    evidence capture snapshots the wal dir + journal pre-context the
    moment the rule arms, because the hourly monad-cruft cleanup
    deletes the proof (the 0-byte chunk) after 5 h.
    """
    window_sec: int = 60
    warn_count: int = 10
    critical_after_sec: int = 900  # 15 min un-restarted -> RED
    capture: bool = True
    wal_dir: str = "/home/monad/monad-bft/wal"


class RulesConfig(BaseModel):
    stall: StallRuleConfig = StallRuleConfig()
    retry_spike: RetrySpikeRuleConfig = RetrySpikeRuleConfig()
    reference_lag: ReferenceLagRuleConfig = ReferenceLagRuleConfig()
    reorg: ReorgRuleConfig = ReorgRuleConfig()
    block_processing_slowdown: BlockProcessingSlowdownRuleConfig = BlockProcessingSlowdownRuleConfig()
    network_layer_signal: NetworkLayerSignalRuleConfig = NetworkLayerSignalRuleConfig()
    process_restart: ProcessRestartRuleConfig = ProcessRestartRuleConfig()
    waltrace_flood: WaltraceFloodRuleConfig = WaltraceFloodRuleConfig()
    dedup: DedupConfig = DedupConfig()


class PersistenceConfig(BaseModel):
    """SQLite-backed persistence for blocks and alerts.

    Defaults: enabled, writes to ``./data/state.db`` relative to the
    working directory. On startup, State bootstraps ``bootstrap_blocks``
    most-recent blocks from the DB so the dashboard isn't empty after
    a restart.
    """
    enabled: bool = True
    path: str = "data/state.db"
    bootstrap_blocks: int = 12_000


class EnrichmentConfig(BaseModel):
    """Receipt-based enrichment: fetch per-tx receipts and attribute
    retry activity to the contracts that appeared.

    Requires persistence to be enabled (the enriched rows live in the
    same SQLite database alongside the blocks table).
    """
    enabled: bool = True
    queue_size: int = 5000
    rpc_timeout_sec: float = 5.0


class LabelsConfig(BaseModel):
    """Path to the contract label registry JSON.

    Ships as ``labels.json`` at the repo root; community contributions
    extend it over time. Missing file → empty map, not an error.
    """
    path: str = "labels.json"


class ValidatorSetConfig(BaseModel):
    """Active validator-set snapshot on the public dashboard.

    Polls the staking precompile (``0x...1000``) via the local node's
    RPC every ``poll_interval_sec``. The set itself changes on epoch
    boundaries (~5.5h on testnet), so a 5-min cadence is generous.
    Mirrors the validator-set block already rendered in
    ``monitoring/monad_monitor.py`` Telegram heartbeats — same data,
    public surface.

    ``enabled=false`` disables both the loop and the API endpoint.
    """
    enabled: bool = True
    poll_interval_sec: int = 300
    timeout_sec: float = 10.0


class VersionWatchConfig(BaseModel):
    """Hourly check that the locally installed monad package is current.

    Polls the configured apt repo's ``Packages.gz`` (no apt-get update
    or sudo required), compares against ``dpkg-query``, and emits an
    INFO/GREEN alert when a newer stable version appears. A daily
    reminder fires while the upgrade is outstanding; a RECOVERED fires
    when the operator picks it up.

    ``enabled=false`` disables the loop entirely — useful on a host
    where the package wasn't installed via apt.
    """
    enabled: bool = True
    package: str = "monad"
    packages_url: str = (
        "https://pkg.category.xyz/dists/noble/main/binary-amd64/Packages.gz"
    )
    poll_interval_sec: int = 3600
    reminder_interval_sec: int = 24 * 3600
    skip_substrings: list[str] = Field(
        default_factory=lambda: ["-debug", "-preview", "~preview", "~rc", "-rc"]
    )
    timeout_sec: float = 20.0


class RetentionConfig(BaseModel):
    """Background pruning of historical rows.

    Prevents unbounded growth: at ~216K blocks/day on testnet + enrichment
    aggregates, a multi-week unmanaged DB accumulates tens of millions of
    rows. A daily pass deletes rows older than ``keep_days`` so the hot
    dataset stays bounded.

    ``enabled=false`` (default) means "accrue forever" — safe for a
    fresh deployment where keeping the full stress-test archive matters.
    Flip on once the dashboard is stable and you're past interesting
    historical events.
    """
    enabled: bool = False
    # 14 days of testnet @ 2.5 bps ≈ 3M blocks, ~few M aggregate rows.
    # Well within sqlite's comfort zone.
    keep_days: int = 14
    # How often to run the pruning task. Daily is enough; the operation
    # is idempotent and cheap (indexed DELETE).
    interval_hours: int = 24


class Config(BaseModel):
    node: NodeConfig
    alerts: AlertsConfig = AlertsConfig()
    rules: RulesConfig = RulesConfig()
    tailer: TailerConfig = TailerConfig()
    persistence: PersistenceConfig = PersistenceConfig()
    enrichment: EnrichmentConfig = EnrichmentConfig()
    labels: LabelsConfig = LabelsConfig()
    retention: RetentionConfig = RetentionConfig()
    version_watch: VersionWatchConfig = VersionWatchConfig()
    validator_set: ValidatorSetConfig = ValidatorSetConfig()
    operator: OperatorConfig = OperatorConfig()


def load_config(path: Path | str | None = None) -> Config:
    """Load configuration from a TOML file.

    Resolution order:
      1. Explicit ``path`` argument.
      2. ``MONAD_OPS_CONFIG`` environment variable.
      3. ``config.toml`` in the current working directory.
    """
    if path is None:
        path = os.environ.get("MONAD_OPS_CONFIG") or "config.toml"
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"config file not found: {path}")
    with path.open("rb") as fh:
        raw = tomllib.load(fh)
    return Config.model_validate(raw)
