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


class TelegramConfig(BaseModel):
    bot_token: str
    chat_id: int
    topic_id: int = 0
    source_tag: str = "monad-ops"


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


class DedupConfig(BaseModel):
    cooldown_sec: int = 300


class RulesConfig(BaseModel):
    stall: StallRuleConfig = StallRuleConfig()
    retry_spike: RetrySpikeRuleConfig = RetrySpikeRuleConfig()
    reference_lag: ReferenceLagRuleConfig = ReferenceLagRuleConfig()
    reorg: ReorgRuleConfig = ReorgRuleConfig()
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
    persistence: PersistenceConfig = PersistenceConfig()
    enrichment: EnrichmentConfig = EnrichmentConfig()
    labels: LabelsConfig = LabelsConfig()
    retention: RetentionConfig = RetentionConfig()


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
