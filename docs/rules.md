# Alerting rules

monad-ops ships ten rules. Each one consumes a typed stream — parsed
`__exec_block` records, parsed monad-bft consensus events, or a poller's
snapshots — and emits `AlertEvent`s. This page documents what every rule
fires on, why it exists, and which `config.toml` keys tune it.

Most of the thresholds here are not guesses. They were re-derived from
this node's own history after the rules produced too much noise, and the
commit or audit that moved them is named in the rule's notes.

## Severity and delivery

Four severities: `INFO`, `WARN`, `CRITICAL`, `RECOVERED`.

`RECOVERED` is the all-clear for a rule that was armed. The convention is
one `RECOVERED` per alert envelope — a rule that was WARN, escalated to
CRITICAL, and then cleared emits a single `RECOVERED`, not one per tier.

Two delivery behaviours are worth knowing before you read the table:

- **The Telegram sink drops `INFO` by default** (`drop_severities`).
  INFO-severity events still land on the dashboard, in `/alerts`, and in
  the JSON API — they just don't page you. This is why the reorg rule's
  default tier is INFO.
- **Dedup suppresses repeat alerts on the same key** for
  `rules.dedup.cooldown_sec` (default 300 s). `RECOVERED` deliberately
  bypasses the cooldown, so the all-clear is never held back.

## The rules at a glance

| Rule | Fires on | Severities | Config |
| --- | --- | --- | --- |
| `stall` | no new block for N seconds | WARN, CRITICAL | `[rules.stall]` |
| `retry_spike` | sustained high `retry_pct` | WARN, CRITICAL | `[rules.retry_spike]` |
| `reorg` | same height, different execution `block_id` | INFO, WARN | `[rules.reorg]` |
| `reference_lag` | local tip behind a public reference RPC | WARN, CRITICAL | `[rules.reference_lag]` |
| `assertion` | assertion / panic / FATAL log lines | WARN, CRITICAL | — |
| `block_processing_slowdown` | rolling median `total_us` in stress territory | WARN, CRITICAL | `[rules.block_processing_slowdown]` |
| `network_layer_signal` | monad-bft network-layer error rate | WARN, CRITICAL | `[rules.network_layer_signal]` |
| `process_restart` | a tracked systemd unit's `InvocationID` changed | WARN | `[rules.process_restart]` |
| `waltrace_flood` | `waltrace thread stopped` flood | WARN, CRITICAL | `[rules.waltrace_flood]` |
| `version_watch` | a newer stable package appeared in the apt repo | INFO | `[version_watch]` |

---

## `stall`

Fires when no new block has been produced for a configured number of
seconds.

The gap is measured as wall-clock now minus the last block's own
`timestamp_ms` — its *produced* time, not the moment monad-ops received
it. That distinction is the whole rule. An earlier design measured
against observation time and lit up whenever the event loop froze and
the journal tailer fell behind; during the 2026-04-20 stress test it
produced roughly 120 false alerts while the node kept producing blocks
every ~400 ms. Because monad-ops runs on the same host as the node,
clock skew is negligible, and if you ever do split them a small negative
gap simply silences the rule.

```toml
[rules.stall]
warn_after_sec = 10
critical_after_sec = 30
```

## `retry_spike`

Fires when the average `retry_pct` across a sliding window of blocks
crosses the arm thresholds.

Averaged over a window rather than per block on purpose: a single 100%
block is usually a dense-contention micro-event (an MEV bundle, a mint,
an oracle update). A 60-block window catches contention that is actually
sustained.

Two suppressors matter here:

- **Hysteresis.** Disarm thresholds sit below the arm thresholds. Without
  the gap, an average hovering at the boundary emits a flood of
  WARN→RECOVERED pairs, and since RECOVERED bypasses dedup by design,
  the channel fills with green "normalized" messages.
- **Quiet-period gate.** On an idle network, blocks carry a handful of
  transactions and one retried tx out of three is 33% retry_pct.
  `min_window_tx_avg` suppresses *arming* while the window's average
  tx-per-block is below the floor. It deliberately does not block
  disarming, so the all-clear still fires promptly when things quiet
  down.

`warn_pct` moved 50 → 65 alongside the gate after the arming behaviour
was reworked in April 2026.

```toml
[rules.retry_spike]
window = 60
warn_pct = 65.0
critical_pct = 75.0
min_window_tx_avg = 50.0
```

## `reorg`

Fires when the same `block_number` is seen twice with a different
`block_id` in the `__exec_block` stream.

**Read the name carefully.** `block_id` is the execution-layer hash, a
different namespace from the EVM-canonical block hash that
`eth_getBlockByNumber` returns — sampling canonical RPC blocks shows the
two never match, on reorged and non-reorged blocks alike. What this rule
measures is execution-layer re-execution at the same height before
finalization: HotStuff-2 speculation, not a finality violation. Monad's
pipelined finality means a divergence at depth 0–1 is expected protocol
behaviour and the chain finalizes correctly regardless. Nobody reading
"reorg" here should infer a chain rollback.

Severity follows the 2026-05-03 reframe:

- **INFO** for a single divergence — surfaced for visibility, and since
  the Telegram sink drops INFO, it won't page you.
- **WARN** when `cluster_threshold` divergences land inside
  `cluster_window_sec`. Clusters can correlate with chain instability or
  correlated validator drop-off, so they're worth a glance. While a
  cluster stays open, further events stay INFO rather than re-arming.

The operationally interesting payload is the journal-capture artifact
written on each fire, not the alert itself.

Memory is bounded: the rule keeps the last 2000 `(number, id)` pairs
rather than the whole history. Divergences show up close in time to the
original observation or not at all. That bound is a rule-level constant,
not a config key.

```toml
[rules.reorg]
cluster_window_sec = 1800
cluster_threshold = 3
recent_window_sec = 86400
```

`recent_window_sec` is decoupled from the cluster window on purpose: it
drives the dashboard's "recent reorgs" count, and you want a 24 h
overview even when no cluster is active.

## `reference_lag`

Fires when the local node is sustained behind a public reference RPC.
This is the "is it me or is it the whole network" question during a
stress test.

A positive delta means the reference is ahead of us, so we're lagging. A
negative delta (our RPC sampling fresher than the public endpoint) is
normal and never alerts. The window is summarized with `min(delta)` — the
*best* sample in recent history has to be bad before this counts as
sustained lag. Probe failures are soft-ignored: they don't change state
and don't update the window, so a five-second blip at the public endpoint
can't raise a lag alert.

```toml
[rules.reference_lag]
warn_blocks = 15
critical_blocks = 60
window = 2
```

The reference endpoint itself lives in `[node]`
(`reference_rpc_url`, `reference_poll_sec`); set the URL to an empty
string to disable the probe and this rule with it.

## `assertion`

Fires on assertion, panic and FATAL patterns in the monad journals. It is
stateless — one parsed event in, one alert out — and has no config
section. Dedup is handled downstream.

| Kind | Matches | Severity |
| --- | --- | --- |
| `cxx_assert` | `Assertion '...' failed` | CRITICAL |
| `rust_panic` | `thread '...' panicked at ...` | CRITICAL |
| `qc_overshoot` | high QC too far ahead of block tree root | CRITICAL |
| `chunks` | `Disk usage: 0.99xx ... Chunks: N fast` | WARN, CRITICAL at ≥ 0.95 |
| `io_uring_init` | `io_uring_queue_init_params` failed at startup | CRITICAL |
| `event_ring_mmap` | `monad_event_ring_mmap` startup failure | CRITICAL |
| `generic_fatal` | catch-all FATAL / fatal error | CRITICAL |

Assertions and panics are CRITICAL without qualification: monad-execution
and monad-bft don't recover from them without a restart. Chunk exhaustion
is the one graduated case — it stays WARN below 0.95 because there's
still a window to add disk or roll chunks before the node halts.

## `block_processing_slowdown`

A predictive rule on the rolling median of `total_us`. It fires while the
node is still producing blocks but per-block processing time has shifted
into stress territory — the point of it is to arrive before `stall` does.

Defaults are calibrated against the 2026-04-20 stress test, where the
median ran 1.2 ms quiet, 24.6 ms mid-stress and 81.3 ms at peak. So
`warn_us` at 10 ms is about 5× the quiet baseline, and `critical_us` at
50 ms is deep into stress territory while still well under the 400 ms
inter-block budget. A 120-block window is roughly 48 s at 2.5 blk/s,
which is enough to be robust against single outliers.

Worth stating plainly: this is a node-stress rule, not a reorg predictor.
Correlating it against observed reorg clusters came back negative —
`total_us` was flat ahead of the clusters.

```toml
[rules.block_processing_slowdown]
window = 120
warn_us = 10000
critical_us = 50000
```

## `network_layer_signal`

Watches the aggregate rate of three monad-bft network-layer error classes
over a rolling 5-minute window: RaptorCast decrypt failures, wireauth
session timeouts, and consensus_state timestamp-validation failures. All
three are sparse at baseline.

This rule has been retuned more than any other, and the history explains
the shape of its config:

- Arm thresholds sit at p99.7 / p99.9 of the measured 30-day
  distribution. Background p90 is about 1 event per window; real
  incidents run 91–2582. At the original `warn = 10` (roughly p98) this
  rule was 74% of all Telegram volume.
- Explicit disarm levels at roughly half the arm thresholds, plus a 600 s
  recovery confirmation, kill the boundary flap. The observed flap median
  gap was four minutes, so the confirmation window is deliberately longer.
- **Peer-diversity gates.** A single chronically desynced neighbour
  spamming RaptorCast crosses the volume thresholds on its own — observed
  2026-05-03 and 2026-05-06, with no correlation to chain-side stress.
  Below `warn_min_unique_peers` the rule stays silent entirely; between
  the WARN and CRITICAL peer floors it holds at WARN with a hint that the
  gate is what's holding it there. Set either to `1` to disable that
  tier's gate.

```toml
[rules.network_layer_signal]
window_sec = 300
warn_count = 25
critical_count = 50
warn_disarm_count = 12
critical_disarm_count = 25
recovery_confirm_sec = 600.0
warn_min_unique_peers = 2
critical_min_unique_peers = 3
```

## `process_restart`

Fires WARN when a tracked systemd unit's `InvocationID` changes between
polls, meaning the unit restarted — whether you triggered it or systemd
auto-restarted it after a crash.

It fills a gap between two things that already exist. `stall` fires on
block-cadence gap, which arrives ~30 s after a real chain-side stop. The
`services` probe reports current activity, so it reads "active" across an
entire uptime envelope and can't tell you the service restarted a moment
ago. This rule pings at the moment of the restart, before downstream
signals catch up.

Severity is WARN and stays WARN. A restart is not by itself a chain
incident — it could be a planned upgrade, an auto-restart after a
transient crash, or a real assertion failure. CRITICAL is reserved for
sustained chain-impact rules. The detail line carries the service, the
new `InvocationID` and the sub-state so you can decide.

Two policies keep it quiet when it should be:

- **First sight is silent.** The first time the rule sees a service it
  records the current `InvocationID` and waits for the next sample, so
  monad-ops starting up never pages you.
- **Probe errors are soft-ignored.** A timed-out or non-zero `systemctl`
  call leaves state untouched. Same lesson as `reference_lag`, from the
  2026-04-20 event-loop freeze that produced phantom probe criticals.

The tracked services are `[node].services`.

```toml
[rules.process_restart]
poll_interval_sec = 60
```

## `waltrace_flood`

Detects the monad-bft v0.14.5 `waltrace thread stopped` flood.

v0.14.5 moved WAL persistence onto a dedicated waltrace thread, and that
thread could die silently: the current chunk fills to exactly 1 GiB, a
fresh chunk is created in the same second and never written again. From
that point WAL persistence and crash recovery are off, and the only
symptom is monad-bft emitting this ERROR at ~220–250 per second:

```
{"level":"ERROR","fields":{"message":"waltrace thread stopped"},
 "target":"monad_node"}
```

Live consensus is unaffected, which is exactly why it needs a rule — the
node looks healthy on every other metric while crash recovery is quietly
disabled and the journal grows by gigabytes. Observed on this node
2026-06-11 and by three other operators in `#fullnode-discussion`; the
Foundation published an RCA on 2026-06-17 and the bug is fixed in
v0.15.0. The rule is kept for nodes still on the affected releases.

Severity ladder: WARN at first detection, CRITICAL if still armed
`critical_after_sec` later (you haven't restarted and WAL persistence has
been off the whole time), RECOVERED once the window fully drains. There
is no hysteresis factor because there is no boundary to flap on — the
healthy rate is exactly zero and the sick rate is ~250/sec, so disarming
requires a completely empty window.

`capture = true` snapshots the WAL directory and journal pre-context the
moment the rule arms. That matters because the hourly cleanup deletes the
proof — the 0-byte chunk — a few hours later.

```toml
[rules.waltrace_flood]
window_sec = 60
warn_count = 10
critical_after_sec = 900
capture = true
wal_dir = "/home/monad/monad-bft/wal"
```

## `version_watch`

Emits INFO when a newer stable version of the monad package appears in
the configured apt repo, with a daily reminder while the upgrade is
outstanding and a RECOVERED once you pick it up.

It polls the repo's `Packages.gz` directly and compares against
`dpkg-query`, so it needs neither `apt-get update` nor sudo.
`skip_substrings` filters out `-debug`, `-preview`, `~rc` and friends so
only stable releases count.

It's a rule rather than a probe because of the cadence. The standard
probe loop fires WARN/CRITICAL on every tick while a probe is non-ok,
with dedup cooldown as the only suppressor — for a package release you
want one alert when the version appears plus a daily reminder, not a
steady drumbeat. And a new release is informational: INFO and RECOVERED
both map to GREEN under the Foundation colour-code framework. If a
release is flagged ORANGE or RED, that arrives through
`#fullnode-announcements` independently.

Set `enabled = false` on a host where the package wasn't installed via
apt.

```toml
[version_watch]
enabled = true
package = "monad"
poll_interval_sec = 3600
reminder_interval_sec = 86400
timeout_sec = 20.0
```

## Not a rule: host probes

The host probes (systemd service state, key-backup exposure, TrieDB disk
health, UDP config, filesystem usage, fd limits) run on their own loop and
emit through the same sinks, but they are not rules and have no
`[rules.*]` section. See `/api/probes` on a running instance for their
current output.
