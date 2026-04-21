# Observations

Operator-side notes from running `monad-ops` against Monad testnet.
What this node saw, with data. Hypotheses are marked explicitly — they
are not conclusions, and may be wrong.

Raw JSON traces for each event are available from the dashboard API
at `/api/reorgs/{block_number}?window=N`.

---

## 2026-04-19 / 2026-04-20 — first reorg window (23 events, ~14 h)

### Summary

Between **2026-04-19 22:43:04 UTC** and **2026-04-20 12:29:58 UTC** the
local `reorg` detector fired **23 times** — each event being the same
`block_number` being observed with a different `block_id` after first
persistence. These events resolved into **16 temporal clusters** (gap
threshold ≥ 5 min).

The node itself stayed on the majority tip throughout the window,
tracking `testnet-rpc.monad.xyz` closely (≤ 2 blocks of drift at any
probe sample).

### Content-profile split (the main observation)

The 23 events fall cleanly into three content regimes, which is the
detail that looked interesting enough to publish:

| regime | events | time window (UTC) | typical `tx_count` | typical `retry_pct` |
|---|---|---|---|---|
| light-load reorgs | 8 | 2026-04-19 22:43 → 2026-04-20 01:11 | 2–5 | 0–60 % |
| stress-window reorgs | 7 | 2026-04-20 01:44 → 03:37 | 361–2225 | 87–99 % |
| post-stress tail | 8 | 2026-04-20 04:24 → 12:29 | varies (1 → 1601) | 0–85 % |

Light-load reorgs sit on near-empty blocks (2 tx, ~225 kB gas each).
Stress-window reorgs sit on dense blocks (100–200 M gas each, ~99 %
retry). The post-stress tail is mixed — includes a 1-tx outlier, then
multi-block clusters with high `tx_count` but comparatively moderate
retry percentages.

### All 23 events

`before` / `after` IDs shown truncated here for readability; full 64-char
hex available from the per-event trace endpoint listed in
[Raw traces](#raw-traces) below.

| # | UTC time | block | cluster | tx | retry % | gas_used | id before → id after |
|---|---|---|---|---|---|---|---|
| 1 | 2026-04-19 22:43:04 | 26543283 | 1 | 2 | 0.0 | 225 280 | `0x8bca75d2…80987e` → `0x4e18d392…ab61d4` |
| 2 | 2026-04-19 23:30:12 | 26550173 | 2 | 3 | 33.3 | 1 900 781 | `0x5b72e641…2a4893` → `0x183e534b…8b1644` |
| 3 | 2026-04-20 00:31:50 | 26559180 | 3 | 2 | 0.0 | 225 337 | `0x9bd12f72…7fec9f` → `0x1b645a48…caebae` |
| 4 | 2026-04-20 00:32:53 | 26559333 | 3 | 5 | 60.0 | 780 890 | `0x715c4075…244fe7` → `0x6e7f8b6b…4b6422` |
| 5 | 2026-04-20 00:48:52 | 26561661 | 4 | 4 | 50.0 | 993 926 | `0x2a3ec895…53dbc8` → `0x5ee94bc9…eee4ee` |
| 6 | 2026-04-20 01:07:31 | 26564372 | 5 | 2 | 0.0 | 225 070 | `0x55e77f3f…3e7f04` → `0x1f401952…e220b9` |
| 7 | 2026-04-20 01:07:59 | 26564439 | 5 | 2 | 0.0 | 224 968 | `0x2b042aba…39e23f` → `0x8ec9d0be…4637a7` |
| 8 | 2026-04-20 01:11:08 | 26564889 | 5 | 2 | 0.0 | 225 152 | `0xff6b27bf…aa719c` → `0x9b01f51b…56da5a` |
| 9 | 2026-04-20 01:44:15 | 26569727 | 6 | 1600 | 89.9 | 199 975 096 | `0x1fd29dfe…1c95e6` → `0xdd4dc1bd…ae25b7` |
| 10 | 2026-04-20 01:44:27 | 26569754 | 6 | 1502 | 87.8 | 187 725 103 | `0xbe7f7270…a01db3` → `0xf9c1c2b0…ce8705` |
| 11 | 2026-04-20 02:01:22 | 26572243 | 7 | 2225 | 99.7 | 200 000 000 | `0xcfe1b8a6…4194c5` → `0xfb0fb53d…60ed2e` |
| 12 | 2026-04-20 02:14:47 | 26574200 | 8 | 571 | 98.6 | 11 970 000 | `0x87ac9157…45c16c` → `0xa74174da…0c64d8` |
| 13 | 2026-04-20 02:27:50 | 26576098 | 9 | 361 | 98.1 | 7 560 000 | `0x6a47c42d…6d0cee` → `0x72efe03a…7b7a98` |
| 14 | 2026-04-20 03:12:59 | 26582542 | 10 | 1403 | 99.3 | 97 425 532 | `0x9679d25e…10538d` → `0x05102bd3…8abb32` |
| 15 | 2026-04-20 03:37:57 | 26586198 | 11 | 1818 | 98.6 | 42 837 000 | `0x4e3e2280…c6bc0c` → `0x77c36ba6…8e17a0` |
| 16 | 2026-04-20 04:24:52 | 26593172 | 12 | 1 | 0.0 | 0 | `0x895f52a9…cfdd9c` → `0x73244f3a…6c0ff5` |
| 17 | 2026-04-20 07:26:44 | 26620047 | 13 | 480 | 76.7 | 10 059 000 | `0xc5e52834…7925bc` → `0x56d88bc2…90cdcd` |
| 18 | 2026-04-20 07:29:41 | 26620481 | 13 | 825 | 84.6 | 28 536 000 | `0x25a4fc39…d398ee` → `0x848943fd…e65b38` |
| 19 | 2026-04-20 07:33:01 | 26620961 | 13 | 1601 | 17.9 | 200 000 000 | `0x0a3fa3f9…078c1e` → `0x8239ccd3…928c47` |
| 20 | 2026-04-20 12:11:06 | 26661597 | 14 | 1496 | 36.8 | 199 886 426 | `0xdb21aca1…32b7dd` → `0x0822298b…30d52d` |
| 21 | 2026-04-20 12:11:07 | 26661651 | 14 | 1601 | 16.6 | 199 998 838 | `0xc2fcba8b…02fa8d` → `0x34c7f103…c143b0` |
| 22 | 2026-04-20 12:21:15 | 26663050 | 15 | 1596 | 19.3 | 199 975 632 | `0x5c6ffe7e…0723b6` → `0x9fcc4921…fced74` |
| 23 | 2026-04-20 12:29:58 | 26664383 | 16 | 1596 | 17.2 | 199 997 933 | `0xa53f03eb…10aa7e` → `0x93e1ab3c…78f8e8` |

### Specifically interesting within the window

- **Cluster 5** (01:07:31 → 01:11:08 UTC) — three light-load reorgs within
  ~3.5 min, every one of them on a 2-tx near-empty block with 0 retry.
  Not a load-induced fork by construction.
- **Cluster 14** (12:11:06 and 12:11:07 UTC) — two reorgs **1 second apart**
  on blocks `26661597` and `26661651`, 54 blocks apart. At ~0.4 s block
  cadence that is ~21 s of block history, both ends of which re-emitted
  within the same wall-second.
- **Event 16** (04:24:52 UTC) — isolated mid-gap reorg on a **1-tx,
  0-gas** block. Sits between the stress window and the second burst
  (07:26 UTC), not clearly attributable to either regime.

### Node-side context during the window

- Local block cadence held ~0.4 s throughout (no stall alerts fired
  during this window).
- `reference_rpc` drift ≤ 2 blocks against `testnet-rpc.monad.xyz`.
- No `assertion` / `CXX_ASSERT` / `RUST_PANIC` / chunk-exhaustion alerts
  from `monad-execution` during or adjacent to any of the 23 events.
- `monad-ops` process state reset mid-window across a couple of
  iter-5 restarts; persisted `reorg_count` survived via sqlite
  bootstrap so the counter is contiguous across restarts.

### Hypothesis (explicitly a hypothesis, not a claim)

Most of these are consistent with HotStuff-2 view changes resolving
into brief reconvergence forks that get corrected one block later — a
single height re-emits with the new majority-branch `block_id`. This
would predict:

- Light-load reorgs being slightly *more* likely when the block content
  is small, because there's less wall-clock spent in execution for the
  loser-fork to have been propagated before the view flips (consistent
  with Cluster 5).
- Stress-window reorgs being heavier on retry-laden high-gas blocks,
  because dense execution is where a brief fork's two versions would
  both exist long enough to be observed by some node somewhere
  (consistent with Cluster 6 onward).

It would *not* predict two reorgs 54 blocks apart inside a one-second
wall-clock window (Cluster 14). That specific shape looks more like a
slightly longer-lived forklet that resolved in a single view change
touching both heights.

This is the view from a single operator. A Foundation engineer with
the full consensus log should be able to confirm, refute, or refine
any of the above from authoritative data.

### Raw traces

Each event has a full-context JSON trace (reorged block + 30 neighbors
on each side) at:

- `GET https://ops.rustemar.dev/api/reorgs/{block_number}` — chain-derived
  fields for a single reorg
- `GET https://ops.rustemar.dev/api/reorgs` — index of all 23 events,
  compact form

The detector source is in
[`monad_ops/rules/reorg.py`](monad_ops/rules/reorg.py) and the reorg
state that bootstraps across process restarts lives in
[`monad_ops/storage.py::load_reorg_history`](monad_ops/storage.py).

---

## Log

- 2026-04-21: file created. 23 reorgs from the 2026-04-19/20 window
  documented. Raw journal lines around these events are **not
  preserved** — `systemd-journald` rotation reclaimed them before they
  were captured. `journald` retention raised to 30 GB afterward to
  avoid the same loss on future events; a follow-up is planned to
  snapshot raw journal context automatically on any `reorg` event.
