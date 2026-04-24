"""Static replay artifact — single self-contained HTML for a time window.

Designed for the post-stress-test publication pattern: an operator
shows their node's view of an event window (e.g. 2026-04-20 stress
batches across epochs 532/533/534) without keeping a live URL up
forever. The output HTML inlines Chart.js and the dataset, so it can
be hosted on a static file server, IPFS, a Gist, or simply emailed.

CLI usage::

    monad-ops replay-export \\
        --from "2026-04-20 00:00" --to "2026-04-20 23:59" \\
        --out replay-2026-04-20.html

Both ``--format html`` (default, single self-contained file) and
``--format json`` (raw data dump) are supported. JSON is for
downstream tooling that wants to compute derived metrics; HTML is
the human-readable artifact.

What's included in the dump (driven by the F4/F6 Sprint-2 audience):
  * Aggregate block metrics (blocks count, peak/avg rtp, total tx, etc.)
  * Consensus aggregate (validator_timeout_pct chain-wide, local fires)
  * Base-fee aggregate (avg/min/max in gwei + sample count)
  * Per-minute consensus series (for the validator-timeouts chart)
  * Downsampled per-block base-fee series (for the fee curve)
  * Downsampled per-block throughput series (rtp + tx + tps)

What's NOT included on purpose (would balloon the artifact):
  * Per-block raw rows
  * Top-contracts ranking (operator-tools concern, not Foundation-facing)
  * Reorg trace data
  * Critical-incidents log
"""

from __future__ import annotations

import html
import json
import time
from datetime import datetime, timezone
from pathlib import Path

from monad_ops.storage import Storage


def assemble_window_data(
    storage: Storage,
    from_ts_ms: int,
    to_ts_ms: int,
    *,
    sampled_points: int = 300,
) -> dict:
    """Pull every replay-relevant aggregate + series for [from, to].

    Sampled-block series uses ``target_points`` so a 24h replay returns
    ~300 bins instead of 200K rows. BFT-minute series is per-minute
    raw — at 1440 rows/day it's already small enough.
    """
    aggregate = storage.block_metrics_aggregate(
        from_ts_ms=from_ts_ms, to_ts_ms=to_ts_ms
    )
    consensus = storage.load_bft_window(from_ts_ms, to_ts_ms)
    base_fee_aggregate = storage.load_base_fee_window(from_ts_ms, to_ts_ms)
    bft_series = storage.list_bft_minutes(from_ts_ms, to_ts_ms)
    base_fee_series = storage.sampled_bft_base_fee(
        from_ts_ms, to_ts_ms, target_points=sampled_points,
    )
    blocks_series = storage.sampled_blocks(
        from_ts_ms=from_ts_ms, to_ts_ms=to_ts_ms,
        target_points=sampled_points,
    )

    return {
        "window": {
            "from_ts_ms": from_ts_ms,
            "to_ts_ms": to_ts_ms,
            "from_iso": _iso_utc(from_ts_ms),
            "to_iso": _iso_utc(to_ts_ms),
            "span_sec": (to_ts_ms - from_ts_ms) / 1000.0,
        },
        "aggregate": aggregate,
        "consensus": consensus,
        "base_fee": base_fee_aggregate,
        "series": {
            "bft": bft_series,
            "base_fee": base_fee_series,
            "blocks": blocks_series,
        },
        "exported_at_ms": int(time.time() * 1000),
        "schema_version": 1,
    }


def render_static_html(
    data: dict,
    *,
    node_name: str,
    chart_js_path: Path,
) -> str:
    """Wrap the data dict in a single self-contained HTML document.

    Inlines Chart.js (~200 KB) and the JSON payload so the artifact
    has no external dependencies. Output size scales with window
    length: typical 1 h replay is ~250 KB, a 24 h replay is ~400 KB.
    """
    chart_js = chart_js_path.read_text()
    # ``</script>`` inside a <script>const x = "..."</script> would
    # close the wrapping tag. JSON has no </script> by definition but
    # belt-and-braces escape forward-slashes. Same for HTML special
    # chars in metadata strings.
    data_json = json.dumps(data, separators=(",", ":")).replace("</", "<\\/")
    win = data["window"]
    title = (
        f"monad-ops replay · {node_name} · "
        f"{win['from_iso']} → {win['to_iso']}"
    )

    return _HTML_TEMPLATE.format(
        title=html.escape(title),
        node_name=html.escape(node_name),
        from_iso=html.escape(win["from_iso"]),
        to_iso=html.escape(win["to_iso"]),
        chart_js=chart_js,
        data_json=data_json,
    )


def _iso_utc(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S UTC"
    )


# Single-file HTML template. Visual style mirrors the dashboard's
# dark theme but stripped to the load-bearing pieces — viewers landing
# here from a Foundation post or a Discord link want the headline
# numbers fast, not the full operator dashboard.
_HTML_TEMPLATE = """<!doctype html>
<html lang=en>
<head>
<meta charset=utf-8>
<meta name=viewport content="width=device-width, initial-scale=1">
<meta name=robots content="noindex">
<title>{title}</title>
<style>
  :root {{
    --bg: #0d1117; --fg: #d7dae0; --muted: #8b8f99;
    --card: #141a23; --border: #2a2f38; --accent: #5b9cf5;
    --ok: #6bcf76; --mid: #5b9cf5; --warn: #ffb454; --crit: #ff6b6b;
  }}
  * {{ box-sizing: border-box; }}
  body {{
    margin: 0; padding: 24px;
    background: var(--bg); color: var(--fg);
    font: 14px/1.5 -apple-system, BlinkMacSystemFont, "Segoe UI",
          Roboto, "Helvetica Neue", sans-serif;
    max-width: 1100px; margin-inline: auto;
  }}
  header {{ margin-bottom: 24px; }}
  h1 {{ margin: 0 0 4px; font-size: 18px; font-weight: 600; }}
  .sub {{ color: var(--muted); font-size: 13px; }}
  .sub code {{ color: var(--fg); }}
  .grid {{
    display: grid; gap: 16px;
    grid-template-columns: repeat(2, 1fr);
    margin-bottom: 24px;
  }}
  @media (max-width: 720px) {{
    .grid {{ grid-template-columns: 1fr; }}
    body {{ padding: 16px; }}
  }}
  .card {{
    background: var(--card);
    border: 1px solid var(--border);
    border-radius: 6px; padding: 16px;
  }}
  .kpi-label {{
    color: var(--muted); font-size: 11px;
    text-transform: uppercase; letter-spacing: 0.5px;
    margin-bottom: 8px;
  }}
  .kpi-value {{
    font-size: 28px; font-weight: 600; color: var(--fg);
  }}
  .kpi-sub {{
    color: var(--muted); font-size: 12px; margin-top: 4px;
  }}
  .val-ok {{ color: var(--ok); }}
  .val-mid {{ color: var(--mid); }}
  .val-warn {{ color: var(--warn); }}
  .val-crit {{ color: var(--crit); }}
  .chart-card {{ padding: 12px; }}
  .chart-title {{
    font-size: 13px; color: var(--fg);
    margin-bottom: 8px; font-weight: 500;
  }}
  .chart-box {{ position: relative; height: 240px; }}
  footer {{
    color: var(--muted); font-size: 11px;
    margin-top: 32px; padding-top: 16px;
    border-top: 1px solid var(--border);
  }}
  footer a {{ color: var(--muted); }}
</style>
</head>
<body>
<header>
  <h1>monad-ops replay · <code>{node_name}</code></h1>
  <div class=sub>
    window: <code>{from_iso}</code> → <code>{to_iso}</code>
    · static replay artifact, no live data
  </div>
</header>

<section class=grid>
  <div class=card>
    <div class=kpi-label>validator timeouts % (chain-wide)</div>
    <div class=kpi-value id=k-vtp>—</div>
    <div class=kpi-sub id=k-vtp-sub>Foundation target &lt;3%</div>
  </div>
  <div class=card>
    <div class=kpi-label>base fee · gwei (avg)</div>
    <div class=kpi-value id=k-basefee>—</div>
    <div class=kpi-sub id=k-basefee-sub>min — / max —</div>
  </div>
  <div class=card>
    <div class=kpi-label>blocks observed</div>
    <div class=kpi-value id=k-blocks>—</div>
    <div class=kpi-sub id=k-blocks-sub>peak tps —</div>
  </div>
  <div class=card>
    <div class=kpi-label>local pacemaker fires (window)</div>
    <div class=kpi-value id=k-lt>—</div>
    <div class=kpi-sub id=k-lt-sub>—</div>
  </div>
</section>

<section class=grid>
  <div class="card chart-card">
    <div class=chart-title>validator timeouts %, per minute</div>
    <div class=chart-box>
      <canvas id=chart-vtp role=img
        aria-label="validator timeout percentage per minute, color-zoned green to red"></canvas>
    </div>
  </div>
  <div class="card chart-card">
    <div class=chart-title>base fee · gwei, per block (downsampled)</div>
    <div class=chart-box>
      <canvas id=chart-basefee role=img
        aria-label="base fee in gwei per block, downsampled across the window"></canvas>
    </div>
  </div>
</section>

<footer>
  Static replay artifact generated by <a href="https://github.com/rustemar/monad-ops">monad-ops</a>.
  Data is frozen at export time; the live dashboard at
  <a href="https://ops.rustemar.dev">ops.rustemar.dev</a> reflects current state.
</footer>

<script>{chart_js}</script>
<script>
const REPLAY = {data_json};

(function () {{
  const $ = id => document.getElementById(id);
  const fmtPct = v => v == null ? "—" : `${{v.toFixed(2)}}%`;
  const fmtInt = n => n == null ? "—" : n.toLocaleString("en-US");
  const fmtCompact = v => {{
    if (v == null) return "—";
    const abs = Math.abs(v);
    if (abs >= 1e9) return (v / 1e9).toFixed(1) + "B";
    if (abs >= 1e6) return (v / 1e6).toFixed(1) + "M";
    if (abs >= 1e3) return (v / 1e3).toFixed(1) + "K";
    return v.toFixed(0);
  }};
  const colorAt = v =>
    v == null ? "#7a7f86"
    : v < 1 ? "#6bcf76"
    : v < 3 ? "#5b9cf5"
    : v < 5 ? "#ffb454"
    : "#ff6b6b";

  // KPI tiles
  const c = REPLAY.consensus || {{}};
  const a = REPLAY.aggregate || {{}};
  const bf = REPLAY.base_fee || {{}};
  const vtp = c.validator_timeout_pct;
  $("k-vtp").textContent = fmtPct(vtp);
  $("k-vtp").className = "kpi-value " +
    (vtp == null ? "" : (vtp < 1 ? "val-ok" : vtp < 3 ? "val-mid" : vtp < 5 ? "val-warn" : "val-crit"));
  $("k-vtp-sub").textContent =
    `Foundation target <3% · ${{fmtInt(c.rounds_total)}} rounds, ${{fmtInt(c.rounds_tc)}} closed by TC`;

  $("k-basefee").textContent = bf.base_fee_gwei_avg != null
    ? `${{bf.base_fee_gwei_avg.toFixed(1)}} gwei` : "—";
  $("k-basefee-sub").textContent =
    `min ${{bf.base_fee_gwei_min ?? "—"}} · max ${{bf.base_fee_gwei_max ?? "—"}} · ${{fmtInt(bf.samples)}} blocks sampled`;

  $("k-blocks").textContent = fmtInt(a.blocks);
  $("k-blocks-sub").textContent =
    `peak tps ${{fmtCompact(a.peak_tps)}} · total tx ${{fmtCompact(a.total_tx)}} · avg rtp ${{fmtPct(a.avg_rtp)}}`;

  $("k-lt").textContent = fmtInt(c.local_timeouts);
  $("k-lt-sub").textContent =
    `${{c.local_timeout_per_min ?? "—"}} per minute average · operator-side complement`;

  // VTP chart
  const bft = (REPLAY.series && REPLAY.series.bft) || [];
  const labelOf = ms => {{
    const d = new Date(ms);
    const hh = String(d.getUTCHours()).padStart(2, "0");
    const mm = String(d.getUTCMinutes()).padStart(2, "0");
    return `${{hh}}:${{mm}}Z`;
  }};
  if (bft.length > 0) {{
    new Chart($("chart-vtp"), {{
      type: "line",
      data: {{
        labels: bft.map(b => labelOf(b.t)),
        datasets: [{{
          data: bft.map(b => b.timeout_pct ?? 0),
          borderColor: "rgba(150,180,210,0.9)",
          segment: {{ borderColor: ctx => colorAt(ctx.p1.parsed.y) }},
          fill: false, borderWidth: 1.75, tension: 0.2, pointRadius: 0,
        }}],
      }},
      options: {{
        responsive: true, maintainAspectRatio: false,
        animation: false, plugins: {{ legend: {{ display: false }} }},
        scales: {{
          y: {{ min: 0, suggestedMax: 5,
                ticks: {{ callback: v => v + "%" , color: "#8b8f99" }},
                grid: {{ color: "#1f2530" }} }},
          x: {{ ticks: {{ color: "#8b8f99", maxTicksLimit: 8 }},
                grid: {{ color: "#1f2530" }} }},
        }},
      }},
    }});
  }}

  // base_fee chart
  const bfs = (REPLAY.series && REPLAY.series.base_fee) || [];
  if (bfs.length > 0) {{
    new Chart($("chart-basefee"), {{
      type: "line",
      data: {{
        labels: bfs.map(b => labelOf(b.t)),
        datasets: [{{
          data: bfs.map(b => b.base_fee_gwei_avg ?? 0),
          borderColor: "rgba(150,180,210,0.9)",
          fill: false, borderWidth: 1.5, tension: 0.2, pointRadius: 0,
        }}],
      }},
      options: {{
        responsive: true, maintainAspectRatio: false,
        animation: false, plugins: {{ legend: {{ display: false }} }},
        scales: {{
          y: {{ suggestedMin: 0,
                ticks: {{ callback: v => v + " gwei", color: "#8b8f99" }},
                grid: {{ color: "#1f2530" }} }},
          x: {{ ticks: {{ color: "#8b8f99", maxTicksLimit: 8 }},
                grid: {{ color: "#1f2530" }} }},
        }},
      }},
    }});
  }}
}})();
</script>
</body>
</html>
"""
