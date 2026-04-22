// Dashboard frontend. Single file. Plain fetch + Chart.js.
// Poll /api/state every 2s, /api/blocks on first load and every 10s.

// Refresh cadences. Doubled during the 2026-04-20 stress round so
// operators see state change at near-real-time. Safe against viewer
// volume because:
//   * /api/state is sub-ms (deque snapshot in memory).
//   * /api/blocks/sampled is bounded to 300 bins regardless of window,
//     uses idx_blocks_ts, well under 100ms even at 24h.
//   * /api/contracts/top_retried is the only heavy one; server- and
//     client-side 15s TTL caches collapse N viewers to 1 SQL per
//     15s per unique filter.
//   * /api/probes/public stays at 15s — host probes don't change faster
//     than that.
const STATE_INTERVAL = 1000;
const BLOCKS_INTERVAL = 5000;
const CONTRACTS_INTERVAL = 15000;
const INCIDENTS_INTERVAL = 5000;
const PROBES_INTERVAL = 15000;

// Inflight-dedup per endpoint: when setInterval fires while a previous
// request is still in flight (slow network, backgrounded tab releasing
// a queue of ticks on focus, rate-limiter delay), we cancel the older
// request. Root cause of the startup request-burst that produced HTTP
// 429 series in the 2026-04-20 audit — browsers batch-fire queued
// interval ticks on tab refocus and without this guard each tick
// spawns a new concurrent fetch. Key per endpoint so polls don't
// interfere across channels.
const _inflightAborts = new Map();
async function pollFetch(key, url, init = {}) {
    const prev = _inflightAborts.get(key);
    if (prev) prev.abort();
    const ctl = new AbortController();
    _inflightAborts.set(key, ctl);
    try {
        return await fetch(url, { ...init, signal: ctl.signal });
    } finally {
        if (_inflightAborts.get(key) === ctl) {
            _inflightAborts.delete(key);
        }
    }
}

let rtpChart = null;
let txChart = null;
let execChart = null;

// Formatter for large integer TPS / gas values. Compact 48_780 → "48.8K".
function fmtCompact(n) {
    if (n == null) return "—";
    const v = Number(n);
    // B/M/K thresholds. 2 decimals of precision — "3.52B" carries
    // ~3 significant figures (the earlier 1-decimal form left "3.5B"
    // collapsing 3.50–3.59 into one readout, which lost too much
    // detail for KPIs the user actually reads numerically).
    if (v >= 1_000_000_000) return (v / 1_000_000_000).toFixed(2).replace(/\.?0+$/, "") + "B";
    if (v >= 1_000_000)     return (v / 1_000_000).toFixed(2).replace(/\.?0+$/, "") + "M";
    if (v >= 1_000)         return (v / 1_000).toFixed(2).replace(/\.?0+$/, "") + "K";
    return String(v);
}

// Shared event hook for both crosshair variants — records the x of the
// latest pointer event so the paint phase (afterDraw) knows where to
// render the cross line. Kept on the chart instance to avoid a global.
function _crosshairAfterEvent(chart, args) {
    const evt = args.event;
    if (evt.type === "mousemove" || evt.type === "touchstart" || evt.type === "touchmove") {
        chart._crosshairX = evt.x;
    } else if (evt.type === "mouseout" || evt.type === "touchend") {
        chart._crosshairX = null;
    }
}

// ---- crosshair plugin: vertical line + value label on hover/touch ----
const crosshairPlugin = {
    id: "crosshair",
    afterEvent: _crosshairAfterEvent,
    afterDraw(chart) {
        const x = chart._crosshairX;
        if (x == null) return;
        const { ctx, chartArea: { top, bottom, left, right } } = chart;
        if (x < left || x > right) { chart._crosshairX = null; return; }

        // vertical line
        ctx.save();
        ctx.beginPath();
        ctx.moveTo(x, top);
        ctx.lineTo(x, bottom);
        ctx.lineWidth = 1;
        ctx.strokeStyle = "rgba(215,218,224,0.35)";
        ctx.setLineDash([4, 3]);
        ctx.stroke();
        ctx.restore();

        // find nearest data point
        const ds = chart.data.datasets[0];
        if (!ds || !ds.data.length) return;
        const meta = chart.getDatasetMeta(0);
        let nearest = 0, minDist = Infinity;
        for (let i = 0; i < meta.data.length; i++) {
            const d = Math.abs(meta.data[i].x - x);
            if (d < minDist) { minDist = d; nearest = i; }
        }
        const val = ds.data[nearest];
        // Optional secondary dataset — used by the rtp envelope (dataset
        // [1] = rtp_max). null when the chart has only one series.
        const ds1 = chart.data.datasets[1];
        const val2 = (ds1 && ds1.data && ds1.data.length) ? ds1.data[nearest] : null;
        const blockNum = (chart._binBlocks && chart._binBlocks[nearest]) || null;
        const binTs = (chart._binTimes && chart._binTimes[nearest]) || null;
        const ptX = meta.data[nearest].x;
        const ptY = meta.data[nearest].y;

        // dot at data point
        ctx.save();
        ctx.beginPath();
        ctx.arc(ptX, ptY, 4, 0, Math.PI * 2);
        ctx.fillStyle = "#5b9cf5";
        ctx.fill();
        ctx.restore();

        // value label
        const isPercent = chart.config.type === "line";
        // Round to 1 decimal (percentages) or a thousands-grouped integer
        // otherwise — raw floats like "1604.3333333" were unreadable.
        let text;
        if (isPercent) {
            // When the chart carries a max-envelope dataset, surface both
            // numbers so viewers don't have to read it off the faint line.
            text = val2 != null
                ? `avg ${Number(val).toFixed(1)}% · p95 ${Number(val2).toFixed(1)}%`
                : `${Number(val).toFixed(1)}%`;
        } else if (val >= 100) {
            text = fmtInt(Math.round(Number(val)));
        } else {
            text = Number(val).toFixed(1);
        }
        const blockText = blockNum != null ? `#${fmtInt(blockNum)}` : "";
        const timeText = binTs != null ? `${_fmtBinClock(binTs)} ${_tzShort}` : "";
        ctx.save();
        // Measure at the fonts we'll actually render with — mixing
        // fonts in measureText vs fillText was under-sizing the box.
        ctx.font = "600 11px 'JetBrains Mono', monospace";
        const valW = ctx.measureText(text).width;
        ctx.font = "10px 'JetBrains Mono', monospace";
        const blockW = ctx.measureText(blockText).width;
        const timeW  = ctx.measureText(timeText).width;
        const tw = Math.max(valW, blockW, timeW) + 16;
        const th = timeText ? 48 : 34;
        let lx = ptX - tw / 2;
        if (lx < left) lx = left;
        if (lx + tw > right) lx = right - tw;
        // Draw INSIDE the chart area, anchored near the top edge. The
        // previous layout placed the box above `top`, which slid under
        // the card header at smaller heights. Anchoring inside means the
        // tooltip is always fully visible, at the cost of a few pixels
        // of chart area — addressed by the chart's layout.padding.top.
        const boxTop = top + 4;
        ctx.fillStyle = "rgba(23,27,36,0.92)";
        ctx.beginPath();
        ctx.roundRect(lx, boxTop, tw, th, 4);
        ctx.fill();
        ctx.strokeStyle = "rgba(91,156,245,0.4)";
        ctx.lineWidth = 1;
        ctx.stroke();
        // Rows top-to-bottom: time, block, value (value biggest, at bottom
        // so the eye lands on the number).
        ctx.textAlign = "center";
        ctx.textBaseline = "top";
        let row = boxTop + 4;
        if (timeText) {
            ctx.font = "10px 'JetBrains Mono', monospace";
            ctx.fillStyle = "#8a8f99";
            ctx.fillText(timeText, lx + tw / 2, row);
            row += 13;
        }
        if (blockText) {
            ctx.font = "10px 'JetBrains Mono', monospace";
            ctx.fillStyle = "#8a8f99";
            ctx.fillText(blockText, lx + tw / 2, row);
            row += 13;
        }
        ctx.font = "600 12px 'JetBrains Mono', monospace";
        ctx.fillStyle = "#d7dae0";
        ctx.fillText(text, lx + tw / 2, row);
        ctx.restore();
    },
};

// Threshold-guides plugin for the retry_pct chart. The line itself is
// colored per-segment by value (green <65, amber 65–75, red ≥75 — see
// rtpColorForValue / drawRtp's segment.borderColor), so the eye reads
// severity directly off the line. This plugin only draws thin dashed
// reference lines at the rule thresholds so viewers can anchor against
// 65/75 without a busy zone fill competing with the line.
// Thresholds are kept in lockstep with rules/retry_spike.py — raising
// WARN from 50 → 65 on 2026-04-21 (baseline noise pinned avg near 50).
const RTP_WARN = 65;
const RTP_CRIT = 75;
const RTP_GREEN = "#6bcf76";
const RTP_AMBER = "#ffb454";
const RTP_RED   = "#ff6b6b";
function rtpColorForValue(v) {
    if (v == null || Number.isNaN(v)) return RTP_GREEN;
    if (v >= RTP_CRIT) return RTP_RED;
    if (v >= RTP_WARN) return RTP_AMBER;
    return RTP_GREEN;
}
const rtpZonesPlugin = {
    id: "rtpZones",
    beforeDatasetsDraw(chart) {
        const { ctx, chartArea: { top, bottom, left, right },
                scales: { y } } = chart;
        if (!y) return;
        const clamp = v => Math.max(top, Math.min(bottom, v));
        ctx.save();
        ctx.lineWidth = 1;
        ctx.setLineDash([3, 3]);
        // Threshold-tinted dashed guides: amber at 50, red at 75. Low
        // opacity so they sit behind the line without stealing focus.
        const guides = [
            [RTP_WARN, "rgba(255,180,84,0.30)"],
            [RTP_CRIT, "rgba(255,107,107,0.30)"],
        ];
        for (const [v, color] of guides) {
            const py = clamp(y.getPixelForValue(v));
            ctx.strokeStyle = color;
            ctx.beginPath();
            ctx.moveTo(left, py);
            ctx.lineTo(right, py);
            ctx.stroke();
        }
        ctx.restore();
    },
    // Draw the line three times, each time clipped to one zone's
    // y-band. The dataset's own borderColor is transparent (see
    // drawRtp), so we fully own the visual line here. Clipping on the
    // rendered line — rather than coloring Chart.js segments by max
    // endpoint — guarantees the color boundary lands exactly at 50/75
    // regardless of where the sample points sit.
    afterDatasetsDraw(chart) {
        const meta = chart.getDatasetMeta(0);
        if (!meta || !meta.data || !meta.data.length) return;
        const { ctx, chartArea: { top, bottom, left, right },
                scales: { y } } = chart;
        if (!y) return;
        const pts = meta.data.filter(
            p => p && Number.isFinite(p.x) && Number.isFinite(p.y));
        if (pts.length < 2) return;
        const clamp = v => Math.max(top, Math.min(bottom, v));
        const drawBand = (vLo, vHi, color) => {
            const yTop = clamp(y.getPixelForValue(vHi));
            const yBot = clamp(y.getPixelForValue(vLo));
            ctx.save();
            ctx.beginPath();
            ctx.rect(left, yTop, right - left, yBot - yTop);
            ctx.clip();
            ctx.beginPath();
            ctx.moveTo(pts[0].x, pts[0].y);
            for (let i = 1; i < pts.length; i++) {
                ctx.lineTo(pts[i].x, pts[i].y);
            }
            ctx.lineWidth = 1.75;
            ctx.strokeStyle = color;
            ctx.lineJoin = "round";
            ctx.stroke();
            ctx.restore();
        };
        drawBand(0,         RTP_WARN, RTP_GREEN);
        drawBand(RTP_WARN,  RTP_CRIT, RTP_AMBER);
        drawBand(RTP_CRIT,  100,      RTP_RED);
    },
};

// Short timezone abbreviation for the user's local tz (e.g. "MSK", "UTC+3").
const _tzShort = (() => {
    const s = new Intl.DateTimeFormat(undefined, { timeZoneName: "short" })
        .formatToParts(new Date()).find(p => p.type === "timeZoneName");
    return s ? s.value : "local";
})();

// HH:MM:SS local tz — used in both tooltips and the x-axis ticks so
// they agree to the second.
function _fmtBinClock(ms) {
    const d = new Date(ms);
    const p = (n) => String(n).padStart(2, "0");
    return `${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
}

// ---- stack crosshair plugin: per-phase tooltip for the stacked
// execution-breakdown chart. Shows all dataset values + total at the
// nearest x-index, formatted in µs/ms, with a vertical guide line.
const stackCrosshairPlugin = {
    id: "stackCrosshair",
    afterEvent: _crosshairAfterEvent,
    afterDraw(chart) {
        const x = chart._crosshairX;
        if (x == null) return;
        const { ctx, chartArea: { top, bottom, left, right } } = chart;
        if (x < left || x > right) { chart._crosshairX = null; return; }

        // vertical guide line
        ctx.save();
        ctx.beginPath();
        ctx.moveTo(x, top);
        ctx.lineTo(x, bottom);
        ctx.lineWidth = 1;
        ctx.strokeStyle = "rgba(215,218,224,0.35)";
        ctx.setLineDash([4, 3]);
        ctx.stroke();
        ctx.restore();

        // nearest index from the first dataset's bar positions
        const meta0 = chart.getDatasetMeta(0);
        if (!meta0 || !meta0.data.length) return;
        let nearest = 0, minDist = Infinity;
        for (let i = 0; i < meta0.data.length; i++) {
            const d = Math.abs(meta0.data[i].x - x);
            if (d < minDist) { minDist = d; nearest = i; }
        }
        const blockNum = (chart._binBlocks && chart._binBlocks[nearest]) || chart.data.labels[nearest];
        const binTs = (chart._binTimes && chart._binTimes[nearest]) || null;
        const datasets = chart.data.datasets;
        const vals = datasets.map(ds => Number(ds.data[nearest]) || 0);
        const names = datasets.map(ds => ds.label);
        const colors = datasets.map(ds => ds.backgroundColor);
        const total = vals.reduce((a, b) => a + b, 0);

        // Header text built up-front so its width can shape the box.
        const headerTxt = binTs
            ? `block #${fmtInt(blockNum)} · ${_fmtBinClock(binTs)} ${_tzShort}`
            : `block #${fmtInt(blockNum)}`;
        // Tooltip layout. Rows: 1 header + N datasets + 1 separator + 1 total.
        const rows = names.length + 2;
        const padX = 10, padY = 8;
        const lineH = 14;
        const hdrH = 16;
        // Measure widest label+value pair at the content font.
        ctx.font = "600 11px 'JetBrains Mono', monospace";
        const fmt = (us) => us >= 1000 ? `${(us/1000).toFixed(2)} ms` : `${Math.round(us)} µs`;
        const labelWidth = Math.max(...names.map(n => ctx.measureText(n).width));
        const valueWidth = Math.max(
            ...vals.map(v => ctx.measureText(fmt(v)).width),
            ctx.measureText(fmt(total)).width,
        );
        const swatch = 8;  // color square width
        const swatchGap = 6;
        const labelGap = 14;
        const contentW = swatch + swatchGap + labelWidth + labelGap + valueWidth;
        // Header is rendered at the header font size, so measure it there.
        ctx.font = "10px 'JetBrains Mono', monospace";
        const headerW = ctx.measureText(headerTxt).width;
        const w = Math.max(contentW, headerW) + padX * 2;
        const h = hdrH + lineH * (names.length + 1) + 6 /*sep*/ + padY * 2 - 4;

        // Anchor near the cursor; flip to the left side if it would
        // overflow the chart area on the right.
        const meta = meta0.data[nearest];
        let tx = meta.x + 12;
        if (tx + w > right) tx = meta.x - 12 - w;
        if (tx < left) tx = left + 4;
        let ty = top + 6;

        // Background box
        ctx.save();
        ctx.fillStyle = "rgba(15,17,21,0.94)";
        ctx.strokeStyle = "rgba(91,156,245,0.45)";
        ctx.lineWidth = 1;
        ctx.beginPath();
        ctx.roundRect(tx, ty, w, h, 5);
        ctx.fill();
        ctx.stroke();

        // Header — block number + bin time (font already set above).
        ctx.fillStyle = "#8a8f99";
        ctx.textAlign = "left";
        ctx.textBaseline = "top";
        ctx.fillText(headerTxt, tx + padX, ty + padY);

        // Rows: swatch, label, value
        ctx.font = "600 11px 'JetBrains Mono', monospace";
        let yCursor = ty + padY + hdrH;
        for (let i = 0; i < names.length; i++) {
            // swatch
            ctx.fillStyle = colors[i];
            ctx.fillRect(tx + padX, yCursor + 2, swatch, swatch);
            // label
            ctx.fillStyle = "#d7dae0";
            ctx.fillText(names[i], tx + padX + swatch + swatchGap, yCursor);
            // value (right-aligned inside the box)
            ctx.textAlign = "right";
            ctx.fillText(fmt(vals[i]), tx + w - padX, yCursor);
            ctx.textAlign = "left";
            yCursor += lineH;
        }

        // Separator + total
        ctx.strokeStyle = "rgba(138,143,153,0.25)";
        ctx.beginPath();
        ctx.moveTo(tx + padX, yCursor + 2);
        ctx.lineTo(tx + w - padX, yCursor + 2);
        ctx.stroke();
        yCursor += 6;
        ctx.fillStyle = "#d7dae0";
        ctx.fillText("total", tx + padX + swatch + swatchGap, yCursor);
        ctx.textAlign = "right";
        ctx.fillStyle = "#5b9cf5";
        ctx.fillText(fmt(total), tx + w - padX, yCursor);
        ctx.restore();
    },
};

function fmtInt(n) {
    if (n === null || n === undefined) return "—";
    return Number(n).toLocaleString("en-US");
}
function fmtPct(p) {
    if (p === null || p === undefined) return "—";
    return `${Number(p).toFixed(1)}%`;
}
function fmtUptime(sec) {
    if (!sec && sec !== 0) return "uptime —";
    const d = Math.floor(sec / 86400);
    const h = Math.floor((sec % 86400) / 3600);
    const m = Math.floor((sec % 3600) / 60);
    const s = sec % 60;
    if (d) return `uptime ${d}d ${h}h`;
    if (h) return `uptime ${h}h ${m}m`;
    if (m) return `uptime ${m}m ${s}s`;
    return `uptime ${s}s`;
}
function fmtSince(ms) {
    if (!ms) return "—";
    const diffSec = Math.round((Date.now() - ms) / 1000);
    if (diffSec < 2) return "just now";
    if (diffSec < 60) return `${diffSec}s ago`;
    const m = Math.floor(diffSec / 60);
    if (m < 60) return `${m}m ${diffSec % 60}s ago`;
    return `${Math.floor(m / 60)}h ${m % 60}m ago`;
}

// ---- severity classifiers ------------------------------------------
// Thresholds are calibrated from this node's own observed history
// (243k blocks, ~27h): retry_pct p50=0 · p90=50 · p95=60 · p99=75 · max=92;
// block time p50=402ms · p95=552ms · p99=636ms. The legend under the KPI
// row communicates these thresholds to the viewer.
//
// "warn" and "crit" boundaries are kept aligned with the retry_spike
// alert rule AND the chart's 3-zone coloring: 65% = rule WARN (chart
// amber line), 75% = rule CRITICAL (chart red line). The "ok"/"mid"
// split at 25 is a finer visual gradient within the chart's green zone.
// Updated on 2026-04-21 together with the rule & chart — old warn=50
// was at median retry_pct and flagged normal operation as elevated.
function classifyRtp(v) {
    if (v == null) return "";
    if (v < 25) return "val-ok";
    if (v < 65) return "val-mid";
    if (v < 75) return "val-warn";
    return "val-crit";
}
function classifyBps(v) {
    if (v == null) return "";
    if (v >= 2.0) return "val-ok";
    if (v >= 1.5) return "val-mid";
    if (v >= 0.5) return "val-warn";
    return "val-crit";
}
function classifyBlockAgeMs(ms) {
    if (!ms) return "";
    const diffSec = (Date.now() - ms) / 1000;
    if (diffSec < 3) return "val-ok";
    if (diffSec < 10) return "val-warn";
    return "val-crit";
}
// Lag severity: positive delta means reference is ahead (we're behind).
// A few blocks of jitter is normal (block time ≈ 400ms + network hop).
// Thresholds are chosen for a 15s polling cadence — tight enough to
// catch transient dips but not so tight that ordinary jitter trips a
// warning on every refresh.
function classifyLag(delta) {
    if (delta == null) return "val-unknown";
    const abs = Math.abs(delta);
    if (abs <= 2)  return "val-ok";
    if (abs <= 10) return "val-mid";
    if (abs <= 50) return "val-warn";
    return "val-crit";
}
// Maps a severity class to the inline fill color used in the
// retried-ratio progress bars. Inline so bars stay colored even if a
// stale browser cache serves an old app.css.
const SEV_COLOR = {
    "val-ok":   "#6bcf76",
    "val-mid":  "#5b9cf5",
    "val-warn": "#ffb454",
    "val-crit": "#ff6b6b",
};
function setKpi(id, text, sevClass) {
    const el = document.getElementById(id);
    if (!el) return;
    el.textContent = text;
    el.classList.remove("val-ok", "val-mid", "val-warn", "val-crit");
    if (sevClass) el.classList.add(sevClass);
}

// Health-pill click handler: operators instinctively tap the status
// pill in the header when they see CRITICAL, expecting to learn what
// failed. On the dashboard, the incidents card is visible on the same
// page — intercept the anchor navigation and smooth-scroll there.
// The `href="/alerts?severity=critical"` remains as a fallback: on any
// page without #incidents-card (e.g. if the pill is ever reused on a
// different template), keyboard / right-click / ctrl-click all still
// do the sensible thing. Wired once on DOMContentLoaded.
(function wireHealthPill() {
    const pill = document.getElementById("health-pill");
    if (!pill) return;
    pill.addEventListener("click", (e) => {
        const target = document.getElementById("incidents-card");
        if (!target) return; // no card on this page — let href navigate
        // Honor common "open-in-new-tab" intents
        if (e.metaKey || e.ctrlKey || e.shiftKey || e.button === 1) return;
        e.preventDefault();
        target.scrollIntoView({ behavior: "smooth", block: "start" });
    });
})();

function updateHealth(data) {
    const pill = document.getElementById("health-pill");
    pill.classList.remove("ok", "warn", "crit");
    // current_alerts is an append-only buffer; a rule can have e.g. warn
    // then recovered. Per-rule latest state wins — otherwise an old warn
    // keeps pinning worst at 2 forever after the rule has recovered.
    const order = {recovered: 0, info: 1, warn: 2, critical: 3};
    const latestByRule = new Map();
    for (const a of (data.current_alerts || [])) {
        const key = a.key || a.rule || "_";
        latestByRule.set(key, a);
    }
    let worst = 0;
    for (const a of latestByRule.values()) {
        worst = Math.max(worst, order[a.severity] ?? 0);
    }
    const labelEl = pill.querySelector(".label");
    if (worst >= 3) { pill.classList.add("crit"); labelEl.textContent = "critical"; }
    else if (worst >= 2) { pill.classList.add("warn"); labelEl.textContent = "warning"; }
    else { pill.classList.add("ok"); labelEl.textContent = "healthy"; }
}

let _dataStartMs = null;
async function fetchState() {
    try {
        const r = await pollFetch("state", "/api/state");
        if (!r.ok) throw new Error(r.statusText);
        const d = await r.json();
        if (d.data_start_ms) _dataStartMs = d.data_start_ms;
        document.getElementById("node-name").textContent = d.node_name;
        document.getElementById("k-last-block").textContent = fmtInt(d.last_block);
        const sub = document.getElementById("k-last-block-sub");
        sub.textContent = d.last_block_seen_ms
            ? `seen ${fmtSince(d.last_block_seen_ms)}`
            : "last seen —";
        sub.classList.remove("val-ok", "val-warn", "val-crit");
        const ageCls = classifyBlockAgeMs(d.last_block_seen_ms);
        if (ageCls) sub.classList.add(ageCls);

        setKpi("k-bps",
               d.blocks_per_sec_1m?.toFixed(2) ?? "—",
               classifyBps(d.blocks_per_sec_1m));
        document.getElementById("k-tx").textContent = `${(d.tx_per_sec_1m ?? 0).toFixed(1)} tx/s`;
        setKpi("k-rtp-1m",  fmtPct(d.rtp_avg_1m),  classifyRtp(d.rtp_avg_1m));
        setKpi("k-rtp-peak", fmtPct(d.rtp_max_1m), classifyRtp(d.rtp_max_1m));
        document.getElementById("k-rtp-5m").textContent = `5m avg ${fmtPct(d.rtp_avg_5m)}`;

        // Parallelism KPIs — intrablock effective peaks. We show the
        // compact form at glance size + the exact thousands-grouped
        // value in the sub-line so the user gets both "rough shape"
        // and "how much exactly" without ever having to hover. Compact
        // alone ("37K", "3.5B") was losing too much precision for KPIs
        // people actually read as numbers.
        const tpsPeak = d.tps_effective_peak_1m;
        const tpsAvg  = d.tps_effective_avg_1m;
        const gasPeak = d.gas_per_sec_effective_peak_1m;
        document.getElementById("k-tps-eff-peak").textContent = fmtCompact(tpsPeak);
        document.getElementById("k-tps-eff-avg").textContent =
            tpsPeak != null
                ? `${fmtInt(tpsPeak)} tx/s · avg ${fmtInt(tpsAvg)} tx/s per block`
                : `avg ${fmtCompact(tpsAvg)} tx/s per block`;
        document.getElementById("k-gas-eff-peak").textContent = fmtCompact(gasPeak);
        const gasSubEl = document.getElementById("k-gas-eff-peak-sub");
        if (gasSubEl) {
            gasSubEl.textContent = gasPeak != null
                ? `${fmtInt(gasPeak)} gas/sec · peak inside a single block`
                : "peak gas/sec inside a single block";
        }

        // Chain integrity panel — reorg counter + last-reorg details.
        updateIntegrity(d);
        // Epoch progress card.
        updateEpoch(d.epoch || null);

        // Reference-RPC lag — distinguishes local-stall from network-halt.
        updateLag(d);

        document.getElementById("uptime").textContent = fmtUptime(d.uptime_sec);
        document.getElementById("seen").textContent = `${fmtInt(d.blocks_seen)} blocks seen`;
        document.getElementById("ts").textContent = _fmtLocal(Date.now()) + " " + _tzShort;

        updateHealth(d);
        renderAlerts(d.current_alerts || []);
    } catch (e) {
        // AbortError fires when a newer poll superseded this one; not
        // a disconnect — the in-flight request was cancelled by design.
        if (e && e.name === "AbortError") return;
        const pill = document.getElementById("health-pill");
        pill.classList.remove("ok", "warn", "crit");
        pill.querySelector(".label").textContent = "disconnected";
    }
}

function renderAlerts(alerts) {
    const list = document.getElementById("alerts-list");
    document.getElementById("alerts-count").textContent = `${alerts.length}`;
    if (!alerts.length) {
        list.innerHTML = '<li class="empty">no alerts in buffer</li>';
        return;
    }
    list.innerHTML = alerts.slice().reverse().map(a => {
        const sev = a.severity || "";
        const sevClass = /^[a-z_-]+$/i.test(sev) ? sev : "";
        // ts_ms populated for every alert served from state (live + bootstrap).
        // Fall back to empty if a legacy cached payload is missing it.
        const timeAttr = a.ts_ms ? new Date(a.ts_ms).toISOString() : "";
        const timeStr = a.ts_ms ? fmtAlertTime(a.ts_ms) : "";
        const fullTs = a.ts_ms ? fmtFullTs(a.ts_ms) : "";
        return `
        <li>
            <time class="alert-ts" datetime="${timeAttr}" title="${escapeHTML(fullTs)}">${escapeHTML(timeStr)}</time>
            <span class="sev ${sevClass}">${escapeHTML(sev)}</span>
            <span class="rule">${escapeHTML(a.rule || "")}</span>
            <span class="detail">${escapeHTML(a.title || "")}${a.detail ? " — " + escapeHTML(a.detail) : ""}</span>
        </li>`;
    }).join("");
}

// Format ms epoch as "YYYY-MM-DD HH:MM:SS" in local tz.
function _fmtLocal(ms) {
    const d = new Date(ms);
    const p = (n, w = 2) => String(n).padStart(w, "0");
    return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())} `
         + `${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
}

// Relative time for alerts that landed recently ("2m ago"), absolute
// local tz for anything older.
function fmtAlertTime(tsMs) {
    const diff = Date.now() - tsMs;
    if (diff < 0) return _fmtLocal(tsMs).slice(5, 16);
    if (diff < 60_000) return "just now";
    if (diff < 3_600_000) return `${Math.floor(diff / 60_000)}m ago`;
    if (diff < 86_400_000) return `${Math.floor(diff / 3_600_000)}h ago`;
    // Older than a day — show MM-DD HH:MM local.
    return _fmtLocal(tsMs).slice(5, 16);
}

// Full precision down to the second, shown on hover/tap — UX pattern
// from Etherscan/Arbiscan ("3h ago" label with exact tooltip).
function fmtFullTs(tsMs) {
    return _fmtLocal(tsMs) + " " + _tzShort;
}

// Chart period state. Two modes:
//   preset — relative "last N seconds" from now (to_ts tracks wall-clock).
//   custom — explicit from/to absolute ms (frozen window, no live scroll).
// Persisted in localStorage and URL (?range=12h  OR  ?range=custom&from=…&to=…)
// so refresh/share preserves the view. Default 12h — wide enough to see
// cross-batch stress-test structure, tight enough for detail.
const CHART_CUSTOM_MAX_SPAN_MS = 7 * 86400 * 1000;
const CHART_DEFAULT_RANGE_SEC = 43200;

let _chartMode = "preset";
let _chartRangeSec = CHART_DEFAULT_RANGE_SEC;
let _customFromMs = null;
let _customToMs = null;
_readChartState();

function _readChartState() {
    const p = new URLSearchParams(window.location.search);
    const urlRange = p.get("range");
    if (urlRange === "custom_live") {
        const from = parseInt(p.get("from") || "", 10);
        if (Number.isFinite(from) && from > 0) {
            _chartMode = "custom_live";
            _customFromMs = from;
            _customToMs = null;
            return;
        }
    }
    if (urlRange === "custom") {
        const from = parseInt(p.get("from") || "", 10);
        const to = parseInt(p.get("to") || "", 10);
        if (Number.isFinite(from) && Number.isFinite(to) && to > from) {
            _chartMode = "custom";
            _customFromMs = from;
            _customToMs = to;
            return;
        }
    }
    const fromUrl = urlRange ? parseInt(urlRange, 10) : NaN;
    if (Number.isFinite(fromUrl) && fromUrl > 0) {
        _chartRangeSec = fromUrl;
        return;
    }
    const storedMode = localStorage.getItem("ops.chartMode");
    if (storedMode === "custom_live") {
        const from = parseInt(localStorage.getItem("ops.chartCustomFrom") || "", 10);
        if (Number.isFinite(from) && from > 0) {
            _chartMode = "custom_live";
            _customFromMs = from;
            _customToMs = null;
            return;
        }
    }
    if (storedMode === "custom") {
        const from = parseInt(localStorage.getItem("ops.chartCustomFrom") || "", 10);
        const to = parseInt(localStorage.getItem("ops.chartCustomTo") || "", 10);
        if (Number.isFinite(from) && Number.isFinite(to) && to > from) {
            _chartMode = "custom";
            _customFromMs = from;
            _customToMs = to;
            return;
        }
    }
    const stored = parseInt(localStorage.getItem("ops.chartRange") || "", 10);
    if (Number.isFinite(stored) && stored > 0) {
        _chartRangeSec = stored;
    }
}

function _persistChartState() {
    const url = new URL(window.location.href);
    try {
        if (_chartMode === "custom_live") {
            url.searchParams.set("range", "custom_live");
            url.searchParams.set("from", String(_customFromMs));
            url.searchParams.delete("to");
            localStorage.setItem("ops.chartMode", "custom_live");
            localStorage.setItem("ops.chartCustomFrom", String(_customFromMs));
            localStorage.removeItem("ops.chartCustomTo");
        } else if (_chartMode === "custom") {
            url.searchParams.set("range", "custom");
            url.searchParams.set("from", String(_customFromMs));
            url.searchParams.set("to", String(_customToMs));
            localStorage.setItem("ops.chartMode", "custom");
            localStorage.setItem("ops.chartCustomFrom", String(_customFromMs));
            localStorage.setItem("ops.chartCustomTo", String(_customToMs));
        } else {
            url.searchParams.set("range", String(_chartRangeSec));
            url.searchParams.delete("from");
            url.searchParams.delete("to");
            localStorage.setItem("ops.chartMode", "preset");
            localStorage.setItem("ops.chartRange", String(_chartRangeSec));
        }
    } catch {}
    window.history.replaceState(null, "", url);
}

function _setChartRange(sec) {
    _chartMode = "preset";
    _chartRangeSec = sec;
    _customFromMs = _customToMs = null;
    _persistChartState();
    _syncChartControlsUI();
    fetchBlocks();
}

function _setChartCustomRange(fromMs, toMs) {
    _chartMode = "custom";
    _customFromMs = fromMs;
    _customToMs = toMs;
    _persistChartState();
    _syncChartControlsUI();
    fetchBlocks();
}

function _setChartCustomLive(fromMs) {
    _chartMode = "custom_live";
    _customFromMs = fromMs;
    _customToMs = null;
    _persistChartState();
    _syncChartControlsUI();
    fetchBlocks();
}

function _syncChartControlsUI() {
    for (const btn of document.querySelectorAll(".charts-range .range-btn")) {
        if (btn.id === "custom-toggle") continue;
        btn.classList.toggle("is-active",
            _chartMode === "preset" && parseInt(btn.dataset.range, 10) === _chartRangeSec);
    }
    const customBtn = document.getElementById("custom-toggle");
    if (customBtn) customBtn.classList.toggle("is-active",
        _chartMode === "custom" || _chartMode === "custom_live");
}

function _fmtRangeLabel(sec) {
    if (sec < 3600) return `${Math.round(sec / 60)}m`;
    if (sec < 86400) return `${Math.round(sec / 3600)}h`;
    return `${Math.round(sec / 86400)}d`;
}
function _fmtBinLabel(binMs) {
    if (binMs < 1000) return `${binMs}ms`;
    if (binMs < 60_000) return `${Math.round(binMs / 1000)}s`;
    if (binMs < 3_600_000) return `${Math.round(binMs / 60_000)}m`;
    return `${(binMs / 3_600_000).toFixed(1)}h`;
}

// Client cache for the sampled-blocks payload. Keyed by mode+params.
// 2-second TTL mirrors the server-side minimum; gives near-instant
// toggling between range buttons (5m ↔ 1h ↔ 24h) without hitting the
// network, while still refreshing at the regular BLOCKS_INTERVAL. The
// server cache handles multi-viewer coalescing; this one handles the
// single-user flip-through experience.
const _chartsCache = new Map();
const CHARTS_CACHE_TTL_MS = 2_000;

function _chartWindow() {
    if (_chartMode === "custom" && _customFromMs != null && _customToMs != null) {
        return { fromMs: _customFromMs, toMs: _customToMs, key: `c:${_customFromMs}-${_customToMs}` };
    }
    if (_chartMode === "custom_live" && _customFromMs != null) {
        // "custom with trailing to=now": from is frozen, to tracks
        // wall-clock. Cache key is the fixed from — server-side
        // quantization of to_ts handles near-simultaneous viewers.
        const toMs = Date.now();
        return { fromMs: _customFromMs, toMs, key: `cl:${_customFromMs}` };
    }
    const toMs = Date.now();
    const fromMs = toMs - _chartRangeSec * 1000;
    // Preset cache key is just the range sec — to_ts tracks wall-clock,
    // so two calls 100ms apart share a cache entry but 3s apart don't
    // (CHARTS_CACHE_TTL_MS guards that).
    return { fromMs, toMs, key: `p:${_chartRangeSec}` };
}

async function fetchBlocks() {
    const { fromMs, toMs, key } = _chartWindow();
    const cached = _chartsCache.get(key);
    if (cached && (Date.now() - cached.at) < CHARTS_CACHE_TTL_MS) {
        _applyChartPayload(cached.payload);
        return;
    }
    try {
        const r = await pollFetch(
            "blocks-sampled",
            `/api/blocks/sampled?from_ts_ms=${fromMs}&to_ts_ms=${toMs}&points=300`);
        if (!r.ok) throw new Error(r.statusText);
        const d = await r.json();
        _chartsCache.set(key, { at: Date.now(), payload: d });
        _applyChartPayload(d);
    } catch (e) { /* swallow; next tick retries (AbortError too — already replaced) */ }
}

// Tiny SVG sparkline renderer (G5). Takes an array of numbers and
// renders a polyline + filled area into the target element.
function _renderSparkline(elId, values) {
    const el = document.getElementById(elId);
    if (!el || !values.length) return;
    const n = values.length;
    const min = Math.min(...values);
    const max = Math.max(...values);
    const range = max - min || 1;
    const w = 100, h = 20;
    const pts = values.map((v, i) => {
        const x = (i / Math.max(1, n - 1)) * w;
        const y = h - ((v - min) / range) * (h - 2) - 1;
        return `${x.toFixed(1)},${y.toFixed(1)}`;
    });
    const line = pts.join(" ");
    const fill = `0,${h} ${line} ${w},${h}`;
    el.innerHTML = `<svg viewBox="0 0 ${w} ${h}" preserveAspectRatio="none">`
        + `<polygon class="spark-fill" points="${fill}"/>`
        + `<polyline points="${line}"/></svg>`;
}

function _applyChartPayload(d) {
    const bins = d.bins || [];
    const binLabel = _fmtBinLabel(d.bin_ms || 0);
    let rangeLabel;
    const fmtShort = ms => _fmtLocal(ms).slice(5, 16);
    if (_chartMode === "custom") {
        rangeLabel = `${fmtShort(_customFromMs)} → ${fmtShort(_customToMs)}`;
    } else if (_chartMode === "custom_live") {
        rangeLabel = `${fmtShort(_customFromMs)} → now`;
    } else {
        rangeLabel = `last ${_fmtRangeLabel(_chartRangeSec)}`;
    }
    const hintText = bins.length > 0
        ? `${rangeLabel} · binned ${binLabel} · ${bins.length} points`
        : `${rangeLabel} · no data yet`;
    // Update each chart's subtitle with window + bin resolution so
    // the viewer always knows what they're looking at.
    const rtpHint = document.getElementById("chart-rtp-hint");
    const txHint = document.getElementById("chart-tx-hint");
    const execHint = document.getElementById("chart-exec-hint");
    const toolbarHint = document.getElementById("charts-range-hint");
    if (rtpHint) rtpHint.textContent = `avg rtp · p95 envelope · ${hintText}`;
    if (txHint) txHint.textContent = `avg tx/block · ${hintText}`;
    if (execHint) execHint.textContent = `time per execution step, in µs · ${hintText}`;
    if (toolbarHint) toolbarHint.textContent = hintText;

    // Auto-refresh presets and custom_live (from is frozen, to tracks
    // wall-clock). Full custom (both bounds frozen) gets the "◼ frozen"
    // prefix so it's clear the window won't scroll.
    const autoRefresh = _chartMode === "preset" || _chartMode === "custom_live";
    if (toolbarHint) toolbarHint.classList.toggle("frozen", !autoRefresh);

    if (!bins.length) return;
    drawRtp(bins);
    drawTx(bins);
    drawExec(bins);
    // Sparklines (G5) — last N bins for inline KPI trends.
    const tail = bins.slice(-60);
    _renderSparkline("spark-rtp", tail.map(b => b.rtp_avg ?? 0));
    _renderSparkline("spark-tx", tail.map(b => b.tx_avg ?? 0));
}

function setLagVal(id, text, sevClass) {
    const el = document.getElementById(id);
    if (!el) return;
    el.textContent = text;
    el.classList.remove("val-ok", "val-mid", "val-warn", "val-crit", "val-unknown");
    if (sevClass) el.classList.add(sevClass);
}
function updateLag(d) {
    // Point-in-time comparison: use the local block captured at the
    // instant the reference was polled, not the live `last_block`.
    // Otherwise we compare fresh-local to 0–15s-stale-reference and
    // over-report "local ahead" by up to ~37 blocks (15s × 2.5 blk/s).
    const local = d.reference_local_at_sample ?? null;
    const ref = d.reference_block ?? null;
    const err = d.reference_error;
    const sub = document.getElementById("lag-sub");

    setLagVal("lag-local", local != null ? fmtInt(local) : "—");

    if (ref == null) {
        setLagVal("lag-reference", "—", "val-unknown");
        setLagVal("lag-delta", "—", "val-unknown");
        sub.textContent = err
            ? `reference unreachable: ${err}`
            : "reference probe warming up…";
        return;
    }
    setLagVal("lag-reference", fmtInt(ref));

    if (local == null) {
        setLagVal("lag-delta", "—", "val-unknown");
        sub.textContent = "local tip not yet observed";
        return;
    }
    const delta = ref - local;
    const sign = delta > 0 ? "+" : delta < 0 ? "−" : "±";
    const sev = classifyLag(delta);
    setLagVal("lag-delta", `${sign}${Math.abs(delta)}`, sev);

    const ageSec = d.reference_checked_ms
        ? Math.round((Date.now() - d.reference_checked_ms) / 1000)
        : null;
    const ageNote = ageSec != null ? `snapshot ${ageSec}s old` : "";
    if (delta > 2) {
        sub.textContent = `we were ${delta} blocks behind the public reference at sample time · ${ageNote}`;
    } else if (delta < -2) {
        // Local ahead of reference is possible: our own RPC is strictly
        // fresher than the round-trip to the public endpoint. Not an alarm.
        sub.textContent = `local was ahead of reference by ${-delta} at sample time — our RPC samples fresher than the public endpoint · ${ageNote}`;
    } else {
        sub.textContent = `tracking reference within jitter (≤2 blocks) · ${ageNote}`;
    }
}

function updateEpoch(ep) {
    const num = document.getElementById("k-epoch");
    const sub = document.getElementById("k-epoch-sub");
    const bar = document.getElementById("k-epoch-bar");
    const progress = document.getElementById("k-epoch-progress");
    if (!num || !sub || !bar) return;
    if (!ep || ep.number == null) {
        num.textContent = "—";
        sub.textContent = "epoch info loading…";
        bar.style.width = "0%";
        if (progress) progress.setAttribute("aria-valuenow", "0");
        return;
    }
    num.textContent = `#${fmtInt(ep.number)}`;
    const done = ep.blocks_in ?? 0;
    const typical = ep.typical_length;
    if (typical && typical > 0) {
        const pct = Math.max(0, Math.min(100, done / typical * 100));
        bar.style.width = `${pct.toFixed(1)}%`;
        if (progress) progress.setAttribute("aria-valuenow", String(Math.round(pct)));
        const eta = ep.eta_sec;
        const etaStr = eta != null ? _fmtEta(eta) : null;
        sub.textContent =
            `${fmtInt(done)} / ~${fmtInt(typical)} blocks · ${pct.toFixed(1)}%`
            + (etaStr ? ` · ~${etaStr} to next epoch` : "");
    } else {
        // No closed-epoch sample yet — show raw count only.
        bar.style.width = "0%";
        if (progress) progress.setAttribute("aria-valuenow", "0");
        sub.textContent =
            `${fmtInt(done)} blocks · epoch length unknown (learning from first rollover)`;
    }
}
function _fmtEta(sec) {
    sec = Math.max(0, Math.round(sec));
    if (sec < 60) return `${sec}s`;
    const m = Math.floor(sec / 60);
    const s = sec % 60;
    if (m < 60) return s ? `${m}m ${s}s` : `${m}m`;
    const h = Math.floor(m / 60);
    const mm = m % 60;
    return mm ? `${h}h ${mm}m` : `${h}h`;
}

function updateIntegrity(d) {
    const card = document.getElementById("integrity-card");
    const count = document.getElementById("integrity-count");
    const label = document.getElementById("integrity-label");
    const detail = document.getElementById("integrity-detail");
    const n = d.reorg_count ?? 0;
    count.textContent = String(n);
    // Pluralize "reorg"/"reorgs" so n=1 reads cleanly ("1 reorg" not
    // "1 reorgs"). iter-5 audit §A3 polish item.
    label.textContent = `${n === 1 ? "reorg" : "reorgs"} observed since process start`;
    if (n === 0) {
        card.classList.remove("has-reorg");
        detail.textContent = d.blocks_seen
            ? `no reorgs across ${fmtInt(d.blocks_seen)} blocks since process start`
            : "collecting…";
        return;
    }
    card.classList.add("has-reorg");
    const when = d.last_reorg_ts_ms
        ? fmtSince(d.last_reorg_ts_ms)
        : "—";
    const short = (id) => id && id.length > 16 ? `${id.slice(0,10)}…${id.slice(-6)}` : (id || "—");
    detail.textContent =
        `last reorg: block #${fmtInt(d.last_reorg_number)} ` +
        `${when} — id changed ${short(d.last_reorg_old_id)} → ${short(d.last_reorg_new_id)}`;
}

// Shared x-axis config: category scale whose labels are time strings.
// Chart.js auto-thins ticks via maxTicksLimit so 300 points don't crush
// the axis. We keep category (not time) because category avoids the
// extra chartjs-adapter-date-fns dep and lets us format once per bin.
const xTickColor = "#8a8f99";
const xAxisTimes = {
    display: true,
    grid: { display: false },
    ticks: {
        color: xTickColor,
        font: { family: "JetBrains Mono", size: 10 },
        autoSkip: true,
        maxRotation: 0,
        maxTicksLimit: 8,
    },
};

const chartCommon = {
    responsive: true,
    maintainAspectRatio: false,
    animation: false,
    plugins: { legend: { display: false }, tooltip: { enabled: false } },
    interaction: { mode: "index", intersect: false },
    events: ["mousemove", "mouseout", "touchstart", "touchmove", "touchend"],
    scales: {
        x: xAxisTimes,
        y: {
            beginAtZero: true,
            grid: { color: "#262b36" },
            ticks: { color: "#8a8f99", font: { family: "JetBrains Mono", size: 10 } },
        },
    },
    elements: { point: { radius: 0 } },
};

// Build both the label array (time strings, stored on labels) and two
// parallel metadata arrays (block numbers + raw ms) that the tooltip
// plugins read from chart._binBlocks / chart._binTimes. Keeping them
// separate means the x-axis stays clean (just "HH:MM") while tooltips
// still get the full context.
function _labelsFromBins(bins) {
    return bins.map(b => _fmtBinClock(b.t));
}
function _attachBinMeta(chart, bins) {
    chart._binBlocks = bins.map(b => b.n_last);
    chart._binTimes  = bins.map(b => b.t);
}

function drawRtp(bins) {
    // Labels = bin time in HH:MM:SS UTC so the x-axis shows a time
    // scale. Block numbers + raw ms stored in meta arrays for the
    // tooltip (see _attachBinMeta).
    const labels = _labelsFromBins(bins);
    // Envelope chart: two datasets.
    //   [0] rtp_avg — primary, zone-colored (green/amber/red vs the
    //                 50/75 thresholds). Matches the alert rule, which
    //                 fires on the sustained 60-block avg. This is the
    //                 line users should read for severity.
    //   [1] rtp_p95 — faint upper envelope: "95% of blocks in this bin
    //                 were this bad or worse". Chosen over rtp_max
    //                 because a single 100% block per 750-block bin
    //                 (near-certain in practice) saturates max to the
    //                 ceiling and the envelope loses all signal. p95 is
    //                 robust to that outlier but still lights up when
    //                 the bin contains a sustained bad stretch — which
    //                 is exactly what correlates with alerts.
    const dataAvg = bins.map(b => b.rtp_avg ?? 0);
    const dataP95 = bins.map(b => b.rtp_p95 ?? b.rtp_max ?? 0);
    if (rtpChart) {
        rtpChart.data.labels = labels;
        rtpChart.data.datasets[0].data = dataAvg;
        rtpChart.data.datasets[1].data = dataP95;
        _attachBinMeta(rtpChart, bins);
        rtpChart.update("none");
        return;
    }
    rtpChart = new Chart(document.getElementById("chart-rtp"), {
        type: "line",
        data: {
            labels, datasets: [{
                data: dataAvg,
                // The visible line is drawn by rtpZonesPlugin's
                // afterDatasetsDraw — three clipped passes, one per
                // zone color. We hide Chart.js's own line stroke (and
                // skip fill) so only the zone-clipped passes show.
                borderColor: "rgba(0,0,0,0)",
                fill: false,
                borderWidth: 1.75,
                tension: 0.2,
                pointRadius: 0,
            }, {
                // p95 envelope — drawn natively by Chart.js, thin and
                // faint so the zone-colored avg line stays dominant.
                // rtpZonesPlugin explicitly targets dataset 0, so this
                // second series is not zone-clipped.
                data: dataP95,
                borderColor: "rgba(215,218,224,0.25)",
                fill: false,
                borderWidth: 1,
                tension: 0.2,
                pointRadius: 0,
            }],
        },
        plugins: [rtpZonesPlugin, crosshairPlugin],
        options: {
            ...chartCommon,
            layout: { padding: { top: 36 } },
            scales: {
                ...chartCommon.scales,
                y: {
                    ...chartCommon.scales.y,
                    min: 0,
                    max: 100,
                    ticks: {
                        ...chartCommon.scales.y.ticks,
                        callback: v => v + "%",
                    },
                },
            },
        },
    });
    _attachBinMeta(rtpChart, bins);
}

function drawTx(bins) {
    const labels = _labelsFromBins(bins);
    const data = bins.map(b => b.tx ?? 0);
    if (txChart) {
        txChart.data.labels = labels;
        txChart.data.datasets[0].data = data;
        _attachBinMeta(txChart, bins);
        txChart.update("none");
        return;
    }
    txChart = new Chart(document.getElementById("chart-tx"), {
        type: "bar",
        data: {
            labels, datasets: [{
                data,
                backgroundColor: "rgba(91,156,245,0.6)",
                borderWidth: 0,
            }],
        },
        plugins: [crosshairPlugin],
        options: { ...chartCommon, layout: { padding: { top: 36 } } },
    });
    _attachBinMeta(txChart, bins);
}

// Stacked bar of execution-time components per block. x: block number,
// y: microseconds. Three phases (state_reset, tx_exec, commit) stacked
// so an operator sees which phase dominates during a stress spike.
// We chose "bar stacked" over "area stacked" because per-block values
// are discrete samples, not a continuous series — area interpolation
// reads as smoothing we didn't actually do.
function drawExec(bins) {
    const labels = _labelsFromBins(bins);
    const sr = bins.map(b => b.state_reset_us ?? 0);
    const te = bins.map(b => b.tx_exec_us ?? 0);
    const cm = bins.map(b => b.commit_us ?? 0);
    if (execChart) {
        execChart.data.labels = labels;
        execChart.data.datasets[0].data = sr;
        execChart.data.datasets[1].data = te;
        execChart.data.datasets[2].data = cm;
        _attachBinMeta(execChart, bins);
        execChart.update("none");
        return;
    }
    execChart = new Chart(document.getElementById("chart-exec"), {
        type: "bar",
        data: {
            labels,
            datasets: [
                { label: "state_reset", data: sr,
                  backgroundColor: "rgba(91,156,245,0.75)",  borderWidth: 0, stack: "t" },
                { label: "tx_exec",     data: te,
                  backgroundColor: "rgba(107,207,118,0.75)", borderWidth: 0, stack: "t" },
                { label: "commit",      data: cm,
                  backgroundColor: "rgba(255,180,84,0.75)",  borderWidth: 0, stack: "t" },
            ],
        },
        plugins: [stackCrosshairPlugin],
        options: {
            ...chartCommon,
            layout: { padding: { top: 8 } },
            scales: {
                x: { ...xAxisTimes, stacked: true },
                y: {
                    ...chartCommon.scales.y,
                    stacked: true,
                    ticks: {
                        ...chartCommon.scales.y.ticks,
                        callback: v => v >= 1000 ? `${(v/1000).toFixed(1)}ms` : `${v}µs`,
                    },
                },
            },
        },
    });
    _attachBinMeta(execChart, bins);
}

function shortAddr(a) {
    if (!a || a.length < 12) return a || "—";
    return `${a.slice(0, 6)}…${a.slice(-4)}`;
}

// Single neutral color for the retried-ratio bars. Earlier versions used
// traffic-light fills (red/amber/green by ratio), but the table is
// already sorted by ratio DESC — meaning the top rows are always red,
// the bottom always green. That duplicated the information the sort
// already conveyed and made "high conflict" feel alarmist by default.
// The bar's LENGTH still encodes the ratio; the COLOR stays uniform.
// Meaningful severity coloring is preserved on the adjacent avg_rtp
// column, which carries an independent signal (retry intensity, not
// ubiquity).
const RATIO_FILL = "#5b9cf5";

// Client-side cache of recent top_retried responses. Keyed by
// (windowSec, minApp). 15s TTL matches the server-side cache so the
// two tiers compose — second click on the same filter inside 15s is
// instant. Without this, switching between two filters quickly made
// every click a 4s round-trip.
const _contractsCache = new Map();
const CONTRACTS_CACHE_TTL_MS = 15_000;
// AbortController for the in-flight contracts request. Switching filters
// while a previous query is still running would otherwise race — and
// the older response could land *after* the newer one, rendering stale
// data. Abort on every new fetch to guarantee last-in-wins.
let _contractsAbort = null;

async function fetchContracts() {
    const windowSec = parseInt(document.getElementById("contracts-window").value, 10);
    const minApp = parseInt(document.getElementById("contracts-min").value, 10);
    const cacheKey = `${windowSec}|${minApp}`;

    // Serve from cache if fresh — instant; no request, no loading state.
    const cached = _contractsCache.get(cacheKey);
    if (cached && (Date.now() - cached.at) < CONTRACTS_CACHE_TTL_MS) {
        renderContracts(cached.rows, windowSec);
        return;
    }

    // Cancel any older in-flight request so its late response can't
    // overwrite a newer one (classic race on fast filter toggling).
    if (_contractsAbort) _contractsAbort.abort();
    _contractsAbort = new AbortController();
    const myAbort = _contractsAbort;

    const qs = new URLSearchParams({ min_appearances: minApp, limit: 20 });
    if (windowSec > 0) {
        qs.set("since_ts_ms", String(Date.now() - windowSec * 1000));
    }
    _setContractsLoading(true);
    try {
        const r = await fetch(`/api/contracts/top_retried?${qs}`, { signal: myAbort.signal });
        if (!r.ok) throw new Error(r.statusText);
        const d = await r.json();
        const rows = d.rows || [];
        _contractsCache.set(cacheKey, { at: Date.now(), rows });
        renderContracts(rows, windowSec);
    } catch (e) {
        // AbortError is expected when the user clicked again — don't
        // flash an error message, the next call is already in flight.
        if (e && e.name === "AbortError") return;
        const body = document.getElementById("contracts-body");
        body.innerHTML = '<tr class="empty"><td colspan="6">query failed — try again</td></tr>';
    } finally {
        // Only drop the loading state if the controller we owned is
        // still the current one (i.e. nobody newer superseded us).
        if (myAbort === _contractsAbort) _setContractsLoading(false);
    }
}

function _setContractsLoading(on) {
    const card = document.getElementById("contracts-card");
    const winSel = document.getElementById("contracts-window");
    const minSel = document.getElementById("contracts-min");
    if (card) card.classList.toggle("loading", on);
    // Disable inputs while a request is in flight so rapid-fire clicks
    // can't stack on top of each other. Cached hits skip this entirely.
    if (winSel) winSel.disabled = on;
    if (minSel) minSel.disabled = on;
}

function renderContracts(rows, windowSec) {
    const body = document.getElementById("contracts-body");
    const hint = document.getElementById("contracts-hint");
    const wLabel = windowSec === 0 ? "all time" :
                   windowSec === 3600 ? "last hour" :
                   windowSec === 86400 ? "last 24h" : `last ${windowSec}s`;
    hint.textContent = `ranked by re-execution rate · ${wLabel} · ${rows.length} rows`;

    if (!rows.length) {
        body.innerHTML = '<tr class="empty"><td colspan="6">no contracts meeting threshold in window</td></tr>';
        return;
    }
    body.innerHTML = rows.map(r => {
        const pct = Math.round(r.retried_ratio * 100);
        const rtpCls = classifyRtp(r.avg_rtp_of_blocks);
        const labeled = r.label
            ? `<div class="contract-label">${escapeHTML(r.label)}${r.category && r.category !== "unknown"
                ? `<span class="cat">${escapeHTML(r.category)}</span>` : ""}</div>
               <div class="contract-addr" title="${escapeHTML(r.to_addr)}">${shortAddr(r.to_addr)}</div>`
            : `<div class="contract-addr unlabeled" title="${escapeHTML(r.to_addr)}">${shortAddr(r.to_addr)}</div>`;
        return `
            <tr>
                <td><div class="contract-cell">${labeled}</div></td>
                <td class="num">${fmtInt(r.blocks_appeared)}</td>
                <td class="num">${fmtInt(r.retried_blocks)}</td>
                <td>
                    <div class="ratio-cell">
                        <div class="ratio-bar">
                            <div class="fill" data-pct="${pct}" data-color="${RATIO_FILL}"></div>
                        </div>
                        <div class="ratio-pct">${pct}%</div>
                    </div>
                </td>
                <td class="num ${rtpCls}">${r.avg_rtp_of_blocks.toFixed(1)}%</td>
                <td class="num">${fmtInt(r.tx_count)}</td>
            </tr>`;
    }).join("");
    // CSP (style-src 'self' without 'unsafe-inline') silently drops
    // inline style="" attributes set via innerHTML, but allows the same
    // declarations when assigned through CSSOM (element.style.*). Set
    // bar width/color here so the hardened CSP stays intact.
    for (const el of body.querySelectorAll(".ratio-cell .fill")) {
        el.style.width = el.dataset.pct + "%";
        el.style.background = el.dataset.color;
    }
}

function escapeHTML(s) {
    return String(s).replace(/[&<>"']/g, c => ({
        "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
    })[c]);
}

// ---- critical incidents panel -----------------------------------------
// Shows the subset of recent alerts classified as operator-critical:
//   * rule starts with "assertion" — from AssertionRule, catches
//     block_cache.emplace, ring.cpp, panics, QC overshoot, chunk exhaustion
//   * rule starts with "probe:" AND severity critical/warn — probe failures
//   * severity "critical" from any rule — catch-all
//
// Intentionally excludes ordinary retry_spike WARN events — those are
// expected during a stress test and would drown out the signal in this
// panel. They still show in the generic "recent alerts" section below.
function isCriticalIncident(a) {
    const rule = a.rule || "";
    const sev = a.severity || "";
    if (rule.startsWith("assertion")) return true;
    if (rule.startsWith("probe:") && (sev === "critical" || sev === "warn")) return true;
    if (sev === "critical") return true;
    return false;
}

async function fetchProbes() {
    try {
        // Public-safe endpoint. /api/probes carries operator paths in
        // `details` and returns 404 through nginx by design (iter-2).
        const r = await pollFetch("probes-public", "/api/probes/public");
        if (!r.ok) throw new Error(r.statusText);
        const d = await r.json();
        renderProbes(d.probes || [], d.ran_at);
    } catch (e) { /* swallow */ }
}

function renderProbes(probes, ranAt) {
    const list = document.getElementById("probes-list");
    const hint = document.getElementById("probes-hint");
    if (!probes.length) {
        list.innerHTML = '<li class="empty">no probes configured</li>';
        return;
    }
    if (ranAt) {
        const agoSec = Math.round(Date.now() / 1000 - ranAt);
        hint.textContent = `periodic local health checks · last run ${agoSec}s ago`;
    }
    list.innerHTML = probes.map(p => {
        const status = p.status || "unknown";
        return `
            <li>
                <span class="status ${status}">${escapeHTML(status)}</span>
                <span class="probe-name">${escapeHTML(p.name || "")}</span>
                <span class="probe-summary">${escapeHTML(p.summary || "")}</span>
            </li>`;
    }).join("");
}

async function fetchIncidents() {
    try {
        const r = await pollFetch("alerts-50", "/api/alerts?limit=50");
        if (!r.ok) throw new Error(r.statusText);
        const alerts = await r.json();
        renderIncidents(alerts);
    } catch (e) { /* swallow, next tick retries */ }
}

function renderIncidents(alerts) {
    const card = document.getElementById("incidents-card");
    const list = document.getElementById("incidents-list");
    const hint = document.getElementById("incidents-hint");

    const incidents = (alerts || []).filter(isCriticalIncident);

    if (incidents.length === 0) {
        card.classList.remove("has-incidents");
        hint.textContent = "assertions · panics · chunk exhaustion · critical probes";
        list.innerHTML = '<li class="empty-ok">● clear — no critical events in buffer</li>';
        return;
    }

    // Collapse repeats per (rule, severity) bucket. A stress test can
    // push 32 retry_spike:critical rows into the buffer — showing all
    // of them hides the signal (distinct rules) behind the noise (same
    // rule, recurring). Keep only the newest per bucket and carry a
    // count so the card still signals recurrence.
    const buckets = new Map();
    for (const a of incidents) {
        const key = `${a.rule || ""}|${a.severity || ""}`;
        const prev = buckets.get(key);
        if (!prev) {
            buckets.set(key, { latest: a, count: 1 });
        } else {
            prev.count += 1;
            if ((a.ts_ms || 0) > (prev.latest.ts_ms || 0)) prev.latest = a;
        }
    }
    const deduped = [...buckets.values()]
        .sort((x, y) => (y.latest.ts_ms || 0) - (x.latest.ts_ms || 0));

    card.classList.add("has-incidents");
    const totalRaw = incidents.length;
    const distinct = deduped.length;
    hint.textContent = distinct === totalRaw
        ? `${distinct} incident${distinct === 1 ? "" : "s"}`
        : `${distinct} distinct incident${distinct === 1 ? "" : "s"} · ${totalRaw} events total`;

    list.innerHTML = deduped.map(({ latest: a, count }) => {
        const sev = a.severity || "critical";
        const kindLabel = (a.rule || "incident").replace(/^assertion$/, "assertion")
                                                  .replace(/^probe:/, "probe ");
        // Split detail into location (if present, typically after "at ")
        // and the rest of the message for cleaner rendering.
        let locationFragment = "";
        let detailFragment = a.detail || "";
        const atIdx = detailFragment.indexOf("  at ");
        if (atIdx > -1) {
            locationFragment = detailFragment.slice(atIdx + 5).trim();
            detailFragment = detailFragment.slice(0, atIdx).trim();
        }
        const recurrenceTag = count > 1
            ? ` <span class="recurrence" title="${count} total events in this bucket">+${count - 1} earlier</span>`
            : "";
        const timeBlock = a.ts_ms
            ? `<time class="alert-ts" datetime="${new Date(a.ts_ms).toISOString()}" title="${fmtFullTs(a.ts_ms)}">${escapeHTML(fmtAlertTime(a.ts_ms))}</time>`
            : "";
        return `
            <li>
                <span class="kind ${sev}">${escapeHTML(sev)}</span>
                <span class="detail">${escapeHTML(kindLabel)}${recurrenceTag}</span>
                <span class="detail">${escapeHTML(a.title || "")}${
                    detailFragment ? " — " + escapeHTML(detailFragment) : ""
                }${locationFragment ? ` <span class="location">${escapeHTML(locationFragment)}</span>` : ""}</span>
                <span class="ago">${timeBlock}</span>
            </li>`;
    }).join("");
}

document.getElementById("contracts-window").addEventListener("change", fetchContracts);
document.getElementById("contracts-min").addEventListener("change", fetchContracts);

// Period-selector buttons for the three main charts. One shared control
// drives all three so they stay visually in sync.
for (const btn of document.querySelectorAll(".charts-range .range-btn")) {
    if (btn.id === "custom-toggle" || btn.id === "custom-apply") continue;
    btn.addEventListener("click", () => {
        const sec = parseInt(btn.dataset.range, 10);
        if (Number.isFinite(sec) && sec > 0) {
            _setChartRange(sec);
            _closeCustomPanel();
        }
    });
}

// Custom-range disclosure & form.
const _customPanel = document.getElementById("charts-custom");
const _customToggle = document.getElementById("custom-toggle");
const _customApply = document.getElementById("custom-apply");
const _customFromInput = document.getElementById("custom-from");
const _customToInput = document.getElementById("custom-to");
const _customToNowCb = document.getElementById("custom-to-now");
// Populate timezone labels in the datepicker.
document.querySelectorAll("#tz-label-from, #tz-label-to")
    .forEach(el => { el.textContent = _tzShort; });
const _customMsg = document.getElementById("charts-custom-msg");

function _openCustomPanel() {
    if (!_customPanel) return;
    _customPanel.classList.remove("hidden");
    if (_customToggle) _customToggle.setAttribute("aria-expanded", "true");
    // Prefill: if we're already in custom mode, show the saved window;
    // otherwise, show (now - current preset) → now so the user can
    // nudge bounds rather than starting from scratch.
    const now = new Date();
    const nowLocal = _toLocalInputValue(now.getTime());
    const isCustomMode = _chartMode === "custom" || _chartMode === "custom_live";
    const fromBase = isCustomMode && _customFromMs != null
        ? _customFromMs : now.getTime() - _chartRangeSec * 1000;
    const toBase = _chartMode === "custom" && _customToMs != null
        ? _customToMs : now.getTime();
    if (!_customFromInput.value) _customFromInput.value = _toLocalInputValue(fromBase);
    if (!_customToInput.value)   _customToInput.value   = _toLocalInputValue(toBase);
    // Clamp max to now so user can't pick the future.
    _customFromInput.max = nowLocal;
    _customToInput.max   = nowLocal;
    // Restore "now (live)" checkbox state from the current mode.
    if (_customToNowCb) {
        _customToNowCb.checked = _chartMode === "custom_live";
        if (_customToInput) _customToInput.disabled = _customToNowCb.checked;
    }
}
function _closeCustomPanel() {
    if (!_customPanel) return;
    _customPanel.classList.add("hidden");
    if (_customToggle) _customToggle.setAttribute("aria-expanded", "false");
    if (_customMsg) _customMsg.textContent = "";
}
// Datepicker uses local timezone — datetime-local inputs natively work
// in the browser's local tz, which now matches the chart axes.
function _toLocalInputValue(ms) {
    const d = new Date(ms);
    const p = (n, w = 2) => String(n).padStart(w, "0");
    return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())}`
         + `T${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
}
// Parse a datetime-local input value as local wall-clock (the browser's
// default behavior for Date constructor with no timezone suffix).
function _parseLocalInputValue(s) {
    if (!s) return NaN;
    return new Date(s).getTime();
}

if (_customToggle) {
    _customToggle.addEventListener("click", () => {
        if (_customPanel.classList.contains("hidden")) _openCustomPanel();
        else _closeCustomPanel();
    });
}
// "now (live)" checkbox disables the 'to' date input and switches the
// apply handler to custom_live mode, where 'to' tracks wall-clock on
// every fetch. Doesn't affect the 300-points contract — bin_ms is
// still (now - from)/300, just recomputed each tick.
if (_customToNowCb) {
    _customToNowCb.addEventListener("change", () => {
        if (_customToInput) _customToInput.disabled = _customToNowCb.checked;
    });
}
if (_customApply) {
    _customApply.addEventListener("click", () => {
        const fromMs = _parseLocalInputValue(_customFromInput.value);
        if (!Number.isFinite(fromMs)) {
            _customMsg.textContent = "pick a valid 'from' date"; return;
        }
        const liveMode = _customToNowCb && _customToNowCb.checked;
        const toMs = liveMode ? Date.now() : _parseLocalInputValue(_customToInput.value);
        if (!liveMode && !Number.isFinite(toMs)) {
            _customMsg.textContent = "pick a valid 'to' date"; return;
        }
        if (toMs <= fromMs) {
            _customMsg.textContent = "'to' must be after 'from'"; return;
        }
        const span = toMs - fromMs;
        if (span > CHART_CUSTOM_MAX_SPAN_MS) {
            _customMsg.textContent = "max span is 7 days"; return;
        }
        if (!liveMode && toMs > Date.now() + 60_000) {
            _customMsg.textContent = "'to' is in the future"; return;
        }
        if (_dataStartMs && fromMs < _dataStartMs) {
            const ds = new Date(_dataStartMs);
            _customMsg.textContent = `no data before ${ds.toISOString().slice(0,10)}`;
            // Don't block — just warn, user can still apply.
        } else {
            _customMsg.textContent = "";
        }
        if (liveMode) _setChartCustomLive(fromMs);
        else _setChartCustomRange(fromMs, toMs);
    });
}

// Apply persisted selection on load (stored in localStorage / URL).
_syncChartControlsUI();

fetchState();
fetchBlocks();
fetchContracts();
fetchIncidents();
fetchProbes();
// Throttle polling when the tab is hidden to save battery and reduce
// rate-limit pressure (B5). On refocus, immediately refresh and
// restore the normal interval.
function _schedule(fn, interval) {
    let id = setInterval(fn, interval);
    document.addEventListener("visibilitychange", () => {
        clearInterval(id);
        const next = document.hidden ? interval * 6 : interval;
        id = setInterval(fn, next);
        if (!document.hidden) fn();
    });
}
_schedule(fetchState, STATE_INTERVAL);
_schedule(fetchBlocks, BLOCKS_INTERVAL);
_schedule(fetchContracts, CONTRACTS_INTERVAL);
_schedule(fetchIncidents, INCIDENTS_INTERVAL);
_schedule(fetchProbes, PROBES_INTERVAL);

// Chart.js internally uses a ResizeObserver on each canvas parent, but
// on mobile orientation change (portrait↔landscape) the parent's
// computed size may settle across two frames while the browser finishes
// CSS relayout. The observer can catch an intermediate size and leave
// the canvas too tall for the container — audit 2026-04-20 showed
// a ~500 px black gap between the exec-breakdown chart and the next
// panel until the next poll-tick's chart.update() happened to reflow.
//
// Explicit window `resize` listener + debounce + chart.resize() on each
// chart closes that gap immediately on reflow. Debounce keeps us off the
// critical path during normal desktop window drags (which fire resize
// at paint cadence). `orientationchange` is deprecated but still fires
// on some mobile browsers in addition to resize; listen for both to be
// safe, the debounce coalesces duplicates.
let _resizeDebounce = null;
function _resizeAllCharts() {
    for (const c of [rtpChart, txChart, execChart]) {
        if (c && typeof c.resize === "function") {
            try { c.resize(); } catch (_) { /* chart not ready yet */ }
        }
    }
}
function _scheduleResize() {
    if (_resizeDebounce) clearTimeout(_resizeDebounce);
    _resizeDebounce = setTimeout(_resizeAllCharts, 120);
}
window.addEventListener("resize", _scheduleResize);
window.addEventListener("orientationchange", _scheduleResize);

// Deep-link copy button (G7) — copies the current URL (including chart
// range params) to clipboard so operators can share a specific view.
const _copyLinkBtn = document.getElementById("copy-chart-link");
if (_copyLinkBtn) {
    _copyLinkBtn.addEventListener("click", () => {
        navigator.clipboard.writeText(window.location.href).then(() => {
            _copyLinkBtn.textContent = "copied!";
            _copyLinkBtn.classList.add("copied");
            setTimeout(() => { _copyLinkBtn.textContent = "copy link"; _copyLinkBtn.classList.remove("copied"); }, 1500);
        }).catch(() => {
            _copyLinkBtn.textContent = "copy failed";
            setTimeout(() => { _copyLinkBtn.textContent = "copy link"; }, 1500);
        });
    });
}
