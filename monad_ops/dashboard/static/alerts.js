// Alerts history page. Queries /api/alerts/history with window+severity
// filters and renders a simple paginated table. Auto-refreshes while
// viewing "last hour" or "last 6h" windows since those change; static
// for larger windows to avoid surprise re-renders mid-read.

const AUTO_REFRESH_WINDOWS = new Set([3600, 21600]);
let lastQueryMs = 0;

const _tzShort = (() => {
    const s = new Intl.DateTimeFormat(undefined, { timeZoneName: "short" })
        .formatToParts(new Date()).find(p => p.type === "timeZoneName");
    return s ? s.value : "local";
})();

function _fmtLocal(ms) {
    const d = new Date(ms);
    const z = (n, w = 2) => String(n).padStart(w, "0");
    return `${d.getFullYear()}-${z(d.getMonth() + 1)}-${z(d.getDate())} `
         + `${z(d.getHours())}:${z(d.getMinutes())}:${z(d.getSeconds())}`;
}

function fmtTime(ms) {
    if (!ms) return "—";
    return _fmtLocal(ms).slice(5);
}
// Exact local tz with year+seconds — shown in the tooltip on hover/tap.
// Matches the dashboard's fmtFullTs format so the two pages read identically.
function fmtFullTs(ms) {
    return _fmtLocal(ms) + " " + _tzShort;
}
function fmtInt(n) { return Number(n).toLocaleString("en-US"); }
function escapeHTML(s) {
    return String(s).replace(/[&<>"']/g, c => ({
        "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
    })[c]);
}

// Reorg alert detail is deterministic: "Block #<n> id changed: …".
// Pull the block_number so we can link to the trace endpoint. Returns
// null if the detail doesn't match (future-proofs the rendering —
// caller just won't emit a button).
function _parseReorgBlockNumber(detail) {
    const m = /Block #(\d+) id changed/.exec(detail || "");
    return m ? parseInt(m[1], 10) : null;
}

async function fetchHistory() {
    const windowSec = parseInt(document.getElementById("alerts-window").value, 10);
    const severity = document.getElementById("alerts-severity").value;
    const limit = parseInt(document.getElementById("alerts-limit").value, 10);
    const qs = new URLSearchParams({ limit: String(limit) });
    if (windowSec > 0) qs.set("from_ts_ms", String(Date.now() - windowSec * 1000));
    if (severity) qs.set("severity", severity);

    const body = document.getElementById("history-body");
    const hint = document.getElementById("alerts-hint");
    const countPill = document.getElementById("alerts-count-pill").querySelector(".label");

    try {
        const r = await fetch(`/api/alerts/history?${qs}`);
        if (!r.ok) throw new Error(r.statusText);
        const d = await r.json();
        lastQueryMs = Date.now();
        document.getElementById("ts").textContent =
            _fmtLocal(Date.now()) + " " + _tzShort;

        countPill.textContent = `${fmtInt(d.count)} rows`;
        const wLabel = windowSec === 0 ? "all time" :
                       windowSec === 3600 ? "last hour" :
                       windowSec === 21600 ? "last 6h" :
                       windowSec === 86400 ? "last 24h" :
                       windowSec === 604800 ? "last 7d" : `last ${windowSec}s`;
        hint.textContent = `${wLabel}${severity ? " · severity=" + severity : ""} · saved in local database`;

        if (!d.alerts || d.alerts.length === 0) {
            body.innerHTML = '<tr class="empty"><td colspan="4">no alerts in window</td></tr>';
            return;
        }
        const _SEV_OK = /^[a-z_-]+$/i;
        body.innerHTML = d.alerts.map(a => {
            const sev = a.severity || "";
            const sevClass = _SEV_OK.test(sev) ? sev : "";
            const fullTs = a.ts_ms ? fmtFullTs(a.ts_ms) : "";
            // Reorg alerts get a trace-download link — /api/reorgs/{n}
            // returns the reorged block plus N neighbors as JSON, safe
            // to save and share in operator discussions.
            let traceBtn = "";
            if (a.rule === "reorg") {
                const bn = _parseReorgBlockNumber(a.detail);
                if (bn != null) {
                    const href = `/api/reorgs/${bn}?window=30`;
                    traceBtn = `<a class="alert-trace-btn" href="${href}" `
                             + `download="reorg-${bn}.json" `
                             + `title="download trace JSON (±30 blocks)">`
                             + `📥 trace</a>`;
                }
            }
            return `
                <tr>
                    <td class="mono-time" title="${escapeHTML(fullTs)}">${fmtTime(a.ts_ms)}</td>
                    <td><span class="sev ${sevClass}">${escapeHTML(sev)}</span></td>
                    <td class="rule">${escapeHTML(a.rule || "")}</td>
                    <td class="detail-cell">
                        <div class="alert-title">${escapeHTML(a.title || "")}</div>
                        ${a.detail ? `<div class="alert-detail">${escapeHTML(a.detail)}</div>` : ""}
                        ${traceBtn}
                    </td>
                </tr>`;
        }).join("");
    } catch (e) {
        body.innerHTML = '<tr class="empty"><td colspan="4">query failed</td></tr>';
    }
}

// Pre-select dropdowns from URL query string so a link like
//   /alerts?severity=critical&window=0
// opens with those filters already applied. iter-5 audit §A7.
function _setSelectIfValid(selectId, val) {
    if (val == null) return;
    const sel = document.getElementById(selectId);
    if (!sel) return;
    const options = Array.from(sel.options).map(o => o.value);
    if (options.includes(String(val))) sel.value = String(val);
}
(function applyUrlFilters() {
    const p = new URLSearchParams(window.location.search);
    _setSelectIfValid("alerts-window",   p.get("window"));
    _setSelectIfValid("alerts-severity", p.get("severity"));
    _setSelectIfValid("alerts-limit",    p.get("limit"));
})();

document.getElementById("alerts-window").addEventListener("change", fetchHistory);
document.getElementById("alerts-severity").addEventListener("change", fetchHistory);
document.getElementById("alerts-limit").addEventListener("change", fetchHistory);

// CSV export (G4) — exports the visible table as a downloadable CSV.
let _lastAlerts = [];
// Patch fetchHistory to stash latest data for export.
const _origFetch = fetchHistory;

fetchHistory();

document.getElementById("export-csv").addEventListener("click", () => {
    const rows = document.querySelectorAll("#history-body tr:not(.empty)");
    if (!rows.length) return;
    const lines = ["time,severity,rule,title,detail"];
    rows.forEach(tr => {
        const cells = tr.querySelectorAll("td");
        if (cells.length < 4) return;
        const time = (cells[0].getAttribute("title") || cells[0].textContent).trim();
        const sev = cells[1].textContent.trim();
        const rule = cells[2].textContent.trim();
        const detailEl = cells[3];
        const title = (detailEl.querySelector(".alert-title") || {}).textContent || "";
        const detail = (detailEl.querySelector(".alert-detail") || {}).textContent || "";
        const esc = s => '"' + s.replace(/"/g, '""') + '"';
        lines.push([esc(time), esc(sev), esc(rule), esc(title.trim()), esc(detail.trim())].join(","));
    });
    const blob = new Blob([lines.join("\n")], { type: "text/csv" });
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = `monad-ops-alerts-${new Date().toISOString().slice(0,10)}.csv`;
    a.click();
    URL.revokeObjectURL(a.href);
});

setInterval(() => {
    const w = parseInt(document.getElementById("alerts-window").value, 10);
    if (AUTO_REFRESH_WINDOWS.has(w)) fetchHistory();
}, 15000);
