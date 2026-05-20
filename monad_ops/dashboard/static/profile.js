// Operator-readiness profile renderer.
//
// One-shot fetch on page load + periodic refresh — pulls /api/state,
// /api/version and /api/validator_set, fills the profile cards. No
// charts here on purpose; the dashboard is the deep-dive surface,
// /profile is the at-a-glance summary a reviewer can skim in 30s.

const PROFILE_REFRESH_MS = 30000;

function profileSetText(id, value, subId, subText) {
    const el = document.getElementById(id);
    if (el) el.textContent = value;
    if (subId && subText !== undefined) {
        const sub = document.getElementById(subId);
        if (sub) sub.textContent = subText;
    }
}

function profileFmtInt(n) {
    if (n == null || !isFinite(n)) return "—";
    return Math.round(n).toLocaleString("en-US");
}

function profileFmtPct(n, digits = 2) {
    if (n == null || !isFinite(n)) return "—";
    return `${Number(n).toFixed(digits)} %`;
}

function profileFmtUptime(sec) {
    if (sec == null || !isFinite(sec)) return "—";
    const s = Math.max(0, Math.floor(sec));
    const d = Math.floor(s / 86400);
    const h = Math.floor((s % 86400) / 3600);
    const m = Math.floor((s % 3600) / 60);
    if (d > 0) return `${d}d ${h}h`;
    if (h > 0) return `${h}h ${m}m`;
    return `${m}m`;
}

function profileFmtCompact(n) {
    if (n == null || !isFinite(n)) return "—";
    const v = Number(n);
    if (v >= 1_000_000_000) return (v / 1_000_000_000).toFixed(2).replace(/\.?0+$/, "") + "B";
    if (v >= 1_000_000)     return (v / 1_000_000).toFixed(2).replace(/\.?0+$/, "") + "M";
    if (v >= 1_000)         return (v / 1_000).toFixed(2).replace(/\.?0+$/, "") + "K";
    return String(Math.round(v));
}

function profileMonFromWei(weiStr) {
    if (weiStr == null) return null;
    try {
        const w = BigInt(String(weiStr));
        return Number(w / (10n ** 18n));
    } catch (_) {
        return null;
    }
}

async function profileLoad() {
    let state = null, version = null, valset = null;
    try {
        const [sr, vr, kr] = await Promise.all([
            fetch("/api/state"),
            fetch("/api/version"),
            fetch("/api/validator_set"),
        ]);
        if (sr.ok) state = await sr.json();
        if (vr.ok) version = await vr.json();
        if (kr.ok) valset = await kr.json();
    } catch (e) { /* surface as dashes; better than half-populated */ }

    // Reliability row.
    if (state) {
        profileSetText("prof-uptime", profileFmtUptime(state.uptime_sec));
        const reorgsTotal = state.reorg_count ?? 0;
        const reorgs24h = state.recent_reorgs_24h ?? 0;
        profileSetText(
            "prof-reorgs",
            `${profileFmtInt(reorgsTotal)} / ${profileFmtInt(reorgs24h)}`,
            "prof-reorgs-sub",
            `lifetime · last 24 h`
        );
        const vtp = state.consensus && state.consensus.validator_timeout_pct_5m;
        profileSetText("prof-vtp", profileFmtPct(vtp));
        // Performance row.
        profileSetText("prof-rtp",
            `${profileFmtPct(state.rtp_avg_5m, 1)} / ${profileFmtPct(state.rtp_max_1m, 1)}`);
        profileSetText("prof-bps",
            state.blocks_per_sec_1m != null
                ? Number(state.blocks_per_sec_1m).toFixed(2) : "—");
        profileSetText("prof-tps", profileFmtCompact(state.tps_effective_peak_1m));
    }

    if (version) {
        if (version.status === "up_to_date") {
            profileSetText("prof-version", version.installed || "—",
                "prof-version-sub", `matches latest stable in repo`);
        } else if (version.status === "update_available") {
            profileSetText("prof-version",
                `${version.installed || "?"} → ${version.latest || "?"}`,
                "prof-version-sub", `update available`);
        } else {
            profileSetText("prof-version", version.installed || "—",
                "prof-version-sub", version.error || "checking apt repo");
        }
    }

    if (valset && valset.status === "ok") {
        const cap = valset.active_valset_cap || 200;
        const floorMon = profileMonFromWei(valset.lowest_active_stake_wei);
        const floorTxt = floorMon != null ? ` · ${profileFmtCompact(floorMon)} MON floor` : "";
        profileSetText("prof-valset",
            `${valset.consensus_count ?? "?"} / ${cap}`,
            "prof-valset-sub",
            `epoch ${valset.epoch ?? "?"}${floorTxt}`);
    } else if (valset) {
        profileSetText("prof-valset", "—", "prof-valset-sub",
            valset.error || "snapshot probe has not produced a result yet");
    }
}

profileLoad();
setInterval(profileLoad, PROFILE_REFRESH_MS);
