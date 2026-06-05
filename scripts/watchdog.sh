#!/usr/bin/env bash
# monad-ops watchdog.
#
# Independent of monad-ops itself so it can alert when monad-ops is
# the thing that died. Invoked by a systemd timer every 5s:
#   1. HTTP-check the local dashboard (127.0.0.1:8873) with a short
#      timeout. Any non-200 or timeout = problem.
#   2. Ingestion-freshness: a 200 with a stale last block is still a
#      failure. If the dashboard hasn't ingested a block in a while AND
#      the node RPC is well ahead, ingestion is wedged (the 2026-06-05
#      broken-follow class) — invisible to a plain HTTP probe.
#   3. Read current memory usage and a few systemd state properties.
#   3. Alerting is edge-triggered with reminder cadence:
#        - healthy → degraded : send UNHEALTHY alert (once)
#        - degraded → healthy : send RECOVERED alert (once)
#        - degraded → degraded: resend a STILL UNHEALTHY reminder
#                               if 5 minutes have passed since the
#                               last message — so the issue doesn't
#                               fade from view, without 5s spam.
#      State lives in /run so a reboot starts clean.
#
# Deliberate simplicity — no python deps, no DB. Must keep working
# even when the main service is completely wedged.

set -u

STATE_FILE="${RUNTIME_DIRECTORY:-/run/monad-ops-watchdog}/state"
HEALTH_URL="http://127.0.0.1:8873/api/state"
HEALTH_TIMEOUT_SEC=5
SERVICE="monad-ops.service"
# Node RPC, used to tell "ingestion wedged" (node ahead, dashboard
# frozen) apart from a genuine chain halt (which the in-process stall
# rule already owns — we stay quiet there to avoid a double alert).
NODE_RPC_URL="${NODE_RPC_URL:-http://127.0.0.1:8080}"
# Consider ingestion stale after no fresh block for this long. The chain
# produces ~2.5 blk/s, so 3 min of silence is far outside normal.
STALE_BLOCK_SEC=180
# Minimum node-ahead-of-dashboard gap to call it a wedge rather than
# ordinary tailer lag. ~2.5 blk/s x 180s ≈ 450 blocks.
INGEST_LAG_BLOCKS=400
# While degraded, send a reminder every N seconds so a slow fire
# doesn't disappear from the chat backlog. 300s = 5 min — often
# enough to stay visible, rare enough to not spam.
REMINDER_INTERVAL_SEC=300
# Hysteresis: require N consecutive failed (or ok) checks before we
# FLIP the announced state. Stops a flapping event-loop from spamming
# Telegram with one DEGRADED + one RECOVERED every few seconds. At
# a 5s cadence, 3 consecutive = 10–15s of sustained failure.
FAIL_STREAK_TO_DEGRADE=3
OK_STREAK_TO_RECOVER=3

# Telegram creds. Expected via environment (systemd EnvironmentFile=
# in monad-ops-watchdog.service, or ad-hoc export for dev runs). Keeps
# live bot token + chat id out of git history — the committed
# watchdog.env.example shows the key names with placeholder values.
: "${TG_BOT_TOKEN:?TG_BOT_TOKEN not set (set via EnvironmentFile in systemd unit)}"
: "${TG_CHAT_ID:?TG_CHAT_ID not set}"
: "${TG_TOPIC_ID:?TG_TOPIC_ID not set}"

# Collect facts.
now_iso=$(date -u +"%Y-%m-%d %H:%M:%S UTC")
active_state=$(systemctl show -p ActiveState --value "$SERVICE" 2>/dev/null || echo "unknown")
sub_state=$(systemctl show -p SubState --value "$SERVICE" 2>/dev/null || echo "unknown")
mem_current=$(systemctl show -p MemoryCurrent --value "$SERVICE" 2>/dev/null || echo "0")
mem_max=$(systemctl show -p MemoryMax --value "$SERVICE" 2>/dev/null || echo "0")

# HTTP probe — capture body + status code in one request so we can also
# inspect ingestion freshness, not just reachability. Body is single-line
# JSON, so we append the code on its own trailing line and split.
http_resp=$(curl -s --max-time "$HEALTH_TIMEOUT_SEC" -w $'\n%{http_code}' "$HEALTH_URL" 2>/dev/null || printf '\n000')
http_code="${http_resp##*$'\n'}"
http_body="${http_resp%$'\n'*}"

# Decide status.
degraded_reason=""
if [[ "$active_state" != "active" ]]; then
    degraded_reason="service not active: ActiveState=$active_state SubState=$sub_state"
elif [[ "$http_code" != "200" ]]; then
    degraded_reason="http $HEALTH_URL returned $http_code (timeout or error)"
fi

# Ingestion freshness — only when the endpoint answered cleanly. A 200
# with a 30h-old last block is the failure mode that ran silent on
# 2026-06-05; the plain probe above can't see it.
if [[ -z "$degraded_reason" && "$http_code" == "200" ]]; then
    ops_last_block=$(grep -oE '"last_block":[0-9]+' <<< "$http_body" | grep -oE '[0-9]+' | head -1)
    ops_seen_ms=$(grep -oE '"last_block_seen_ms":[0-9]+' <<< "$http_body" | grep -oE '[0-9]+' | head -1)
    if [[ "$ops_seen_ms" =~ ^[0-9]+$ ]]; then
        now_ms=$(date +%s%3N)
        age_ms=$(( now_ms - ops_seen_ms ))
        if (( age_ms > STALE_BLOCK_SEC * 1000 )); then
            age_s=$(( age_ms / 1000 ))
            # Is the node still producing? Compare to its RPC tip.
            node_hex=$(curl -s --max-time 4 -X POST \
                -H 'content-type: application/json' \
                --data '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}' \
                "$NODE_RPC_URL" 2>/dev/null | grep -oiE '0x[0-9a-f]+' | head -1)
            if [[ "$node_hex" =~ ^0[xX][0-9a-fA-F]+$ ]]; then
                node_block=$(( node_hex ))
                lag=$(( node_block - ${ops_last_block:-0} ))
                if (( lag > INGEST_LAG_BLOCKS )); then
                    degraded_reason="ingestion wedged: node tip $node_block, dashboard ${ops_last_block:-?} (Δ${lag} blocks, no fresh block ${age_s}s) — tailer follow likely broken"
                fi
                # node≈dashboard ⇒ chain itself quiet/halted; the
                # in-process stall rule owns that. Stay quiet here.
            else
                degraded_reason="ingestion stale ${age_s}s and node RPC unreachable at $NODE_RPC_URL"
            fi
        fi
    fi
fi

# MemoryMax pressure warning (80% threshold) — useful as a leading
# indicator before we actually OOM / get killed.
if [[ "$mem_max" != "infinity" && "$mem_max" -gt 0 ]]; then
    pct=$(( mem_current * 100 / mem_max ))
    if (( pct >= 80 )); then
        if [[ -z "$degraded_reason" ]]; then
            degraded_reason="memory pressure: ${pct}% of MemoryMax ($mem_current / $mem_max)"
        else
            degraded_reason="$degraded_reason; memory ${pct}% of MemoryMax"
        fi
    fi
fi

# State file, now extended to carry streak counters so hysteresis
# works across invocations. Format (fields separated by ':'):
#   healthy:<ok_streak>:<fail_streak>
#   degraded:<last_notify_ts>:<ok_streak>:<fail_streak>
# A missing file or legacy "healthy"/"degraded:ts" value is tolerated.
prev_raw="healthy:0:0"
if [[ -f "$STATE_FILE" ]]; then
    prev_raw=$(cat "$STATE_FILE" 2>/dev/null || echo "healthy:0:0")
fi
IFS=':' read -r prev_state f1 f2 f3 <<< "$prev_raw"
prev_notify_ts=0
ok_streak=0
fail_streak=0
if [[ "$prev_state" == "degraded" ]]; then
    prev_notify_ts="${f1:-0}"
    ok_streak="${f2:-0}"
    fail_streak="${f3:-0}"
else
    ok_streak="${f1:-0}"
    fail_streak="${f2:-0}"
fi
# Guard against non-integers from an older on-disk format.
[[ "$prev_notify_ts" =~ ^[0-9]+$ ]] || prev_notify_ts=0
[[ "$ok_streak" =~ ^[0-9]+$ ]] || ok_streak=0
[[ "$fail_streak" =~ ^[0-9]+$ ]] || fail_streak=0

# Raw observation this tick.
observed="healthy"
if [[ -n "$degraded_reason" ]]; then
    observed="degraded"
fi

# Update streaks.
if [[ "$observed" == "healthy" ]]; then
    ok_streak=$((ok_streak + 1))
    fail_streak=0
else
    fail_streak=$((fail_streak + 1))
    ok_streak=0
fi

# Hysteresis: flip announced state only when a streak crosses the
# threshold. A single bad tick can't move us to degraded, and a single
# good tick can't declare recovered.
announced_state="$prev_state"
if [[ "$prev_state" == "healthy" && $fail_streak -ge $FAIL_STREAK_TO_DEGRADE ]]; then
    announced_state="degraded"
elif [[ "$prev_state" == "degraded" && $ok_streak -ge $OK_STREAK_TO_RECOVER ]]; then
    announced_state="healthy"
fi

new_state="$announced_state"
now_ts=$(date +%s)

send_telegram() {
    local emoji="$1"
    local title="$2"
    local body="$3"
    local text="${emoji} <b>[monad-ops-watchdog]</b> ${title}
<i>external health check · ${now_iso}</i>

${body}"
    curl -s --max-time 10 \
        -X POST "https://api.telegram.org/bot${TG_BOT_TOKEN}/sendMessage" \
        -d "chat_id=${TG_CHAT_ID}" \
        -d "message_thread_id=${TG_TOPIC_ID}" \
        -d "parse_mode=HTML" \
        -d "disable_web_page_preview=true" \
        --data-urlencode "text=${text}" \
        >/dev/null || true
}

# Alert policy:
#   healthy → degraded   : UNHEALTHY alert, reset notify_ts
#   degraded → healthy   : RECOVERED alert, clear notify_ts
#   degraded → degraded  : if >=REMINDER_INTERVAL_SEC since last notify,
#                          send STILL UNHEALTHY reminder; otherwise quiet
#   healthy → healthy    : nothing; we don't even rewrite the state file
notify_type=""
if [[ "$prev_state" == "healthy" && "$new_state" == "degraded" ]]; then
    notify_type="unhealthy"
elif [[ "$prev_state" == "degraded" && "$new_state" == "healthy" ]]; then
    notify_type="recovered"
elif [[ "$prev_state" == "degraded" && "$new_state" == "degraded" ]]; then
    if (( now_ts - prev_notify_ts >= REMINDER_INTERVAL_SEC )); then
        notify_type="still_unhealthy"
    fi
fi

common_body="reason: ${degraded_reason:-n/a}
http_code: ${http_code}
active_state: ${active_state}/${sub_state}
memory: ${mem_current} / ${mem_max}"

case "$notify_type" in
    unhealthy)
        send_telegram "🔴" "monad-ops UNHEALTHY" "$common_body
streaks: fail=${fail_streak} ok=${ok_streak}"
        echo "degraded:$now_ts:$ok_streak:$fail_streak" > "$STATE_FILE"
        ;;
    still_unhealthy)
        elapsed_min=$(( (now_ts - prev_notify_ts) / 60 ))
        send_telegram "🔴" "monad-ops STILL UNHEALTHY (${elapsed_min}m reminder)" "$common_body"
        echo "degraded:$now_ts:$ok_streak:$fail_streak" > "$STATE_FILE"
        ;;
    recovered)
        send_telegram "🟢" "monad-ops recovered" "service reachable again
http_code: ${http_code}
active_state: ${active_state}/${sub_state}
memory: ${mem_current} / ${mem_max}"
        echo "healthy:$ok_streak:$fail_streak" > "$STATE_FILE"
        ;;
    *)
        # No announcement this tick. Still persist streaks so hysteresis
        # math works on the next tick.
        if [[ "$new_state" == "degraded" ]]; then
            echo "degraded:${prev_notify_ts}:$ok_streak:$fail_streak" > "$STATE_FILE"
        else
            echo "healthy:$ok_streak:$fail_streak" > "$STATE_FILE"
        fi
        ;;
esac

# Always print to journal so `journalctl -u monad-ops-watchdog` tells
# the story even when Telegram is blocked.
echo "$(date -u +%H:%M:%S) obs=$observed announced=$new_state prev=$prev_state http=$http_code ok=$ok_streak fail=$fail_streak mem=${mem_current}/${mem_max} notify=${notify_type:-none}"
exit 0
