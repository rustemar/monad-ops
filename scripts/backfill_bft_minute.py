#!/usr/bin/env python3
"""Backfill bft_minute network-layer counters from monad-bft journald.

Re-running is safe — uses MAX() on update so live counters aren't
overwritten if the live tailer already accumulated this minute.
"""
import argparse
import json
import sqlite3
import subprocess
import sys
from collections import defaultdict
from datetime import datetime

MARKERS = {
    '"message":"failed to decrypt message"': "decrypt",
    '"message":"session timeout expired"': "session",
    '"message":"Timestamp validation failed"': "tstamp",
}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="/home/solana/monad-project/monad-ops/data/state.db")
    ap.add_argument("--unit", default="monad-bft")
    ap.add_argument("--hours", type=int, default=12)
    ap.add_argument("--journal-timeout", type=int, default=180)
    args = ap.parse_args()

    proc = subprocess.run(
        ["sudo", "journalctl", "-u", args.unit,
         "--since", f"{args.hours} hours ago", "--no-pager", "-o", "cat"],
        capture_output=True, text=True, timeout=args.journal_timeout,
    )
    if proc.returncode != 0:
        print(f"journalctl failed: {proc.stderr.strip()}", file=sys.stderr)
        return 1

    buckets: dict[int, dict[str, int]] = defaultdict(
        lambda: {"decrypt": 0, "session": 0, "tstamp": 0}
    )
    matched = 0
    for line in proc.stdout.splitlines():
        kind = next((k for m, k in MARKERS.items() if m in line), None)
        if kind is None:
            continue
        try:
            d = json.loads(line)
        except ValueError:
            continue
        ts = d.get("timestamp", "")
        if not ts:
            continue
        if ts.endswith("Z"):
            ts = ts[:-1] + "+00:00"
        try:
            dt_obj = datetime.fromisoformat(ts)
        except ValueError:
            continue
        minute = (int(dt_obj.timestamp() * 1000) // 60_000) * 60_000
        buckets[minute][kind] += 1
        matched += 1

    con = sqlite3.connect(args.db)
    cur = con.cursor()
    rows_touched = 0
    for minute, c in buckets.items():
        if c["decrypt"] + c["session"] + c["tstamp"] == 0:
            continue
        existing = cur.execute(
            "SELECT 1 FROM bft_minute WHERE ts_minute = ?", (minute,)
        ).fetchone()
        if existing is None:
            cur.execute(
                "INSERT INTO bft_minute(ts_minute, rounds_total, rounds_tc, "
                "local_timeouts, decrypt_fails, session_timeouts, "
                "timestamp_invalids) VALUES (?, 0, 0, 0, ?, ?, ?)",
                (minute, c["decrypt"], c["session"], c["tstamp"]),
            )
        else:
            cur.execute(
                "UPDATE bft_minute SET "
                "decrypt_fails = MAX(decrypt_fails, ?), "
                "session_timeouts = MAX(session_timeouts, ?), "
                "timestamp_invalids = MAX(timestamp_invalids, ?) "
                "WHERE ts_minute = ?",
                (c["decrypt"], c["session"], c["tstamp"], minute),
            )
        rows_touched += 1
    con.commit()
    con.close()
    print(f"matched {matched} events across {len(buckets)} minute buckets, "
          f"rows touched: {rows_touched}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
