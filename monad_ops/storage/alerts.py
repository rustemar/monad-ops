"""Alert recording and envelope queries over shared storage resources."""

from __future__ import annotations

import sqlite3
import time
from _thread import LockType

from monad_ops.rules.events import AlertEvent


class _AlertEnvelopeMixin:
    """Alert methods using the connection and lock owned by ``Storage``."""

    _conn: sqlite3.Connection
    _lock: LockType

    def write_alert(self, a: AlertEvent, ts: float | None = None) -> None:
        with self._lock:
            self._conn.execute(
                """INSERT INTO alerts (ts, rule, severity, key, title, detail)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    ts if ts is not None else time.time(),
                    a.rule, a.severity.value, a.key, a.title, a.detail,
                ),
            )

    def recovered_envelopes(self) -> set[str]:
        """Alert keys that have ever closed with RECOVERED — the envelope-shaped ones."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT DISTINCT key FROM alerts WHERE severity = 'recovered'"
            ).fetchall()
        return {str(r["key"]) for r in rows}

    def last_severity_before(self, envelope: str, ts_sec: float) -> str | None:
        """Severity of the envelope's last row before ``ts_sec``, if any."""
        with self._lock:
            row = self._conn.execute(
                "SELECT severity FROM alerts WHERE ts < ? AND key IN (?, ?, ?) "
                "ORDER BY ts DESC, id DESC LIMIT 1",
                (ts_sec, envelope, f"{envelope}:warn", f"{envelope}:critical"),
            ).fetchone()
        return None if row is None else str(row["severity"])

    def alerts_between(
        self, since_sec: float, until_sec: float, *, before: float | None = None
    ) -> list[tuple[float, str, str, str]]:
        """(ts, rule, severity, key) for alerts recorded inside a window, oldest
        first; ``before`` excludes rows stamped at or after it."""
        sql = "SELECT ts, rule, severity, key FROM alerts WHERE ts >= ? AND ts <= ?"
        args: list[float] = [since_sec, until_sec]
        if before is not None:
            sql += " AND ts < ?"
            args.append(before)
        with self._lock:
            rows = self._conn.execute(sql + " ORDER BY ts, id", args).fetchall()
        return [(float(r["ts"]), str(r["rule"]), str(r["severity"]), str(r["key"])) for r in rows]

    def list_stress_envelopes(
        self,
        *,
        limit: int = 5,
        max_age_days: int = 30,
        merge_gap_sec: float = 1800.0,
    ) -> list[dict]:
        """Group retry_spike alerts into stress-event envelopes.

        Each envelope is a contiguous run of CRITICAL alerts terminated
        by the next RECOVERED — i.e. one closed retry_spike incident.
        Adjacent envelopes whose gap (recovered_ts → next critical_ts)
        is below ``merge_gap_sec`` are merged into one — covers the
        case of retry_pct briefly dipping below the recovery threshold
        mid-stress, producing 30 micro-flaps that semantically are one
        event. Default 30 min comfortably covers within-batch dips
        (observed up to 19 min on 2026-04-20) without merging real
        between-batch gaps (observed 2.4–4.5 h).

        A trailing CRITICAL with no matching RECOVERED is returned as
        ``status="live"`` with ``to_ts_ms=None`` — caller substitutes
        wall-clock ``now`` so the button targets the *current* window.
        Closed envelopes are ``status="past"``.

        Returns up to ``limit`` envelopes, newest first. Each entry:
            {from_ts_ms, to_ts_ms|None, status, critical_count}
        """
        cutoff = time.time() - max_age_days * 86400.0
        sql = (
            "SELECT ts, severity FROM alerts "
            "WHERE rule = 'retry_spike' AND ts >= ? "
            "AND severity IN ('critical', 'recovered') "
            "ORDER BY ts ASC"
        )
        with self._lock:
            rows = self._conn.execute(sql, (cutoff,)).fetchall()

        envelopes: list[dict] = []
        cur_start: float | None = None
        cur_count = 0
        for r in rows:
            sev = r["severity"]
            ts = float(r["ts"])
            if sev == "critical":
                if cur_start is None:
                    cur_start = ts
                cur_count += 1
            elif sev == "recovered" and cur_start is not None:
                envelopes.append({
                    "from_ts_ms": int(cur_start * 1000),
                    "to_ts_ms": int(ts * 1000),
                    "status": "past",
                    "critical_count": cur_count,
                })
                cur_start = None
                cur_count = 0
        # Trailing live envelope (CRITICAL armed, no RECOVERED yet).
        if cur_start is not None:
            envelopes.append({
                "from_ts_ms": int(cur_start * 1000),
                "to_ts_ms": None,
                "status": "live",
                "critical_count": cur_count,
            })

        # Merge envelopes whose gap is below the threshold. A live
        # envelope can only ever be the last one (no envelope follows
        # an open one), so merging just extends prev → live cleanly.
        merged: list[dict] = []
        for e in envelopes:
            if merged:
                prev = merged[-1]
                prev_end = (
                    prev["to_ts_ms"] if prev["to_ts_ms"] is not None else int(time.time() * 1000)
                )
                gap_ms = e["from_ts_ms"] - prev_end
                if gap_ms < merge_gap_sec * 1000:
                    prev["to_ts_ms"] = e["to_ts_ms"]
                    prev["critical_count"] += e["critical_count"]
                    if e["status"] == "live":
                        prev["status"] = "live"
                    continue
            merged.append(dict(e))

        # Newest first, capped.
        merged.reverse()
        return merged[: max(1, min(int(limit), 50))]
