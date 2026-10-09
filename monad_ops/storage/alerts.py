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
