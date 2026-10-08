"""Persisted metadata and maintenance windows over shared storage resources."""

from __future__ import annotations

import sqlite3
import time
from _thread import LockType


class _MetadataMixin:
    """Metadata operations using the connection and lock owned by ``Storage``."""

    _conn: sqlite3.Connection
    _lock: LockType

    def get_meta(self, key: str) -> str | None:
        """Read a meta-table value, or None if the key isn't set."""
        with self._lock:
            row = self._conn.execute(
                "SELECT value FROM meta WHERE key = ?", (key,)
            ).fetchone()
        return None if row is None else str(row["value"])

    def _meta_in_txn(self, key: str) -> str | None:
        row = self._conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        return None if row is None else str(row["value"])

    def _put_meta_in_txn(self, key: str, value: str) -> None:
        self._conn.execute(
            "INSERT OR REPLACE INTO meta(key, value, updated_ts) VALUES (?,?,?)",
            (key, value, int(time.time())),
        )

    def maintenance_window(self) -> tuple[float | None, float | None]:
        """(since, until) of the alert-delivery maintenance window, epoch seconds."""
        from monad_ops.alerts.sink import (
            MAINTENANCE_SINCE_KEY,
            MAINTENANCE_UNTIL_KEY,
            parse_maintenance_ts,
        )
        with self._lock:
            since = self._meta_in_txn(MAINTENANCE_SINCE_KEY)
            until = self._meta_in_txn(MAINTENANCE_UNTIL_KEY)
        return parse_maintenance_ts(since), parse_maintenance_ts(until)

    def open_maintenance(self, until_sec: float, now_sec: float | None = None) -> None:
        """Open (or extend) the window in one transaction. ``since`` is kept
        while a window is open so the summary covers the whole stretch, and
        started afresh once the stored ``until`` has passed. The service's
        ``take_closed_window`` runs in its own IMMEDIATE transaction, so the
        two never interleave half-way."""
        from monad_ops.alerts.sink import (
            MAINTENANCE_SINCE_KEY,
            MAINTENANCE_UNTIL_KEY,
            parse_maintenance_ts,
        )
        now = time.time() if now_sec is None else now_sec
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                since = parse_maintenance_ts(self._meta_in_txn(MAINTENANCE_SINCE_KEY))
                until = parse_maintenance_ts(self._meta_in_txn(MAINTENANCE_UNTIL_KEY))
                # Full precision, no rounding: a row recorded right after the
                # open (or right before `--off`) must fall inside [since, until].
                self._put_meta_in_txn(MAINTENANCE_UNTIL_KEY, repr(float(until_sec)))
                if since is None or until is None or until <= now:
                    self._put_meta_in_txn(MAINTENANCE_SINCE_KEY, repr(float(now)))
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    def take_closed_window(self, now_sec: float) -> tuple[float, float] | None:
        """Claim a window that has ended: clear ``since`` and return the bounds,
        or None when no closed, unsummarised window exists. One transaction, so
        only one caller ever gets a given window."""
        from monad_ops.alerts.sink import (
            MAINTENANCE_SINCE_KEY,
            MAINTENANCE_UNTIL_KEY,
            parse_maintenance_ts,
        )
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                since = parse_maintenance_ts(self._meta_in_txn(MAINTENANCE_SINCE_KEY))
                until = parse_maintenance_ts(self._meta_in_txn(MAINTENANCE_UNTIL_KEY))
                if since is None or until is None or until > now_sec:
                    self._conn.execute("COMMIT")
                    return None
                self._put_meta_in_txn(MAINTENANCE_SINCE_KEY, "0")
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
        return since, until

    def put_meta(self, key: str, value: str) -> None:
        """Upsert a meta-table value with current wall-clock ts."""
        ts = int(time.time())
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO meta(key, value, updated_ts) VALUES (?,?,?)",
                (key, value, ts),
            )
            self._conn.commit()
