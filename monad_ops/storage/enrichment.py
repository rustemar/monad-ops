"""Enrichment inventory queries over shared storage resources."""

from __future__ import annotations

import sqlite3
from _thread import LockType


class _EnrichmentQueryMixin:
    """Enrichment queries using the connection and lock owned by ``Storage``."""

    _conn: sqlite3.Connection
    _lock: LockType

    def tx_enrichment_count(self) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) AS n FROM tx_enrichment"
            ).fetchone()
        return int(row["n"])

    def tx_contract_block_count(self) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) AS n FROM tx_contract_block"
            ).fetchone()
        return int(row["n"])

    def contract_hour_count(self) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) AS n FROM contract_hour"
            ).fetchone()
        return int(row["n"])

    def contract_hour_range(self) -> tuple[int | None, int | None]:
        """Return (min_hour_ms, max_hour_ms) present in the rollup, or
        ``(None, None)`` if empty. Used by the rebuild task to decide
        whether a full backfill is needed."""
        with self._lock:
            row = self._conn.execute(
                "SELECT MIN(hour_ms) AS lo, MAX(hour_ms) AS hi FROM contract_hour"
            ).fetchone()
        if row is None or row["lo"] is None:
            return (None, None)
        return (int(row["lo"]), int(row["hi"]))

    def enrichment_has_block(self, block_number: int) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM tx_enrichment WHERE block_number = ? LIMIT 1",
                (int(block_number),),
            ).fetchone()
        return row is not None
