"""Proposer lookups and reorg attribution over shared storage resources."""

from __future__ import annotations

import sqlite3
from _thread import LockType
from collections.abc import Iterable


class _ProposerQueryMixin:
    """Proposer queries using the connection and lock owned by ``Storage``."""

    _conn: sqlite3.Connection
    _lock: LockType

    def get_proposers(self, block_seqs: Iterable[int]) -> dict[int, str]:
        """Map block_seq → proposer secp key for the blocks we saw proposed.

        Deliberately a lookup rather than a column on ``list_bft_base_fee``:
        the author is 66 characters and that reader feeds a 5000-row chart
        payload, where it would be pure weight. The question this answers
        is per-block ("who proposed the block that got reorged"), so the
        access shape is a lookup.

        Blocks with no row, or rows written before the author column
        existed, are simply absent from the result.
        """
        seqs = [int(s) for s in block_seqs]
        if not seqs:
            return {}
        out: dict[int, str] = {}
        with self._lock:
            # Chunked so a large reorg window cannot exceed SQLite's
            # variable limit (999 by default).
            for i in range(0, len(seqs), 500):
                chunk = seqs[i:i + 500]
                placeholders = ",".join("?" * len(chunk))
                rows = self._conn.execute(
                    f"""SELECT block_seq, author FROM bft_base_fee
                        WHERE author IS NOT NULL
                          AND block_seq IN ({placeholders})""",
                    chunk,
                ).fetchall()
                for r in rows:
                    out[int(r["block_seq"])] = str(r["author"])
        return out

    def reorg_proposer_stats(self) -> dict:
        """Do observed reorgs concentrate on particular proposers?

        Joins every reorg alert to the author recorded for that block and
        weighs each proposer's reorg count against the share of blocks
        they actually proposed. Without that denominator the answer is
        meaningless — a validator proposing 5% of blocks should collect
        5% of reorgs.

        ``collision`` is ``sum(share_i^2)``: the chance two independently
        chosen blocks share a proposer. It is the null hypothesis to
        judge a run of same-proposer reorgs against.

        Internal analysis only — no endpoint exposes this. A proposer key
        is public on-chain, but a published "these validators cause
        reorgs" table is an accusation, and the counts here are small
        enough to mislead.
        """
        with self._lock:
            shares = self._conn.execute(
                """SELECT author, COUNT(*) AS n FROM bft_base_fee
                   WHERE author IS NOT NULL GROUP BY author"""
            ).fetchall()
            keys = self._conn.execute(
                "SELECT key FROM alerts WHERE rule = 'reorg'"
            ).fetchall()

        total_blocks = sum(r["n"] for r in shares)
        if not total_blocks:
            return {
                "window_blocks": 0, "proposers": 0, "collision": 0.0,
                "reorgs_total": 0, "reorgs_attributed": 0, "by_proposer": [],
            }
        share = {r["author"]: r["n"] / total_blocks for r in shares}
        blocks = {r["author"]: r["n"] for r in shares}

        # Reorg alert keys are "reorg:<block_number>:<new_id>".
        numbers: list[int] = []
        for (key,) in ((r["key"],) for r in keys):
            parts = (key or "").split(":", 2)
            if len(parts) >= 3 and parts[1].isdigit():
                numbers.append(int(parts[1]))

        attributed = self.get_proposers(numbers)
        counts: dict[str, int] = {}
        for author in attributed.values():
            counts[author] = counts.get(author, 0) + 1

        by_proposer = [
            {
                "author": a,
                "reorgs": c,
                "blocks_proposed": blocks.get(a, 0),
                "share": share.get(a, 0.0),
                # What this proposer "should" have collected if reorgs
                # were independent of who proposed the block.
                "expected": len(attributed) * share.get(a, 0.0),
            }
            for a, c in sorted(counts.items(), key=lambda kv: -kv[1])
        ]
        return {
            "window_blocks": total_blocks,
            "proposers": len(share),
            "collision": sum(s * s for s in share.values()),
            "reorgs_total": len(numbers),
            "reorgs_attributed": len(attributed),
            "by_proposer": by_proposer,
        }
