"""Background enrichment worker.

Owns a bounded ``asyncio.Queue`` of block numbers. The collector loop
calls ``submit(block_number)`` after each block is added to shared
state. The worker serially pulls block numbers off the queue and
persists per-tx receipts via ``enrich_block``.

Design decisions:
  * **Serial, not concurrent.** The local RPC is fast (<50ms per block)
    and the testnet cadence is ~2.5 blocks/sec, so a single worker
    keeps up with headroom. Concurrency would add complexity (shared
    HTTP client state, storage lock contention) without a real win.
  * **Drop oldest on overflow.** If enrichment falls behind (e.g. RPC
    hiccup), we'd rather lose the oldest backlog than stall live writes.
  * **Idempotent.** ``INSERT OR IGNORE`` on the storage side means
    re-enriching a block is harmless, which simplifies restart recovery.
"""

from __future__ import annotations

import asyncio

import structlog

from monad_ops.enricher.receipts import EnrichmentError, ReceiptsClient, enrich_block
from monad_ops.storage import Storage


log = structlog.stdlib.get_logger()


class EnrichmentWorker:
    def __init__(
        self,
        client: ReceiptsClient,
        storage: Storage,
        queue_size: int = 5000,
    ) -> None:
        self._client = client
        self._storage = storage
        self._queue: asyncio.Queue[int] = asyncio.Queue(maxsize=queue_size)
        self._attempts = 0
        self._succeeded = 0
        self._failed = 0
        self._dropped = 0

    def submit(self, block_number: int) -> None:
        """Queue a block for enrichment.

        Non-blocking: if the queue is full, we drop the oldest pending
        entry so newer (more-relevant) blocks still get enriched.
        """
        try:
            self._queue.put_nowait(block_number)
        except asyncio.QueueFull:
            try:
                dropped = self._queue.get_nowait()
                self._dropped += 1
                log.warning("enricher.queue_full", dropped=dropped, new=block_number)
            except asyncio.QueueEmpty:
                pass
            try:
                self._queue.put_nowait(block_number)
            except asyncio.QueueFull:
                self._dropped += 1

    async def run(self) -> None:
        while True:
            block_number = await self._queue.get()
            self._attempts += 1
            try:
                n = await enrich_block(self._client, self._storage, block_number)
                self._succeeded += 1
                log.debug("enricher.block", block=block_number, rows=n)
            except EnrichmentError as e:
                self._failed += 1
                log.warning("enricher.fail", block=block_number, err=str(e))
            except Exception as e:  # noqa: BLE001
                self._failed += 1
                log.error("enricher.unexpected", block=block_number, err=str(e))

    @property
    def stats(self) -> dict[str, int]:
        return {
            "queue_size": self._queue.qsize(),
            "attempts": self._attempts,
            "succeeded": self._succeeded,
            "failed": self._failed,
            "dropped": self._dropped,
        }
