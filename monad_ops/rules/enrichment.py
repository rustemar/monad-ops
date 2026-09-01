"""Receipts-enrichment health rule.

The enricher is the one data path in monad-ops that can fail without
changing anything an operator looks at. Blocks keep arriving from the
journal, the dashboard stays green, the stall and retry rules stay
quiet — and the per-tx receipt fetch against the local RPC silently
returns nothing, so contract attribution, the top-contracts tables and
the ``contract_hour`` rollup quietly go empty for as long as it lasts.
The only trace today is a ``enricher.fail`` warning line nobody greps.

Two conditions, two envelopes, because they have different causes and
different answers:

  * **Failure rate.** The RPC is answering with errors (or not
    answering). ``failed`` climbs with ``attempts`` and nothing is
    written. Usually monad-rpc itself — restarted, still replaying, or
    pointed at the wrong port.
  * **Dropped blocks.** The RPC is *slow* rather than broken, the queue
    backs up to ``enrichment.queue_size``, and ``submit`` starts
    discarding the oldest pending block to keep live writes moving.
    Those blocks are gone: nothing re-queues them, so the gap in the
    tables is permanent unless the operator backfills.

Neither is a node incident, so both are WARN — the chain is fine, our
own data is not. It is also deliberately not CRITICAL because the
dashboard's ``isCriticalIncident`` catch-all would paint the public page
red for a sidecar problem, the same reasoning as ``dual_write``.

Calibration on this node, 2026-09-01: one uninterrupted process since
2026-08-21 had **3,077,696 attempts, 0 failed, 0 dropped**, and the
queue empty at every observation. The retained journal (since 08-18)
carries exactly one ``enricher.fail`` and zero ``enricher.queue_full``.
The 2026-08-04 audit measured the busiest failure class this path has
ever produced at 0.03% of blocks, a transient RPC-side blip. So the
25% arm sits roughly 800x above the worst background ever recorded,
which is what buys the ``min_window_attempts`` gate the right to be
small: on a quiet window three failures out of five attempts is 60% and
means nothing.

The drop condition has no threshold band at all. Its baseline is not
"low", it is zero, and the queue holds ~25 minutes of blocks at testnet
cadence — reaching it takes a sustained backlog, never a blip. Same
shape as ``waltrace_flood``: nothing to flap on, so no hysteresis.

Counters are cumulative for the life of the process and the rule reads
deltas between samples. A restart resets the counters and the rule
together, so there is no cross-restart state to reconcile; the first
sample after start only establishes a baseline and never fires.

Green is slow on purpose. The failure ratio stays above the disarm line
until the bad samples age out of the rate window, so with the defaults
RECOVERED lands ``window + recovery_confirm_samples`` samples — about
10 minutes — after the RPC actually comes back. Late is the safe
direction for an all-clear that bypasses the dedup cooldown.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field

from monad_ops.rules.events import AlertEvent, Severity


@dataclass(slots=True)
class EnrichmentHealthRule:
    """Fires when the receipts enricher is failing or shedding blocks.

    ``on_sample`` returns a list because both conditions can arm on the
    same sample — a backlog that overflows while the RPC is also
    erroring is one situation, but it is two different alerts with two
    different fixes, and collapsing them would drop one.
    """

    warn_fail_pct: float = 25.0
    # Disarm band. 15 pp below the arm point, wider than the ~5 pp house
    # default because the ratio is computed over a short window and a
    # recovering RPC answers in bursts.
    disarm_fail_pct: float = 10.0
    # Arming gate. Below this many attempts in the window the ratio is
    # noise — same motivation as retry_spike's min_window_tx_avg.
    min_window_attempts: int = 20
    window: int = 5  # samples retained for the rate
    # Consecutive clean samples before either envelope closes. RECOVERED
    # bypasses the dedup sink, so without a confirmation window a single
    # good sample delivers an orphan green.
    recovery_confirm_samples: int = 5

    _last: tuple[int, int, int] | None = None  # attempts, failed, dropped
    _attempts: deque[int] = field(default_factory=deque)
    _failed: deque[int] = field(default_factory=deque)
    _fail_armed: bool = False
    _drop_armed: bool = False
    _clean_fail_samples: int = 0
    _clean_drop_samples: int = 0

    def on_sample(
        self,
        *,
        attempts: int,
        failed: int,
        dropped: int,
        queue_size: int,
    ) -> list[AlertEvent]:
        """Feed one reading of the worker's cumulative counters."""
        previous = self._last
        self._last = (attempts, failed, dropped)

        # First sight, or counters that went backwards (a re-created
        # worker behind a rule that outlived it). Re-baseline in silence.
        if (
            previous is None
            or attempts < previous[0]
            or failed < previous[1]
            or dropped < previous[2]
        ):
            self._attempts.clear()
            self._failed.clear()
            return []

        self._attempts.append(attempts - previous[0])
        self._failed.append(failed - previous[1])
        while len(self._attempts) > self.window:
            self._attempts.popleft()
            self._failed.popleft()

        events = self._evaluate_failures(queue_size)
        drop_event = self._evaluate_drops(dropped - previous[2], queue_size)
        if drop_event is not None:
            events.append(drop_event)
        return events

    def _evaluate_failures(self, queue_size: int) -> list[AlertEvent]:
        window_attempts = sum(self._attempts)
        window_failed = sum(self._failed)
        if window_attempts < self.min_window_attempts:
            # Too little traffic to judge. An idle enricher is also how a
            # healthy quiet chain looks, so this must not disarm either.
            return []

        fail_pct = 100.0 * window_failed / window_attempts

        if not self._fail_armed:
            if fail_pct < self.warn_fail_pct:
                return []
            self._fail_armed = True
            self._clean_fail_samples = 0
            return [AlertEvent(
                rule="enrichment_health",
                severity=Severity.WARN,
                key="enrichment_health:failing",
                title="Receipt enrichment is failing",
                detail=(
                    f"{window_failed} of the last {window_attempts} enrichment "
                    f"attempts failed ({fail_pct:.1f}%, threshold "
                    f"{self.warn_fail_pct:.0f}%). Queue depth {queue_size}. "
                    "Per-tx receipts are not being written, so contract "
                    "attribution and the top-contracts tables go empty for "
                    "as long as this lasts — the node itself is unaffected. "
                    "Check monad-rpc is up and answering on [node] rpc_url, "
                    "then look for enricher.fail lines in the journal for the "
                    "RPC's own error text."
                ),
            )]

        if fail_pct <= self.disarm_fail_pct:
            self._clean_fail_samples += 1
        else:
            self._clean_fail_samples = 0
        if self._clean_fail_samples < self.recovery_confirm_samples:
            return []

        self._fail_armed = False
        self._clean_fail_samples = 0
        return [AlertEvent(
            rule="enrichment_health",
            severity=Severity.RECOVERED,
            key="enrichment_health:failing",
            title="Receipt enrichment recovered",
            detail=(
                f"Enrichment failure rate back to {fail_pct:.1f}% over "
                f"{self.recovery_confirm_samples} consecutive samples. "
                "Blocks that failed while the RPC was down were not "
                "retried; their contract rows stay missing."
            ),
        )]

    def _evaluate_drops(self, dropped_delta: int, queue_size: int) -> AlertEvent | None:
        if dropped_delta > 0:
            self._clean_drop_samples = 0
            if self._drop_armed:
                return None
            self._drop_armed = True
            return AlertEvent(
                rule="enrichment_health",
                severity=Severity.WARN,
                key="enrichment_health:dropping",
                title="Receipt enrichment is dropping blocks",
                detail=(
                    f"{dropped_delta} block(s) discarded from the enrichment "
                    f"queue since the last sample; queue depth {queue_size}. "
                    "The queue is full, so the oldest pending block is shed "
                    "on every new submit to keep live writes moving. Dropped "
                    "blocks are never re-queued — their per-tx rows stay "
                    "missing. The RPC is answering too slowly to keep up "
                    "rather than erroring; if it is healthy, raise "
                    "[enrichment] queue_size."
                ),
            )

        if not self._drop_armed:
            return None
        self._clean_drop_samples += 1
        if self._clean_drop_samples < self.recovery_confirm_samples:
            return None
        self._drop_armed = False
        self._clean_drop_samples = 0
        return AlertEvent(
            rule="enrichment_health",
            severity=Severity.RECOVERED,
            key="enrichment_health:dropping",
            title="Receipt enrichment stopped dropping blocks",
            detail=(
                f"No blocks discarded for {self.recovery_confirm_samples} "
                f"consecutive samples; queue depth {queue_size}. The gap "
                "left by the dropped blocks does not fill itself."
            ),
        )
