"""MIP-8 dual-write liveness rule.

During the MIP-8 migration a node commits every block to both timelines
and logs one state-root line per block carrying both roots. The official
engineering runbook makes "the secondary is tracking the primary
block-for-block" the load-bearing check to run *before* the hard fork:
past it the page root becomes the consensus ``state_root``, and a node
that arrives with a secondary that quietly stopped advancing fail-stops,
with recovery only forward (hard reset, rebuild page-only).

This rule watches exactly that liveness and nothing else. It cannot see
whether the page root AGREES with the rest of the fleet — the two roots
on one line are the same state under two encodings and are expected to
differ, so the log carries no divergence signal. Read every alert here
as "dual-write logging stopped", never as "the secondary diverged".

**Counting blocks, not seconds — this is the whole design.**
The naive shape is "no dual-root line for N seconds while blocks keep
arriving". That reproduces the false positive this repo has already paid
for twice: ``tail_raw_lines`` kills its journalctl child on a 30 s idle
timeout and respawns it live-only, dropping up to that much history with
no in-stream marker. On the first block after the respawn, blocks are
advancing again while a wall clock has been accumulating across the
whole dropped interval, so the rule fires on a gap our own reader made.
A block-number delta (``block.block_number - last_dual_root_block``)
has the identical flaw — it also advances across data we never saw.

Counting *arrivals* is immune. Both line types come from one process, on
one unit, through one ``tail_raw_lines`` iterator, so nothing can remove
the dual-root line from view without removing ``__exec_block`` too. A
freeze, a restart, a rotation or a dropped interval takes both, and a
counter of received blocks cannot advance across records that never
arrived. That also makes the cold start silent for free: a fresh process
starts at zero, so "never seen one" and "stopped seeing them" are the
same code path and neither needs an armed flag.

Calibration (measured on this node, 2026-08-10, the day after phase A):
three 5-minute windows spread over 15 hours plus one 3-hour capture gave
a dual-root line for **every** ``__exec_block``, 36,170 / 36,170, with a
maximum of 1 exec block between consecutive dual-root lines and zero
block-number skips. Healthy is a hard 1. So ``warn_after_blocks = 200``
(~60 s at the post-MIP-12 3.3 blk/s) sits 200× above the observed
maximum, and there is no arm/disarm band to set: healthy is exactly 1
and sick is unbounded, so there is no boundary to flap on. Same reason
``WaltraceFloodRule`` carries no hysteresis.

WARN only, deliberately. A stalled secondary is not a 3am action — the
operator cannot repair the node's dual-write, the response is "report it
and do not upgrade yet", which is business hours. CRITICAL would also
trip the dashboard's ``isCriticalIncident`` catch-all and paint the
public page red for something that is not a node outage.

Off by default. This is a migration-window rule with a real end: after
phase C the slot timeline is decommissioned and the line stops for good,
which is indistinguishable from the failure it watches for. Nothing in
the log can tell those apart, so the switch is the operator's. Turn it
off *before* running phase C, and delete the config section afterwards
rather than leaving it off with stale docs.

The ``dual_root`` parse-drift counter on ``/api/status/errors`` runs
regardless of this switch. It is a partial backstop, and worth stating
precisely: ``drift > 0`` proves the record is still being logged while
our extraction broke, which rules the node out. But if the record were
renamed outright the marker stops matching, and then ``ok`` sits flat at
zero with no drift — indistinguishable from the node having stopped
dual-writing. That ambiguity is inherent (``drift`` documents it too),
so a WARN from this rule always needs one look at the journal before it
is believed.
"""

from __future__ import annotations

from dataclasses import dataclass

from monad_ops.parser import DualRoot, ExecBlock
from monad_ops.rules.events import AlertEvent, Severity


@dataclass(slots=True)
class DualWriteRule:
    """Fires when exec blocks keep arriving without their dual-root line."""

    enabled: bool = False
    warn_after_blocks: int = 200
    # Blocks that must arrive carrying their dual-root line before
    # RECOVERED. RECOVERED bypasses the dedup sink by design, so without
    # a confirmation window every blip delivers an orphan green.
    recovery_confirm_blocks: int = 200

    # Exec blocks received since the last dual-root line. The dual-root
    # line is emitted just before its block's __exec_block record, so
    # the steady-state value right after on_block is 1, not 0.
    _blocks_since: int = 0
    # Consecutive blocks that arrived carrying their dual-root line. Runs
    # unarmed too; only the armed branch reads it, and arming cannot be
    # reached on a stream that keeps resetting it.
    _clean_blocks: int = 0
    _state: Severity | None = None

    def on_dual_root(self, event: DualRoot) -> None:
        """Call for each parsed dual-root line. Never emits by itself."""
        if not self.enabled:
            return
        self._blocks_since = 0

    def on_block(self, block: ExecBlock) -> AlertEvent | None:
        """Call for each ExecBlock. The only place this rule emits."""
        if not self.enabled:
            return None

        self._blocks_since += 1
        # A block is clean when a dual-root line reset the counter
        # immediately before it. On the rare occasion the two lines are
        # journalled out of order the streak restarts, which only ever
        # delays RECOVERED — the safe direction.
        if self._blocks_since <= 1:
            self._clean_blocks += 1
        else:
            self._clean_blocks = 0

        if self._state is None:
            if self._blocks_since >= self.warn_after_blocks:
                self._state = Severity.WARN
                return AlertEvent(
                    rule="dual_write",
                    severity=Severity.WARN,
                    key="dual_write:warn",
                    title="MIP-8 dual-write logging stopped",
                    detail=(
                        f"{self._blocks_since} exec block(s) received since the "
                        f"last state-root line from the secondary timeline "
                        f"(healthy is 1, threshold {self.warn_after_blocks}). "
                        f"Last block seen: "
                        f"{block.block_number}. The node may have stopped writing "
                        "the page timeline, which must be tracking before the MIP-8 "
                        "hard fork. Check `monad-mpt --storage /dev/triedb` at the "
                        "next stop window. Confirm against the journal before "
                        "believing this: a renamed log record looks the same from "
                        "here. If the dual-write phase has ended, set "
                        "[rules.dual_write] enabled = false."
                    ),
                )
            return None

        if self._clean_blocks >= self.recovery_confirm_blocks:
            self._state = None
            self._clean_blocks = 0
            return AlertEvent(
                rule="dual_write",
                severity=Severity.RECOVERED,
                key="dual_write",
                title="MIP-8 dual-write logging resumed",
                detail=(
                    f"{self.recovery_confirm_blocks} consecutive block(s) carried "
                    f"a secondary state root again. Last block seen: "
                    f"{block.block_number}."
                ),
            )
        return None
