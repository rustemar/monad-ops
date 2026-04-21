"""Convert ``AssertionEvent`` instances into ``AlertEvent`` instances
with appropriate severity mapping.

Dedup is handled downstream by ``DedupingSink`` — this rule only needs
to do the severity classification and craft human-readable titles.
"""

from __future__ import annotations

from monad_ops.parser import AssertionEvent, AssertionKind
from monad_ops.rules.events import AlertEvent, Severity


# Severity policy. Assertions and panics are always CRITICAL —
# monad-execution/bft don't recover from these without a restart.
# Chunk-exhaustion crosses from WARN to CRITICAL at 0.95, because
# operators still have a ~5% window to add disk / roll chunks before
# the node actually halts.
_DEFAULT_SEVERITY: dict[AssertionKind, Severity] = {
    AssertionKind.CXX_ASSERT: Severity.CRITICAL,
    AssertionKind.RUST_PANIC: Severity.CRITICAL,
    AssertionKind.QC_OVERSHOOT: Severity.CRITICAL,
    AssertionKind.GENERIC_FATAL: Severity.CRITICAL,
    # Chunk exhaustion handled specially below.
}


class AssertionRule:
    """Stateless — one input event → one alert event.

    Kept as a class for symmetry with ``StallRule`` / ``RetrySpikeRule``
    so the collector loop can dispatch uniformly.
    """

    def on_event(self, ev: AssertionEvent) -> AlertEvent:
        severity = self._severity_for(ev)
        return AlertEvent(
            rule="assertion",
            severity=severity,
            key=ev.key,
            title=self._title_for(ev),
            detail=ev.summary + (f"  at {ev.location}" if ev.location else ""),
        )

    def _severity_for(self, ev: AssertionEvent) -> Severity:
        if ev.kind is AssertionKind.CHUNK_EXHAUSTION:
            # parse the ratio out of the summary ("TrieDB chunk exhaustion: 0.9754 used, ...")
            # — simpler than threading it through the dataclass.
            ratio = _extract_ratio(ev.summary)
            if ratio is not None and ratio >= 0.95:
                return Severity.CRITICAL
            return Severity.WARN
        return _DEFAULT_SEVERITY.get(ev.kind, Severity.CRITICAL)

    def _title_for(self, ev: AssertionEvent) -> str:
        titles = {
            AssertionKind.CXX_ASSERT: "monad-execution assertion failed",
            AssertionKind.RUST_PANIC: "monad process panicked",
            AssertionKind.QC_OVERSHOOT: "consensus QC overshoot (statesync needed)",
            AssertionKind.CHUNK_EXHAUSTION: "TrieDB chunk exhaustion",
            AssertionKind.GENERIC_FATAL: "FATAL log line",
        }
        return titles.get(ev.kind, "monad incident")


def _extract_ratio(text: str) -> float | None:
    # naive float scan — works for "0.9754" style tokens in our summary format.
    import re
    m = re.search(r"(0\.\d{2,})", text)
    if not m:
        return None
    try:
        return float(m.group(1))
    except ValueError:
        return None
