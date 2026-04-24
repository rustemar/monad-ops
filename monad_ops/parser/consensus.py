"""Parser for monad-bft consensus log lines.

Two signals matter for the validator-timeouts metric Foundation tracks
(Abraar 2026-04-20: ``<3% validator timeouts``):

1. **Chain-wide round-decision certificate.** Every ``advancing round``
   line carries a ``certificate`` field whose value is a Rust Debug
   repr starting with either ``Qc(QuorumCertificate {...})`` or
   ``Tc(TimeoutCertificate {...})``. A TC means the round closed via
   2f+1 timeout votes instead of 2f+1 commit votes — the canonical
   chain-wide timeout signature.

2. **Local pacemaker timer fire.** ``local timeout`` fires when *this*
   node's pacemaker timer expired waiting for a proposal at a round.
   Strictly an operator-side signal (a single node's view), and at
   healthy steady-state hovers near the chain-wide TC rate (because
   when the network times out, every honest node's local timer also
   fires). Surfaced separately so an operator can spot the case where
   their node's timer is firing but the chain isn't actually timing
   out — usually a local clock or peering issue.

Volume note: the monad-bft journal is dominated by ``sending keepalive
packet`` DEBUG lines (one per peer per few seconds, 200 peers in the
active validator set as of 2026-04-23 = ~5500 lines/sec). The two
signals we care about are ~2.6/sec combined. We do a cheap substring
pre-filter at the top of ``parse_consensus`` so the hot path skips the
keepalive firehose without paying the regex cost.

We deliberately avoid json.loads — the values of ``certificate`` and
``advance_round_msg`` are Rust Debug reprs containing nested braces,
brackets, BLS bitvecs and quotes, which are not valid JSON inside a
JSON envelope. Direct regex on the raw line is robust and ~10x faster
than parse-then-extract.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum


class ConsensusEventKind(StrEnum):
    """Classes of consensus log line we surface."""
    ROUND_ADVANCE_QC = "round_advance_qc"   # round closed by QuorumCertificate (normal)
    ROUND_ADVANCE_TC = "round_advance_tc"   # round closed by TimeoutCertificate (chain-wide timeout)
    LOCAL_TIMEOUT = "local_timeout"         # this node's pacemaker fired
    PROPOSAL = "proposal"                    # proposal message with base_fee for the fee curve


@dataclass(frozen=True, slots=True)
class ConsensusEvent:
    """One parsed consensus event.

    ``epoch`` is None for ``LOCAL_TIMEOUT`` and ``PROPOSAL`` (those
    messages don't carry epoch directly). All other fields are best-
    effort: extraction failures fall back to 0/None rather than raising,
    because the dashboard tolerates occasional gaps far better than the
    tailer dying on a single malformed line.
    """
    kind: ConsensusEventKind
    round: int
    epoch: int | None
    ts_ms: int
    leader: str | None = None        # populated for LOCAL_TIMEOUT
    next_leader: str | None = None   # populated for LOCAL_TIMEOUT
    block_seq: int | None = None     # populated for PROPOSAL (= block_number)
    base_fee: int | None = None      # populated for PROPOSAL, in wei


# ── pre-filter substrings ─────────────────────────────────────────────
# Cheapest possible reject for the keepalive firehose. Both markers
# include the surrounding quotes from the JSON envelope so we don't
# accidentally match the literal phrases inside arbitrary Debug reprs.
_LOCAL_TIMEOUT_MARKER = '"local timeout"'
_ADVANCING_ROUND_MARKER = '"advancing round"'
# A live proposal carries `seq_num: NNN, ..., base_fee: NNN` inside
# the ``proposal`` field's Rust Debug repr. Pre-filter on the message
# string PLUS the base_fee literal so the cheaper "dropping proposal,
# already received for this round" sibling lines (which lack base_fee)
# don't even pay the regex cost.
_PROPOSAL_MARKER = '"proposal message"'
_BASE_FEE_MARKER = "base_fee:"

# ── extraction patterns ───────────────────────────────────────────────
# ISO-8601 timestamp at start of the JSON envelope.
_TS_RX = re.compile(r'"timestamp":"([^"]+)"')

# local timeout fields. round is quoted in the JSON envelope.
_LOCAL_TIMEOUT_RX = re.compile(
    r'"local timeout".*?"round":"(\d+)".*?"leader":"([0-9a-f]+)".*?"next_leader":"([0-9a-f]+)"'
)

# advancing round + certificate prefix (Qc or Tc). The kind is
# determined by the OUTER prefix; nested Qc inside a Tc (TimeoutCert
# carries the high_extend QC) does not flip the classification.
_ADVANCING_ROUND_QC_RX = re.compile(
    r'"advancing round","certificate":"Qc\(QC \{ info: Vote \{ id: [0-9a-f.]+, epoch: (\d+), round: (\d+) \}'
)
_ADVANCING_ROUND_TC_RX = re.compile(
    r'"advancing round","certificate":"Tc\(TimeoutCertificate \{ epoch: (\d+), round: (\d+),'
)

# proposal message — extract seq_num + base_fee from inside the Debug
# repr of ProposalMessage. We anchor on `seq_num: <digits>, ...,
# base_fee: <digits>` because both are sibling fields on the same
# ConsensusBlockHeader and appear in that order in monad-bft 0.14.x.
# proposal_round is captured for completeness so the snapshot can
# correlate fee changes with consensus rounds without a second parse.
_PROPOSAL_RX = re.compile(
    r'"proposal message".*?proposal_round: (\d+).*?'
    r'seq_num: (\d+),\s*timestamp_ns: \d+,\s*id: [0-9a-f.]+,\s*base_fee: (\d+)'
)


def parse_consensus(line: str) -> ConsensusEvent | None:
    """Inspect one monad-bft journal line; return a ConsensusEvent or None.

    Cheap substring filter first — returns immediately for the ~99.95%
    of lines that aren't consensus-state events.
    """
    if not line:
        return None

    has_local_timeout = _LOCAL_TIMEOUT_MARKER in line
    has_advancing_round = _ADVANCING_ROUND_MARKER in line
    has_proposal = _PROPOSAL_MARKER in line and _BASE_FEE_MARKER in line
    if not (has_local_timeout or has_advancing_round or has_proposal):
        return None

    ts_ms = _extract_ts_ms(line)

    if has_local_timeout:
        m = _LOCAL_TIMEOUT_RX.search(line)
        if m is None:
            return None
        return ConsensusEvent(
            kind=ConsensusEventKind.LOCAL_TIMEOUT,
            round=int(m.group(1)),
            epoch=None,
            ts_ms=ts_ms,
            leader=m.group(2),
            next_leader=m.group(3),
        )

    # advancing round: try TC first (rarer, more specific prefix).
    m = _ADVANCING_ROUND_TC_RX.search(line)
    if m is not None:
        return ConsensusEvent(
            kind=ConsensusEventKind.ROUND_ADVANCE_TC,
            round=int(m.group(2)),
            epoch=int(m.group(1)),
            ts_ms=ts_ms,
        )

    m = _ADVANCING_ROUND_QC_RX.search(line)
    if m is not None:
        return ConsensusEvent(
            kind=ConsensusEventKind.ROUND_ADVANCE_QC,
            round=int(m.group(2)),
            epoch=int(m.group(1)),
            ts_ms=ts_ms,
        )

    if has_proposal:
        m = _PROPOSAL_RX.search(line)
        if m is not None:
            return ConsensusEvent(
                kind=ConsensusEventKind.PROPOSAL,
                round=int(m.group(1)),
                epoch=None,
                ts_ms=ts_ms,
                block_seq=int(m.group(2)),
                base_fee=int(m.group(3)),
            )

    # Marker matched but neither extractor did — schema drift (e.g.
    # monad-bft renamed a field). Skip rather than raise; the caller
    # tails forever and would be fragile if we threw.
    return None


def _extract_ts_ms(line: str) -> int:
    """Parse the ISO-8601 timestamp at the start of the JSON envelope.

    Falls back to 0 on parse failure — the rollup uses ts_ms only for
    minute-bucketing, and a 0 timestamp falls into the 1970 bucket which
    the rollup writer already filters as obviously bogus.
    """
    m = _TS_RX.search(line)
    if m is None:
        return 0
    raw = m.group(1)
    try:
        # Replace trailing Z with +00:00 — fromisoformat accepts both
        # only on Python 3.11+; on 3.12 (this host) the explicit form
        # is unambiguous regardless.
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        return int(datetime.fromisoformat(raw).timestamp() * 1000)
    except ValueError:
        return 0
