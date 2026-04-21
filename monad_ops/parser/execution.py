"""Parser for monad-execution log lines.

The monad-execution binary emits dense per-block summaries on the
`runloop_monad.cpp:346` logger. Each summary is a comma-separated
`__exec_block` record. Example (stripped of the log prefix):

    __exec_block,bl=26243796,id=0xafe0...cee27,ts=1776517155390,
    tx=    3,rt=   1,rtp=33.33%,sr=   73µs,txe=   653µs,cmt=   448µs,
    tot=  1232µs,tpse= 4594,tps= 2435,gas=   450276,gpse= 689,gps=365,
    ac= 1085212,sc=10000000

Field glossary:
  bl    — block number
  id    — block id (0x-hex)
  ts    — block timestamp, ms
  tx    — total transactions in block
  rt    — transactions retried (serialized after parallel conflict)
  rtp   — retry percentage (rt / tx), parallelism proxy
  sr    — state reset time, microseconds
  txe   — transaction execution time, microseconds
  cmt   — commit time, microseconds
  tot   — total block processing time, microseconds
  tpse  — effective TPS for this block
  tps   — rolling average TPS
  gas   — gas used
  gpse  — effective gas per second for this block
  gps   — rolling average gas per second
  ac    — active chunks in MonadDB
  sc    — slow chunks in MonadDB
"""

from __future__ import annotations

import re
from dataclasses import dataclass

_EXEC_BLOCK_MARKER = "__exec_block,"
# Match "<key>=<value>" where value runs until comma or end of string.
# Values can contain unit suffixes (µs, %) and leading whitespace.
_FIELD_RE = re.compile(r"([a-z]+)=\s*([^,]+?)\s*(?=,|$)")


@dataclass(frozen=True, slots=True)
class ExecBlock:
    """One parsed __exec_block record."""

    block_number: int
    block_id: str
    timestamp_ms: int
    tx_count: int
    retried: int
    retry_pct: float
    state_reset_us: int
    tx_exec_us: int
    commit_us: int
    total_us: int
    tps_effective: int
    tps_avg: int
    gas_used: int
    gas_per_sec_effective: int
    gas_per_sec_avg: int
    active_chunks: int
    slow_chunks: int

    @property
    def parallelism_ratio(self) -> float:
        """Fraction of transactions that executed in parallel (0..1)."""
        if self.tx_count == 0:
            return 1.0
        return 1.0 - (self.retried / self.tx_count)


def parse_exec_block(line: str) -> ExecBlock | None:
    """Parse a single log line; return an ExecBlock or None if it is not
    an __exec_block record.

    The ``line`` argument may include the surrounding log prefix — the
    parser locates the marker and consumes the rest.
    """
    marker = line.find(_EXEC_BLOCK_MARKER)
    if marker == -1:
        return None

    payload = line[marker + len(_EXEC_BLOCK_MARKER) :]
    fields: dict[str, str] = {}
    for key, value in _FIELD_RE.findall(payload):
        fields[key] = value

    required = {"bl", "id", "ts", "tx", "rt", "rtp", "sr", "txe", "cmt", "tot",
                "tpse", "tps", "gas", "gpse", "gps", "ac", "sc"}
    if not required.issubset(fields):
        return None

    return ExecBlock(
        block_number=int(fields["bl"]),
        block_id=fields["id"],
        timestamp_ms=int(fields["ts"]),
        tx_count=int(fields["tx"]),
        retried=int(fields["rt"]),
        retry_pct=_pct(fields["rtp"]),
        state_reset_us=_micros(fields["sr"]),
        tx_exec_us=_micros(fields["txe"]),
        commit_us=_micros(fields["cmt"]),
        total_us=_micros(fields["tot"]),
        tps_effective=int(fields["tpse"]),
        tps_avg=int(fields["tps"]),
        gas_used=int(fields["gas"]),
        gas_per_sec_effective=int(fields["gpse"]),
        gas_per_sec_avg=int(fields["gps"]),
        active_chunks=int(fields["ac"]),
        slow_chunks=int(fields["sc"]),
    )


def _pct(s: str) -> float:
    # "33.33%" -> 33.33
    return float(s.rstrip("%"))


def _micros(s: str) -> int:
    # "1232µs" -> 1232
    # The µ is a multi-byte character; strip by suffix length.
    if s.endswith("µs"):
        return int(s[:-2])
    if s.endswith("us"):
        return int(s[:-2])
    return int(s)
