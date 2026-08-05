"""Parse-drift counters: a recognised line the extractor could not read.

The regression these guard against is the 0.14.5 one — a field vanishes
from a log record, the parser drops every line, and nothing anywhere
says so.
"""

from pathlib import Path

import pytest

from monad_ops.parser import drift, parse_consensus, parse_exec_block

EXEC_FIXTURE = Path(__file__).parent / "fixtures" / "execution_sample.log"
BFT_FIXTURE = Path(__file__).parent / "fixtures" / "bft_consensus_sample.log"
PROPOSAL_FIXTURE = Path(__file__).parent / "fixtures" / "bft_proposal_sample.log"


@pytest.fixture(autouse=True)
def _clean_counters():
    drift.reset()
    yield
    drift.reset()


def _kind(name: str) -> dict:
    return drift.snapshot()["kinds"][name]


# ---------------------------------------------------------------------------
# execution
# ---------------------------------------------------------------------------

def test_healthy_exec_line_counts_as_ok():
    line = EXEC_FIXTURE.read_text().splitlines()[0]
    assert parse_exec_block(line) is not None
    assert _kind(drift.EXEC_BLOCK) == {"ok": 1, "drift": 0, "last_drift_ms": None}


def test_exec_line_missing_required_field_counts_as_drift():
    line = EXEC_FIXTURE.read_text().splitlines()[0].replace("gas=", "gasused=")
    assert parse_exec_block(line) is None
    entry = _kind(drift.EXEC_BLOCK)
    assert entry["ok"] == 0
    assert entry["drift"] == 1
    assert entry["last_drift_ms"] > 0
    assert drift.snapshot()["total"] == 1


def test_line_without_the_marker_touches_no_counter():
    assert parse_exec_block("random log noise") is None
    assert _kind(drift.EXEC_BLOCK) == {"ok": 0, "drift": 0, "last_drift_ms": None}


def test_dropped_ac_sc_is_not_drift():
    # 0.14.5 removed both; the parser defaults them, so this must not
    # register as schema drift.
    line = EXEC_FIXTURE.read_text().splitlines()[0]
    line = line.split(",ac=")[0]
    assert parse_exec_block(line) is not None
    assert _kind(drift.EXEC_BLOCK)["drift"] == 0


# ---------------------------------------------------------------------------
# consensus
# ---------------------------------------------------------------------------

def test_healthy_advancing_round_counts_as_ok():
    line = BFT_FIXTURE.read_text().splitlines()[0]
    assert parse_consensus(line) is not None
    assert _kind(drift.CONSENSUS_ADVANCING_ROUND)["ok"] == 1


def test_renamed_certificate_field_counts_as_drift():
    line = BFT_FIXTURE.read_text().splitlines()[0].replace("info: Vote", "info: Ballot")
    assert parse_consensus(line) is None
    assert _kind(drift.CONSENSUS_ADVANCING_ROUND)["drift"] == 1


def test_local_timeout_missing_next_leader_counts_as_drift():
    line = next(
        line for line in BFT_FIXTURE.read_text().splitlines()
        if '"local timeout"' in line
    )
    healthy = parse_consensus(line)
    assert healthy is not None
    assert _kind(drift.CONSENSUS_LOCAL_TIMEOUT)["ok"] == 1

    assert parse_consensus(line.replace('"next_leader"', '"following_leader"')) is None
    assert _kind(drift.CONSENSUS_LOCAL_TIMEOUT)["drift"] == 1


def test_proposal_with_moved_seq_num_counts_as_drift():
    line = PROPOSAL_FIXTURE.read_text().splitlines()[0]
    assert parse_consensus(line) is not None
    assert _kind(drift.CONSENSUS_PROPOSAL)["ok"] == 1

    assert parse_consensus(line.replace("seq_num:", "sequence:")) is None
    assert _kind(drift.CONSENSUS_PROPOSAL)["drift"] == 1


def test_keepalive_firehose_touches_no_counter():
    line = (
        '{"timestamp":"2026-08-05T06:00:00.000000Z","level":"DEBUG","fields":'
        '{"message":"sending keepalive packet"},"target":"monad_raptorcast"}'
    )
    assert parse_consensus(line) is None
    assert drift.snapshot()["total"] == 0
    assert all(entry["ok"] == 0 for entry in drift.snapshot()["kinds"].values())


# ---------------------------------------------------------------------------
# snapshot shape
# ---------------------------------------------------------------------------

def test_snapshot_declares_every_kind_at_zero():
    snap = drift.snapshot()
    assert snap["total"] == 0
    assert set(snap["kinds"]) == set(drift.KINDS)
    for entry in snap["kinds"].values():
        assert entry == {"ok": 0, "drift": 0, "last_drift_ms": None}


def test_drift_timestamp_comes_from_the_injected_clock():
    drift.record_drift(drift.EXEC_BLOCK, "line", now=lambda: 1_780_000_000.5)
    assert _kind(drift.EXEC_BLOCK)["last_drift_ms"] == 1_780_000_000_500
