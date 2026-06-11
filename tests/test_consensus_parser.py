"""Tests for the monad-bft consensus event parser.

The fixture file ``bft_consensus_sample.log`` was captured from a live
testnet node on 2026-04-24 06:00:00–06:00:30 UTC: a 30-second window
that happened to contain one TC (round 27597178 timed out chain-wide)
and one corresponding local-pacemaker fire on the same round, in
addition to 72 normal QC advances. Keeping the fixture verbatim from
real journal output guards against regex drift if monad-bft tweaks
its log schema in a future release.
"""

from __future__ import annotations

from pathlib import Path

from monad_ops.parser import (
    ConsensusEvent,
    ConsensusEventKind,
    parse_consensus,
)

FIXTURE = Path(__file__).parent / "fixtures" / "bft_consensus_sample.log"


def _load_fixture() -> list[str]:
    return FIXTURE.read_text().splitlines()


def test_parses_qc_round_advance() -> None:
    """A normal QC-decided round closure carries epoch + round."""
    lines = _load_fixture()
    qc_lines = [l for l in lines if '"advancing round","certificate":"Qc' in l]
    assert qc_lines, "fixture has no Qc-certificate lines — fixture corrupted"

    ev = parse_consensus(qc_lines[0])
    assert ev is not None
    assert ev.kind is ConsensusEventKind.ROUND_ADVANCE_QC
    assert ev.epoch == 549
    assert ev.round > 0
    assert ev.ts_ms > 0
    assert ev.leader is None
    assert ev.next_leader is None


def test_parses_tc_round_advance() -> None:
    """A TC-decided round closure is the chain-wide timeout signature.

    Round 27597178 epoch 549 timed out at 06:00:09.820Z in the captured
    window — verified against the source fixture.
    """
    lines = _load_fixture()
    tc_lines = [l for l in lines if '"advancing round","certificate":"Tc' in l]
    assert len(tc_lines) == 1, "fixture should have exactly one TC line"

    ev = parse_consensus(tc_lines[0])
    assert ev is not None
    assert ev.kind is ConsensusEventKind.ROUND_ADVANCE_TC
    assert ev.epoch == 549
    assert ev.round == 27597178


def test_parses_local_timeout() -> None:
    """Local pacemaker fire carries round + leader + next_leader,
    but NOT epoch (the message simply doesn't include it)."""
    lines = _load_fixture()
    lt_lines = [l for l in lines if '"local timeout"' in l]
    assert len(lt_lines) == 1, "fixture should have exactly one local-timeout line"

    ev = parse_consensus(lt_lines[0])
    assert ev is not None
    assert ev.kind is ConsensusEventKind.LOCAL_TIMEOUT
    assert ev.round == 27597178
    assert ev.epoch is None
    assert ev.leader == "03d95b80275720d1c93d9e83039535f58bda3acc4f4a8b7ae9a958b1ea85a86dce"
    assert ev.next_leader == "02dffcb8ecaffd9eaf85c13b10c9c8ce7d1b874543c3661daeccb34e54339a7a54"


def test_parses_proposal_with_base_fee() -> None:
    """Proposal messages carry seq_num + base_fee for the fee curve.
    Captured from a live testnet node 2026-04-24 06:29Z (the testnet
    has been quiescent for weeks at base_fee=100 gwei = 100000000000 wei,
    which is the floor and should look uniform on the chart)."""
    fixture = (
        Path(__file__).parent / "fixtures" / "bft_proposal_sample.log"
    ).read_text().splitlines()
    assert fixture, "bft_proposal_sample.log fixture is empty"

    ev = parse_consensus(fixture[0])
    assert ev is not None
    assert ev.kind is ConsensusEventKind.PROPOSAL
    assert ev.block_seq == 27_470_642
    assert ev.base_fee == 100_000_000_000   # 100 gwei × 1e9
    assert ev.round > 0                      # proposal_round captured
    assert ev.ts_ms > 0


def test_proposal_without_base_fee_is_skipped() -> None:
    """A "dropping proposal, already received" sibling line lacks the
    base_fee field — the cheap pre-filter must reject it before the
    regex even fires (otherwise we'd waste regex work on every dedup
    hit, which fires roughly once per real proposal)."""
    drop = (
        '{"timestamp":"2026-04-24T06:45:09.520280Z","level":"DEBUG",'
        '"fields":{"message":"dropping proposal, already received for this round",'
        '"proposal_round":"27603878"},"target":"monad_consensus_state"}'
    )
    assert parse_consensus(drop) is None


def test_returns_none_for_keepalive_line() -> None:
    """The vast majority of monad-bft log lines are keepalive packets
    that must be cheaply rejected without firing any regex."""
    keepalive = (
        '{"timestamp":"2026-04-24T06:45:09.382662Z","level":"DEBUG",'
        '"fields":{"message":"sending keepalive packet",'
        '"duration_since_start":"144757.857210112s",'
        '"remote_addr":"84.46.214.13:8001"},'
        '"target":"monad_wireauth::session::transport"}'
    )
    assert parse_consensus(keepalive) is None


def test_returns_none_for_unrelated_consensus_message() -> None:
    """Many DEBUG lines mention consensus state but aren't the two
    signals we surface — leader-for-round, vote-successful, etc."""
    leader_for_round = (
        '{"timestamp":"2026-04-24T06:45:09.438077Z","level":"DEBUG",'
        '"fields":{"message":"leader for round","round":"27603878",'
        '"round_leader":"Some(02624d6561f75564b013f3f833834b3ebe554afd43bb81340dabd1bdd1603efba1)"},'
        '"target":"monad_consensus_state"}'
    )
    assert parse_consensus(leader_for_round) is None


def test_proposal_with_last_round_tc_does_not_double_count() -> None:
    """A ``proposal message`` whose ``last_round_tc: Some(...)`` field
    reflects a prior TC must NOT be mistaken for the TC's own
    ``advancing round`` line — otherwise we'd double-count timeouts."""
    proposal_with_tc = (
        '{"timestamp":"2026-04-24T06:00:09.869105Z","level":"DEBUG",'
        '"fields":{"message":"proposal message","author":"02dffcb8...",'
        '"proposal":"ProposalMessage { proposal_round: 27597179, '
        'proposal_epoch: 549, ... last_round_tc: Some(TimeoutCertificate '
        '{ epoch: 549, round: 27597178, ... }) }",'
        '"block_id":"32d8..0b9e"},"target":"monad_consensus_state"}'
    )
    assert parse_consensus(proposal_with_tc) is None


def test_returns_none_for_empty_line() -> None:
    assert parse_consensus("") is None


def test_extracted_ts_ms_is_monotonic_in_fixture() -> None:
    """Sanity: parsing the whole fixture in order yields ts_ms that
    never goes backwards — guards against ts extraction silently
    pulling the wrong field."""
    parsed = [parse_consensus(l) for l in _load_fixture()]
    parsed = [p for p in parsed if p is not None]
    assert len(parsed) >= 70, "fixture should yield at least 70 events"
    timestamps = [p.ts_ms for p in parsed]
    assert timestamps == sorted(timestamps)
    assert all(t > 0 for t in timestamps)


def test_parses_network_decrypt_fail() -> None:
    """RaptorCast UDP-auth decrypt-fail line surfaces as
    NETWORK_DECRYPT_FAIL with ts_ms + peer populated; round/epoch unset."""
    line = (
        '{"timestamp":"2026-05-03T11:43:41.179560Z","level":"DEBUG",'
        '"fields":{"message":"failed to decrypt message",'
        '"addr":"23.83.186.216:8001","error":"SessionIndexNotFound { index: 41143 }"},'
        '"target":"monad_raptorcast::auth::socket"}'
    )
    ev = parse_consensus(line)
    assert ev is not None
    assert ev.kind is ConsensusEventKind.NETWORK_DECRYPT_FAIL
    assert ev.ts_ms > 0
    assert ev.epoch is None
    assert ev.peer == "23.83.186.216:8001"


def test_parses_network_session_timeout() -> None:
    """wireauth session-timeout-expired line surfaces as
    NETWORK_SESSION_TIMEOUT."""
    line = (
        '{"timestamp":"2026-05-03T11:43:41.677330Z","level":"DEBUG",'
        '"fields":{"message":"session timeout expired",'
        '"remote_addr":"<ip>"},'
        '"target":"monad_wireauth::session::transport"}'
    )
    ev = parse_consensus(line)
    assert ev is not None
    assert ev.kind is ConsensusEventKind.NETWORK_SESSION_TIMEOUT
    assert ev.ts_ms > 0


def test_parses_network_timestamp_invalid() -> None:
    """consensus_state Timestamp-validation-failed line surfaces as
    NETWORK_TIMESTAMP_INVALID. WARN-level, distinct from the DEBUG
    network-stack events above."""
    line = (
        '{"timestamp":"2026-05-03T07:33:14.577673Z","level":"WARN",'
        '"fields":{"message":"Timestamp validation failed",'
        '"prev_block_ts":"1777793594035849935",'
        '"curr_block_ts":"1777793595654058190",'
        '"local_ts":"1777793594573060313"},'
        '"target":"monad_consensus_state"}'
    )
    ev = parse_consensus(line)
    assert ev is not None
    assert ev.kind is ConsensusEventKind.NETWORK_TIMESTAMP_INVALID
    assert ev.ts_ms > 0


def test_network_event_substrings_dont_match_unrelated_lines() -> None:
    """The bare phrase 'session timeout' shouldn't match if it appears
    in some other field's value (e.g. an error string). Markers are
    fully qualified on `"message":"<phrase>"` to avoid that."""
    # Nested `session timeout` mention inside a Debug repr — should NOT
    # be classified as a network event.
    line = (
        '{"timestamp":"2026-05-03T11:43:41Z","level":"DEBUG",'
        '"fields":{"message":"some other event",'
        '"detail":"caused by session timeout expired earlier"},'
        '"target":"some_target"}'
    )
    assert parse_consensus(line) is None


def test_full_fixture_yields_expected_distribution() -> None:
    """End-to-end: count of TC vs QC vs local timeout matches what we
    captured. If this breaks, either the regex drifted or the fixture
    file got corrupted."""
    parsed = [parse_consensus(l) for l in _load_fixture()]
    parsed = [p for p in parsed if p is not None]

    qc = sum(1 for p in parsed if p.kind is ConsensusEventKind.ROUND_ADVANCE_QC)
    tc = sum(1 for p in parsed if p.kind is ConsensusEventKind.ROUND_ADVANCE_TC)
    lt = sum(1 for p in parsed if p.kind is ConsensusEventKind.LOCAL_TIMEOUT)

    assert qc == 72
    assert tc == 1
    assert lt == 1


def test_parses_waltrace_stopped() -> None:
    """v0.14.5 WAL-thread death spam surfaces as WALTRACE_STOPPED with
    ts_ms populated. Line captured live 2026-06-11 on this node."""
    line = (
        '{"timestamp":"2026-06-11T09:34:00.003336Z","level":"ERROR",'
        '"fields":{"message":"waltrace thread stopped"},'
        '"target":"monad_node"}'
    )
    ev = parse_consensus(line)
    assert ev is not None
    assert ev.kind is ConsensusEventKind.WALTRACE_STOPPED
    assert ev.ts_ms > 0
    assert ev.epoch is None


def test_waltrace_phrase_inside_other_field_is_ignored() -> None:
    """The marker anchors on "message":"..." so the phrase embedded in
    an arbitrary detail string does not classify."""
    line = (
        '{"timestamp":"2026-06-11T09:34:00.003336Z","level":"INFO",'
        '"fields":{"message":"something else",'
        '"detail":"recovered after waltrace thread stopped earlier"},'
        '"target":"monad_node"}'
    )
    assert parse_consensus(line) is None
