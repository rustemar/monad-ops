"""Tests for the ``__exec_block`` parser.

Two fixtures on purpose, captured from real nodes two releases apart:

* ``execution_sample_0160.log`` — the release this node runs today.
* ``execution_sample_0142.log`` — an April capture, kept because its
  format is genuinely dead (``runloop_monad.cpp:346``, ``ac=``/``sc=``
  still present, ``µs`` suffixes). A parser that only handles the
  current shape is one release away from the 0.14.5 outage, so both
  must keep parsing.

The pair is not ceremonial. Between them the log changed the source
line number twice, dropped two fields, and swapped the microsecond
suffix — none of which was announced anywhere.
"""

from pathlib import Path

from monad_ops.parser import parse_exec_block

FIXTURE = Path(__file__).parent / "fixtures" / "execution_sample_0160.log"
LEGACY_FIXTURE = Path(__file__).parent / "fixtures" / "execution_sample_0142.log"


def _lines(path: Path) -> list[str]:
    return path.read_text().splitlines()


# ── current release (v0.16.0) ─────────────────────────────────────────

def test_parses_exec_block_with_zero_retries():
    result = parse_exec_block(_lines(FIXTURE)[0])
    assert result is not None
    assert result.block_number == 52836030
    assert result.tx_count == 2
    assert result.retried == 0
    assert result.retry_pct == 0.0
    assert result.parallelism_ratio == 1.0
    assert result.total_us == 2037
    assert result.commit_us == 1297
    assert result.gas_used == 225302


def test_parses_exec_block_with_partial_retries():
    result = parse_exec_block(_lines(FIXTURE)[1])
    assert result is not None
    assert result.block_number == 52836017
    assert result.tx_count == 4
    assert result.retried == 2
    assert result.retry_pct == 50.0
    assert abs(result.parallelism_ratio - 0.5) < 1e-9
    assert result.total_us == 6067
    assert result.gas_used == 2074190


def test_does_not_claim_the_dual_root_line():
    # Line 3 is the MIP-8 dual-write state-root record, which shares the
    # block number but is a different record type.
    assert parse_exec_block(_lines(FIXTURE)[2]) is None


def test_does_not_claim_the_run_to_block_line():
    # Line 4 is the sibling "Run to block= …" record.
    assert parse_exec_block(_lines(FIXTURE)[3]) is None


# ── the suffix change nobody announced ────────────────────────────────

def test_both_microsecond_suffixes_parse():
    """v0.16.0 swapped ``µs`` for a plain ASCII ``us``.

    Verified on this node across its own upgrade: every ``__exec_block``
    line before 2026-08-11 15:07 UTC used ``µs``, every line after used
    ``us``. The parser only survived because it already accepted both —
    ``int("93us")`` would otherwise have raised straight into the tailer
    loop, taking ingestion down on a routine upgrade.
    """
    legacy = parse_exec_block(_lines(LEGACY_FIXTURE)[0])
    current = parse_exec_block(_lines(FIXTURE)[0])
    assert legacy is not None and current is not None
    assert "µs" in _lines(LEGACY_FIXTURE)[0]
    assert "us," in _lines(FIXTURE)[0] and "µs" not in _lines(FIXTURE)[0]
    # Both yield plain integers — the suffix never reaches the model.
    assert legacy.total_us == 760
    assert current.total_us == 2037


# ── legacy release (0.14.2 era), still has to parse ───────────────────

def test_legacy_fixture_still_parses_with_ac_sc():
    result = parse_exec_block(_lines(LEGACY_FIXTURE)[0])
    assert result is not None
    assert result.block_number == 26243795
    assert result.tx_count == 2
    assert result.gas_used == 168650
    # Fields that later releases dropped entirely.
    assert result.active_chunks == 1085212
    assert result.storage_cache_size == 10000000


def test_legacy_run_to_block_line_is_not_claimed():
    assert parse_exec_block(_lines(LEGACY_FIXTURE)[2]) is None


def test_returns_none_for_empty_line():
    assert parse_exec_block("") is None
    assert parse_exec_block("random log noise") is None


# monad 0.14.5 dropped the ac= and sc= fields from __exec_block. The
# parser must still accept the line (caches default to 0) — otherwise
# ingestion freezes on every block, as it did on 2026-06-05.
_LINE_0145_NO_AC_SC = (
    "2026-06-05 21:13:43.728677624 [2225383] runloop_monad.cpp:361 LOG_INFO\t"
    "__exec_block,bl=36392630,"
    "id=0xe61fc6c8d4e1150722d3f9959c74d6dc618b5e8101626601f617ed41b0fb45a9,"
    "ts=1780694023727,tx=    2,rt=   0,rtp= 0.00%,sr=   73µs,txe=   365µs,"
    "cmt=   541µs,tot=  1039µs,tpse= 5479,tps= 1924,gas=   225046,"
    "gpse= 616,gps=216"
)


def test_parses_0145_line_without_ac_sc():
    result = parse_exec_block(_LINE_0145_NO_AC_SC)
    assert result is not None
    assert result.block_number == 36392630
    assert result.tx_count == 2
    assert result.total_us == 1039
    assert result.gas_used == 225046
    # Dropped fields degrade to 0, not a parse failure.
    assert result.active_chunks == 0
    assert result.storage_cache_size == 0


def test_still_parses_ac_sc_when_present():
    line = _LINE_0145_NO_AC_SC + ",ac=10467,sc=123"
    result = parse_exec_block(line)
    assert result is not None
    assert result.active_chunks == 10467
    assert result.storage_cache_size == 123


# Observed on v0.16.0: blocks that carry transactions but report zero
# gas, 27 of 410 in a two-minute sample. Whatever the chain-level
# reason, gas=0 is a value the parser must carry through rather than
# treat as a missing field — a zero here feeds the gas-per-second
# aggregates directly.
_LINE_0160_ZERO_GAS = (
    "2026-08-11 15:10:42.122521205 [3274977] runloop_monad.cpp:389 LOG_INFO\t"
    "__exec_block,bl=52836024,"
    "id=0xeedf696fe13246e49abb44a711df9a5810268bcdfa7fdd739e986ee4162c4b78,"
    "ts=1786461042120,tx=    1,rt=   0,rtp= 0.00%,sr=   63us,txe=   188us,"
    "cmt=  1264us,tot=  2119us,tpse= 5319,tps=  471,gas=        0,"
    "gpse=   0,gps=  0"
)


def test_zero_gas_block_with_transactions_parses():
    result = parse_exec_block(_LINE_0160_ZERO_GAS)
    assert result is not None
    assert result.tx_count == 1
    assert result.gas_used == 0
    assert result.total_us == 2119
