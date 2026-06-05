from pathlib import Path

from monad_ops.parser import parse_exec_block

FIXTURE = Path(__file__).parent / "fixtures" / "execution_sample.log"


def test_parses_exec_block_with_zero_retries():
    lines = FIXTURE.read_text().splitlines()
    result = parse_exec_block(lines[0])
    assert result is not None
    assert result.block_number == 26243795
    assert result.tx_count == 2
    assert result.retried == 0
    assert result.retry_pct == 0.0
    assert result.parallelism_ratio == 1.0
    assert result.total_us == 760
    assert result.gas_used == 168650


def test_parses_exec_block_with_partial_retries():
    lines = FIXTURE.read_text().splitlines()
    result = parse_exec_block(lines[1])
    assert result is not None
    assert result.block_number == 26243796
    assert result.tx_count == 3
    assert result.retried == 1
    assert result.retry_pct == 33.33
    assert abs(result.parallelism_ratio - (2 / 3)) < 1e-9
    assert result.commit_us == 448


def test_returns_none_for_non_exec_block_line():
    lines = FIXTURE.read_text().splitlines()
    # The third fixture line is a runloop_monad.cpp:99 entry (not __exec_block).
    result = parse_exec_block(lines[2])
    assert result is None


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
