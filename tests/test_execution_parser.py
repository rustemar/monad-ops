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
