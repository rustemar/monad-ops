"""MaintenanceGate: hold delivery inside a window, one DB-derived summary after it."""

from __future__ import annotations

import asyncio

import pytest

from monad_ops.alerts.sink import MaintenanceGate, parse_maintenance_ts
from monad_ops.rules.events import AlertEvent, Severity


class _Capture:
    def __init__(self) -> None:
        self.events: list[AlertEvent] = []

    async def deliver(self, event: AlertEvent) -> None:
        self.events.append(event)


class _Store:
    """The slice of Storage the gate uses, in memory. Rows are what the
    outer recording sink would have written."""

    def __init__(self) -> None:
        self.since: float | None = None
        self.until: float | None = None
        self.rows: list[tuple[float, str, str, str]] = []
        self.recovering: set[str] = {"stall", "reference_lag", "service_failure", "retry_spike"}
        self.closed = 0
        self.reads = 0
        self.fail = False

    def maintenance_window(self):
        self.reads += 1
        if self.fail:
            raise RuntimeError("database is locked")
        return (self.since, self.until)

    def alerts_between(self, since, until):
        return [r for r in self.rows if since <= r[0] <= until]

    def recovering_rules(self):
        return set(self.recovering)

    def close_maintenance(self):
        self.since = None
        self.closed += 1


def _key(rule: str, sev: Severity) -> str:
    # The envelope rules' convention: WARN/CRITICAL keyed rule:severity, RECOVERED bare.
    return rule if sev is Severity.RECOVERED else f"{rule}:{sev.value}"


def _ev(rule: str, sev: Severity = Severity.CRITICAL) -> AlertEvent:
    return AlertEvent(rule=rule, severity=sev, key=_key(rule, sev), title="t", detail="d")


def _row(ts: float, rule: str, sev: str) -> tuple[float, str, str, str]:
    return (ts, rule, sev, rule if sev == "recovered" else f"{rule}:{sev}")


def _setup(*, since=None, until=None, now=1000.0, cache_sec=0.0):
    store, inner, clock, recorded = _Store(), _Capture(), {"now": now}, []
    store.since, store.until = since, until

    async def record(ev):
        recorded.append(ev)

    gate = MaintenanceGate(
        inner, store, record=record, now=lambda: clock["now"], cache_sec=cache_sec
    )
    return gate, inner, store, clock, recorded


@pytest.mark.asyncio
async def test_delivery_is_live_without_a_window() -> None:
    gate, inner, *_ = _setup()
    await gate.deliver(_ev("stall"))
    assert [e.rule for e in inner.events] == ["stall"]


@pytest.mark.asyncio
async def test_events_inside_the_window_are_held() -> None:
    gate, inner, store, *_ = _setup(since=900.0, until=2000.0)
    await gate.deliver(_ev("stall", Severity.WARN))
    await gate.deliver(_ev("stall", Severity.CRITICAL))
    await gate.deliver(_ev("service_failure"))
    await gate.deliver(_ev("stall", Severity.RECOVERED))
    assert inner.events == []
    assert gate.window_end() == 2000.0
    assert store.closed == 0


@pytest.mark.asyncio
async def test_recovered_of_an_alert_seen_live_passes_inside_the_window() -> None:
    """The red message the operator already has must get its green close —
    matched on the rule, since stall:critical recovers as plain 'stall'."""
    gate, inner, store, clock, _ = _setup()
    await gate.deliver(_ev("stall", Severity.CRITICAL))
    store.since, store.until = 1100.0, 2000.0
    clock["now"] = 1200.0
    await gate.deliver(_ev("stall", Severity.RECOVERED))
    await gate.deliver(_ev("reference_lag", Severity.RECOVERED))  # never seen live: held
    assert [(e.rule, e.severity) for e in inner.events] == [
        ("stall", Severity.CRITICAL), ("stall", Severity.RECOVERED)]


@pytest.mark.asyncio
async def test_summary_is_built_from_recorded_rows_and_sent_once() -> None:
    gate, inner, store, clock, recorded = _setup(since=900.0, until=2000.0)
    store.rows = [
        _row(1000.0, "stall", "warn"),
        _row(1010.0, "stall", "critical"),
        _row(1020.0, "service_failure", "critical"),
        _row(1030.0, "process_restart", "warn"),
        _row(1100.0, "service_failure", "recovered"),
        _row(1200.0, "stall", "recovered"),
    ]
    clock["now"] = 2001.0
    await gate.deliver(_ev("retry_spike", Severity.WARN))

    assert [e.rule for e in inner.events] == ["maintenance", "retry_spike"]
    summary = inner.events[0]
    assert summary.severity is Severity.INFO  # process_restart never recovers: not "open"
    assert "Held 6 alert(s)" in summary.detail
    assert "process_restart ×1" in summary.detail and "stall ×3" in summary.detail
    assert "Still open" not in summary.detail
    assert summary.key == "maintenance:closed:2000"
    assert recorded == [summary]  # the history gets the marker too
    assert store.closed == 1
    await gate.flush()
    assert store.closed == 1 and len(inner.events) == 2


@pytest.mark.asyncio
async def test_still_open_envelopes_raise_the_summary_severity() -> None:
    """An overrun upgrade must not end on a blue line while the node is down."""
    gate, inner, store, clock, _ = _setup(since=900.0, until=2000.0)
    store.rows = [
        _row(1000.0, "stall", "warn"),
        _row(1005.0, "stall", "critical"),
        _row(1010.0, "reference_lag", "warn"),
        _row(1020.0, "process_restart", "warn"),
        _row(1030.0, "service_failure", "critical"),
        _row(1040.0, "service_failure", "recovered"),
    ]
    clock["now"] = 2000.0
    await gate.flush()
    summary = inner.events[0]
    assert summary.severity is Severity.CRITICAL
    assert "Still open: reference_lag WARN, stall CRITICAL." in summary.detail


@pytest.mark.asyncio
async def test_summary_survives_a_restart_inside_the_window() -> None:
    """Nothing lives in the gate: a fresh instance over the same store
    summarises the rows the previous process recorded."""
    gate, inner, store, clock, _ = _setup(since=900.0, until=2000.0)
    store.rows = [_row(1000.0, "stall", "critical"), _row(1500.0, "stall", "recovered")]
    await gate.deliver(_ev("stall"))  # held by the first process
    restarted = MaintenanceGate(inner, store, now=lambda: clock["now"], cache_sec=0.0)
    clock["now"] = 2000.0
    await restarted.flush()
    assert [e.rule for e in inner.events] == ["maintenance"]
    assert "stall ×2" in inner.events[0].detail


@pytest.mark.asyncio
async def test_quiet_window_sends_one_closing_line() -> None:
    gate, inner, store, clock, _ = _setup(since=900.0, until=2000.0)
    clock["now"] = 2000.0
    await gate.flush()
    await gate.flush()
    assert len(inner.events) == 1
    assert "Nothing was held" in inner.events[0].detail
    assert inner.events[0].severity is Severity.INFO


@pytest.mark.asyncio
async def test_closing_early_summarises_on_the_next_flush() -> None:
    gate, inner, store, clock, _ = _setup(since=900.0, until=2000.0)
    store.rows = [_row(1000.0, "stall", "critical")]
    await gate.deliver(_ev("stall"))
    store.until = 1500.0  # `maintenance --off` sets until = now
    clock["now"] = 1500.0
    await gate.flush()
    assert [e.rule for e in inner.events] == ["maintenance"]
    assert gate.window_end() is None


@pytest.mark.asyncio
async def test_an_event_held_on_the_cached_window_after_off_is_still_counted() -> None:
    gate, inner, store, clock, _ = _setup(since=900.0, until=1500.0, cache_sec=2.0)
    clock["now"] = 1499.0
    await gate.deliver(_ev("stall"))  # reads the window: open until 1500
    store.until = 1499.5  # --off, unseen by the cache
    clock["now"] = 1499.8
    await gate.deliver(_ev("reference_lag"))  # cache still says open: held
    store.rows = [_row(1499.0, "stall", "critical"), _row(1499.8, "reference_lag", "critical")]
    assert inner.events == []
    clock["now"] = 1502.0
    await gate.flush()
    # The second row sits after `until`; counting up to the flush time keeps it.
    assert "Held 2 alert(s)" in inner.events[0].detail


@pytest.mark.asyncio
async def test_concurrent_flushes_send_one_summary() -> None:
    gate, inner, store, clock, _ = _setup(since=900.0, until=2000.0)
    store.rows = [_row(1000.0, "stall", "critical"), _row(1001.0, "stall", "recovered")]
    clock["now"] = 2000.0
    await asyncio.gather(gate.flush(), gate.flush(), gate.flush())
    assert [e.rule for e in inner.events] == ["maintenance"]
    assert store.closed == 1


@pytest.mark.asyncio
async def test_window_read_is_cached_and_a_failing_read_keeps_the_last_answer() -> None:
    gate, inner, store, clock, _ = _setup(since=900.0, until=2000.0, cache_sec=2.0)
    await gate.deliver(_ev("stall"))
    await gate.deliver(_ev("stall", Severity.WARN))
    assert store.reads == 1  # second event inside the cache window
    clock["now"] = 1003.0
    store.fail = True
    await gate.deliver(_ev("service_failure"))
    assert inner.events == []  # still held on the last known window
    clock["now"] = 1006.0
    store.fail = False
    store.since = store.until = None
    await gate.deliver(_ev("retry_spike", Severity.WARN))
    assert [e.rule for e in inner.events] == ["retry_spike"]


def test_parse_maintenance_ts_tolerates_bad_meta() -> None:
    assert parse_maintenance_ts(None) is None
    assert parse_maintenance_ts("") is None
    assert parse_maintenance_ts("0") is None
    assert parse_maintenance_ts("garbage") is None
    assert parse_maintenance_ts("1789740000") == 1789740000.0
