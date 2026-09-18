"""MaintenanceGate: hold delivery inside a window, one DB-derived summary after it."""

from __future__ import annotations

import asyncio

import pytest

from monad_ops.alerts.sink import MaintenanceGate, envelope_id, parse_maintenance_ts
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
        self.recovered: set[str] = {"stall", "reference_lag", "retry_spike"}
        self.taken = 0
        self.reads = 0
        self.fail = False

    def maintenance_window(self):
        self.reads += 1
        if self.fail:
            raise RuntimeError("database is locked")
        return (self.since, self.until)

    def take_closed_window(self, now):
        if self.since is None or self.until is None or self.until > now:
            return None
        since, self.since = self.since, None
        self.taken += 1
        return (since, self.until)

    def alerts_between(self, since, until, *, before=None):
        return [r for r in self.rows
                if since <= r[0] <= until and (before is None or r[0] < before)]

    def recovered_envelopes(self):
        return set(self.recovered)

    def last_severity_before(self, envelope, ts):
        hits = [r for r in self.rows if r[0] < ts and envelope_id(r[3]) == envelope]
        return hits[-1][2] if hits else None


def _key(rule: str, sev: Severity, entity: str | None = None) -> str:
    if entity:  # per-entity rules use one key for every severity
        return f"{rule}:{entity}"
    return rule if sev is Severity.RECOVERED else f"{rule}:{sev.value}"


def _ev(rule: str, sev: Severity = Severity.CRITICAL, entity: str | None = None) -> AlertEvent:
    return AlertEvent(rule=rule, severity=sev, key=_key(rule, sev, entity), title="t", detail="d")


def _row(ts: float, rule: str, sev: str, entity: str | None = None) -> tuple[float, str, str, str]:
    return (ts, rule, sev, _key(rule, Severity(sev), entity))


def _setup(*, since=None, until=None, now=1000.0, cache_sec=0.0):
    store, inner, clock, recorded = _Store(), _Capture(), {"now": now}, []
    store.since, store.until = since, until

    async def record(ev):
        recorded.append(ev)

    gate = MaintenanceGate(
        inner, store, record=record, now=lambda: clock["now"], cache_sec=cache_sec
    )
    return gate, inner, store, clock, recorded


def test_envelope_id_strips_only_the_severity_suffix() -> None:
    assert envelope_id("stall:critical") == "stall"
    assert envelope_id("stall") == "stall"
    assert envelope_id("service_failure:monad-rpc") == "service_failure:monad-rpc"
    assert envelope_id("service_failure:monad-rpc:crash") == "service_failure:monad-rpc:crash"
    assert envelope_id("enrichment_health:failing") == "enrichment_health:failing"


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
    await gate.deliver(_ev("service_failure", entity="monad-bft"))
    await gate.deliver(_ev("stall", Severity.RECOVERED))
    assert inner.events == []
    assert gate.window_end() == 2000.0
    assert store.taken == 0


@pytest.mark.asyncio
async def test_a_row_stamped_before_the_window_opened_is_delivered_live() -> None:
    gate, inner, *_ = _setup(since=1000.0, until=2000.0, now=1001.0)
    await gate.deliver(_ev("stall"), ts=999.9)
    await gate.deliver(_ev("reference_lag"), ts=1000.5)
    assert [e.rule for e in inner.events] == ["stall"]


@pytest.mark.asyncio
async def test_one_shot_rules_pass_through_the_window() -> None:
    gate, inner, *_ = _setup(since=900.0, until=2000.0)
    upgraded = AlertEvent(rule="version_watch", severity=Severity.RECOVERED,
                          key="version_watch:upgraded:0.16.3", title="Node upgraded", detail="d")
    await gate.deliver(upgraded)
    assert [e.rule for e in inner.events] == ["version_watch"]


@pytest.mark.asyncio
async def test_recovered_of_an_alert_seen_live_passes_inside_the_window() -> None:
    """The red message the operator already has must get its green close —
    matched on the envelope, since stall:critical recovers as plain 'stall'."""
    gate, inner, store, clock, _ = _setup()
    await gate.deliver(_ev("stall", Severity.CRITICAL))
    store.since, store.until = 1100.0, 2000.0
    clock["now"] = 1200.0
    await gate.deliver(_ev("stall", Severity.RECOVERED))
    await gate.deliver(_ev("reference_lag", Severity.RECOVERED))  # never seen red: held
    assert [(e.rule, e.severity) for e in inner.events] == [
        ("stall", Severity.CRITICAL), ("stall", Severity.RECOVERED)]


@pytest.mark.asyncio
async def test_recovered_pass_through_survives_a_restart_via_the_history() -> None:
    gate, inner, store, *_ = _setup(since=1100.0, until=2000.0, now=1200.0)
    store.rows = [_row(1000.0, "stall", "critical")]  # delivered by the previous process
    await gate.deliver(_ev("stall", Severity.RECOVERED))
    assert [(e.rule, e.severity) for e in inner.events] == [("stall", Severity.RECOVERED)]


@pytest.mark.asyncio
async def test_two_envelopes_of_one_rule_both_get_their_green() -> None:
    gate, inner, store, clock, _ = _setup()
    await gate.deliver(_ev("enrichment_health", Severity.WARN, entity="failing"))
    await gate.deliver(_ev("enrichment_health", Severity.WARN, entity="dropping"))
    store.since, store.until = 1100.0, 2000.0
    clock["now"] = 1200.0
    await gate.deliver(_ev("enrichment_health", Severity.RECOVERED, entity="failing"))
    await gate.deliver(_ev("enrichment_health", Severity.RECOVERED, entity="dropping"))
    assert [e.severity for e in inner.events[2:]] == [Severity.RECOVERED, Severity.RECOVERED]


@pytest.mark.asyncio
async def test_summary_is_built_from_recorded_rows_and_sent_once() -> None:
    gate, inner, store, clock, recorded = _setup(since=900.0, until=2000.0)
    store.rows = [
        _row(1000.0, "stall", "warn"),
        _row(1010.0, "stall", "critical"),
        _row(1020.0, "service_failure", "critical", "monad-bft"),
        _row(1030.0, "process_restart", "warn", "monad-bft"),
        _row(1100.0, "service_failure", "recovered", "monad-bft"),
        _row(1200.0, "stall", "recovered"),
    ]
    clock["now"] = 2001.0
    await gate.deliver(_ev("retry_spike", Severity.WARN), ts=2001.0)

    assert [e.rule for e in inner.events] == ["maintenance", "retry_spike"]
    summary = inner.events[0]
    assert summary.severity is Severity.INFO
    assert "6 alert(s) recorded" in summary.detail
    assert "process_restart ×1" in summary.detail and "stall ×3" in summary.detail
    assert "Still open" not in summary.detail
    assert summary.key == "maintenance:closed:2000"
    assert recorded == [summary]  # the history gets the marker too
    assert store.taken == 1
    await gate.flush()
    assert store.taken == 1 and len(inner.events) == 2


@pytest.mark.asyncio
async def test_still_open_envelopes_raise_the_summary_severity() -> None:
    """An overrun upgrade must not end on a blue line while the node is down."""
    gate, inner, store, clock, _ = _setup(since=900.0, until=2000.0)
    store.recovered |= {"service_failure:monad-rpc", "service_failure:monad-bft"}
    store.rows = [
        _row(1000.0, "stall", "warn"),
        _row(1005.0, "stall", "critical"),
        _row(1010.0, "reference_lag", "warn"),
        _row(1020.0, "process_restart", "warn", "monad-bft"),
        _row(1030.0, "service_failure", "critical", "monad-rpc"),
        _row(1035.0, "service_failure", "critical", "monad-bft"),
        _row(1040.0, "service_failure", "recovered", "monad-bft"),  # rpc is still down
        (1050.0, "service_failure", "critical", "service_failure:monad-bft:crash"),
    ]
    clock["now"] = 2000.0
    await gate.flush()
    summary = inner.events[0]
    assert summary.severity is Severity.CRITICAL
    assert ("Still open: reference_lag WARN, service_failure:monad-rpc CRITICAL, "
            "stall CRITICAL.") in summary.detail


@pytest.mark.asyncio
async def test_summary_survives_a_restart_inside_the_window() -> None:
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
    assert "Nothing was recorded" in inner.events[0].detail
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
    await gate.deliver(_ev("stall"), ts=1499.0)  # reads the window: open until 1500
    store.until = 1499.5  # --off, unseen by the cache
    clock["now"] = 1499.8
    await gate.deliver(_ev("reference_lag"), ts=1499.8)  # cache still says open: held
    store.rows = [_row(1499.0, "stall", "critical"), _row(1499.8, "reference_lag", "critical")]
    assert inner.events == []
    clock["now"] = 1502.0
    await gate.flush()
    assert "2 alert(s) recorded" in inner.events[0].detail


@pytest.mark.asyncio
async def test_the_event_that_triggers_the_flush_is_not_counted_as_held() -> None:
    gate, inner, store, clock, _ = _setup(since=900.0, until=1500.0)
    store.rows = [_row(1000.0, "stall", "critical"), _row(1600.0, "stall", "recovered")]
    clock["now"] = 1600.0
    await gate.deliver(_ev("stall", Severity.RECOVERED), ts=1600.0)
    summary, live = inner.events
    assert "1 alert(s) recorded" in summary.detail
    assert "Still open" in summary.detail  # the stall row inside the window was red
    assert live.severity is Severity.RECOVERED  # and its green follows right after


@pytest.mark.asyncio
async def test_concurrent_flushes_send_one_summary() -> None:
    gate, inner, store, clock, _ = _setup(since=900.0, until=2000.0)
    store.rows = [_row(1000.0, "stall", "critical"), _row(1001.0, "stall", "recovered")]
    clock["now"] = 2000.0
    await asyncio.gather(gate.flush(), gate.flush(), gate.flush())
    assert [e.rule for e in inner.events] == ["maintenance"]
    assert store.taken == 1


@pytest.mark.asyncio
async def test_a_window_taken_by_someone_else_is_not_summarised_again() -> None:
    gate, inner, store, clock, _ = _setup(since=900.0, until=2000.0)
    clock["now"] = 2000.0
    store.take_closed_window(2000.0)  # another process got there first
    await gate.flush()
    assert inner.events == []


@pytest.mark.asyncio
async def test_summary_goes_out_even_when_recording_it_fails() -> None:
    store, inner, clock = _Store(), _Capture(), {"now": 2000.0}
    store.since, store.until = 900.0, 2000.0

    async def record(_ev):
        raise RuntimeError("database is locked")

    gate = MaintenanceGate(inner, store, record=record, now=lambda: clock["now"], cache_sec=0.0)
    await gate.flush()
    assert [e.rule for e in inner.events] == ["maintenance"]


@pytest.mark.asyncio
async def test_a_failing_read_during_a_timer_flush_keeps_the_window_held() -> None:
    gate, inner, store, clock, _ = _setup(since=900.0, until=2000.0, cache_sec=2.0)
    await gate.deliver(_ev("stall"))  # good read: open
    store.fail = True
    clock["now"] = 1030.0
    await gate.flush()  # timer tick, read fails: last good answer stands
    await gate.deliver(_ev("reference_lag"))
    assert inner.events == []


@pytest.mark.asyncio
async def test_window_read_is_cached() -> None:
    gate, inner, store, clock, _ = _setup(since=900.0, until=2000.0, cache_sec=2.0)
    await gate.deliver(_ev("stall"))
    await gate.deliver(_ev("stall", Severity.WARN))
    assert store.reads == 1  # second event inside the cache window
    clock["now"] = 1003.0
    store.since = store.until = None
    await gate.deliver(_ev("retry_spike", Severity.WARN))
    assert [e.rule for e in inner.events] == ["retry_spike"]


def test_parse_maintenance_ts_tolerates_bad_meta() -> None:
    assert parse_maintenance_ts(None) is None
    assert parse_maintenance_ts("") is None
    assert parse_maintenance_ts("0") is None
    assert parse_maintenance_ts("garbage") is None
    assert parse_maintenance_ts("1789740000") == 1789740000.0
