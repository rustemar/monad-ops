"""Tests for Storage.list_stress_envelopes — the data feed for the
stress-event quick-jump buttons.

Covers the four scenarios that matter:
  1. Single closed envelope (one critical → one recovered).
  2. Multi-envelope merge — micro-flaps within ``merge_gap_sec`` collapse
     into one event; real between-batch silence stays split.
  3. Live envelope — last critical with no matching recovered.
  4. Age cap — events older than ``max_age_days`` excluded.
"""

from __future__ import annotations

import time
from pathlib import Path

from monad_ops.rules.events import AlertEvent, Severity
from monad_ops.storage import Storage


def _mk_alert(severity: Severity, key: str = "retry_spike") -> AlertEvent:
    return AlertEvent(
        rule="retry_spike",
        severity=severity,
        key=key,
        title="Re-execution rate" if severity == Severity.CRITICAL else "Retry rate normalized",
        detail="test detail",
    )


def test_single_closed_envelope(tmp_path: Path) -> None:
    s = Storage(tmp_path / "state.db")
    t0 = time.time() - 3600
    s.write_alert(_mk_alert(Severity.CRITICAL, "retry_spike:critical"), ts=t0)
    s.write_alert(_mk_alert(Severity.RECOVERED), ts=t0 + 60)
    out = s.list_stress_envelopes(limit=5)
    assert len(out) == 1
    assert out[0]["status"] == "past"
    assert out[0]["from_ts_ms"] == int(t0 * 1000)
    assert out[0]["to_ts_ms"] == int((t0 + 60) * 1000)
    assert out[0]["critical_count"] == 1
    s.close()


def test_micro_flaps_merge_within_gap(tmp_path: Path) -> None:
    """Three CRIT→RECOVERED pairs spaced 60s and 120s apart merge into
    one envelope under the default 30 min gap. Real-world model: stress
    test where retry_pct dipped briefly below recovery threshold and
    re-armed twice within minutes."""
    s = Storage(tmp_path / "state.db")
    t0 = time.time() - 7200
    # Envelope 1: t0 → t0+30
    s.write_alert(_mk_alert(Severity.CRITICAL, "retry_spike:critical"), ts=t0)
    s.write_alert(_mk_alert(Severity.RECOVERED), ts=t0 + 30)
    # Envelope 2: 60s gap, t0+90 → t0+120
    s.write_alert(_mk_alert(Severity.CRITICAL, "retry_spike:critical"), ts=t0 + 90)
    s.write_alert(_mk_alert(Severity.RECOVERED), ts=t0 + 120)
    # Envelope 3: 120s gap, t0+240 → t0+300
    s.write_alert(_mk_alert(Severity.CRITICAL, "retry_spike:critical"), ts=t0 + 240)
    s.write_alert(_mk_alert(Severity.RECOVERED), ts=t0 + 300)
    out = s.list_stress_envelopes(limit=5)
    assert len(out) == 1, f"expected 1 merged envelope, got {len(out)}"
    assert out[0]["from_ts_ms"] == int(t0 * 1000)
    assert out[0]["to_ts_ms"] == int((t0 + 300) * 1000)
    assert out[0]["critical_count"] == 3
    s.close()


def test_real_between_batch_silence_stays_split(tmp_path: Path) -> None:
    """Two stress envelopes 2 hours apart — well beyond the 30 min merge
    threshold — must remain distinct events. Models the 2026-04-20
    between-batch gap pattern."""
    s = Storage(tmp_path / "state.db")
    t0 = time.time() - 86400  # yesterday
    # Batch 1
    s.write_alert(_mk_alert(Severity.CRITICAL, "retry_spike:critical"), ts=t0)
    s.write_alert(_mk_alert(Severity.RECOVERED), ts=t0 + 600)
    # Batch 2 — 2h later
    s.write_alert(_mk_alert(Severity.CRITICAL, "retry_spike:critical"), ts=t0 + 7800)
    s.write_alert(_mk_alert(Severity.RECOVERED), ts=t0 + 8400)
    out = s.list_stress_envelopes(limit=5)
    assert len(out) == 2
    # Newest first
    assert out[0]["from_ts_ms"] == int((t0 + 7800) * 1000)
    assert out[1]["from_ts_ms"] == int(t0 * 1000)
    s.close()


def test_live_envelope_has_null_to_ts(tmp_path: Path) -> None:
    """A trailing CRITICAL with no matching RECOVERED is reported as
    status='live' with to_ts_ms=None — the client substitutes wall-clock
    now so the button always targets the current moment."""
    s = Storage(tmp_path / "state.db")
    t0 = time.time() - 600
    s.write_alert(_mk_alert(Severity.CRITICAL, "retry_spike:critical"), ts=t0)
    out = s.list_stress_envelopes(limit=5)
    assert len(out) == 1
    assert out[0]["status"] == "live"
    assert out[0]["to_ts_ms"] is None
    assert out[0]["from_ts_ms"] == int(t0 * 1000)
    s.close()


def test_age_cap_excludes_old_envelopes(tmp_path: Path) -> None:
    """Envelopes older than max_age_days are not returned."""
    s = Storage(tmp_path / "state.db")
    now = time.time()
    # Recent: 1 day old
    t_recent = now - 86400
    s.write_alert(_mk_alert(Severity.CRITICAL, "retry_spike:critical"), ts=t_recent)
    s.write_alert(_mk_alert(Severity.RECOVERED), ts=t_recent + 60)
    # Ancient: 60 days old
    t_old = now - 60 * 86400
    s.write_alert(_mk_alert(Severity.CRITICAL, "retry_spike:critical"), ts=t_old)
    s.write_alert(_mk_alert(Severity.RECOVERED), ts=t_old + 60)
    out = s.list_stress_envelopes(limit=5, max_age_days=30)
    assert len(out) == 1
    assert out[0]["from_ts_ms"] == int(t_recent * 1000)
    s.close()


def test_limit_caps_count(tmp_path: Path) -> None:
    """Returns at most ``limit`` envelopes, newest first."""
    s = Storage(tmp_path / "state.db")
    t0 = time.time() - 86400
    # 7 well-separated envelopes (4h apart, well beyond merge gap)
    for i in range(7):
        ts = t0 + i * 14400
        s.write_alert(_mk_alert(Severity.CRITICAL, "retry_spike:critical"), ts=ts)
        s.write_alert(_mk_alert(Severity.RECOVERED), ts=ts + 60)
    out = s.list_stress_envelopes(limit=3)
    assert len(out) == 3
    # Newest first → indices 6, 5, 4
    assert out[0]["from_ts_ms"] == int((t0 + 6 * 14400) * 1000)
    assert out[1]["from_ts_ms"] == int((t0 + 5 * 14400) * 1000)
    assert out[2]["from_ts_ms"] == int((t0 + 4 * 14400) * 1000)
    s.close()


def test_orphan_recovered_ignored(tmp_path: Path) -> None:
    """A RECOVERED with no preceding CRITICAL (e.g. WARN→RECOVERED that
    was filtered out of our query) doesn't open or close an envelope."""
    s = Storage(tmp_path / "state.db")
    t0 = time.time() - 3600
    s.write_alert(_mk_alert(Severity.RECOVERED), ts=t0)  # orphan
    s.write_alert(_mk_alert(Severity.CRITICAL, "retry_spike:critical"), ts=t0 + 60)
    s.write_alert(_mk_alert(Severity.RECOVERED), ts=t0 + 120)
    out = s.list_stress_envelopes(limit=5)
    assert len(out) == 1
    assert out[0]["from_ts_ms"] == int((t0 + 60) * 1000)
    s.close()
