"""Tests for the static replay-artifact exporter.

Validates that ``assemble_window_data`` returns a well-formed dict
covering every section a Foundation-facing replay needs (window
metadata, aggregate, consensus, base_fee, three series), and that
``render_static_html`` produces a single self-contained document
with both Chart.js and the JSON payload inlined.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from monad_ops.replay_export import assemble_window_data, render_static_html
from monad_ops.storage import BftBaseFee, BftMinute, Storage


_T0_MS = 1_777_003_200_000
_MIN_MS = 60_000


@pytest.fixture()
def seeded_storage(tmp_path: Path) -> Storage:
    storage = Storage(tmp_path / "state.db")
    # 3 minutes of bft data: 100 rounds/min, 1 TC each minute, 0 local fires
    for i in range(3):
        storage.upsert_bft_minute(BftMinute(
            ts_minute=_T0_MS + i * _MIN_MS,
            rounds_total=100, rounds_tc=1, local_timeouts=0,
        ))
    # 5 base-fee samples at floor
    for i in range(5):
        storage.insert_bft_base_fee(BftBaseFee(
            block_seq=27_000_000 + i,
            ts_ms=_T0_MS + i * 30_000,
            base_fee_wei=100_000_000_000,
        ))
    return storage


def test_assemble_window_data_full_shape(seeded_storage: Storage) -> None:
    """Every top-level section the HTML viewer expects is present."""
    data = assemble_window_data(
        seeded_storage, _T0_MS, _T0_MS + 5 * _MIN_MS,
    )
    expected = {"window", "aggregate", "consensus", "base_fee",
                "series", "exported_at_ms", "schema_version"}
    assert expected <= set(data.keys())

    win = data["window"]
    assert {"from_ts_ms", "to_ts_ms", "from_iso", "to_iso", "span_sec"} <= set(win.keys())
    assert "UTC" in win["from_iso"]

    series = data["series"]
    assert {"bft", "base_fee", "blocks"} <= set(series.keys())
    assert isinstance(series["bft"], list)


def test_assemble_window_data_consensus_aggregates_correctly(
    seeded_storage: Storage,
) -> None:
    """Window covers all 3 seeded minutes — totals match seeded values."""
    data = assemble_window_data(
        seeded_storage, _T0_MS, _T0_MS + 3 * _MIN_MS,
    )
    c = data["consensus"]
    assert c["rounds_total"] == 300
    assert c["rounds_tc"] == 3
    assert c["validator_timeout_pct"] == 1.0
    bf = data["base_fee"]
    assert bf["samples"] == 5
    assert bf["base_fee_gwei_avg"] == 100.0


def test_render_static_html_inlines_chart_js_and_data(
    tmp_path: Path,
    seeded_storage: Storage,
) -> None:
    """Render produces a self-contained doc — Chart.js code + JSON
    payload both inlined, no external <script src=...> required."""
    data = assemble_window_data(seeded_storage, _T0_MS, _T0_MS + 3 * _MIN_MS)
    chart_js_path = (
        Path(__file__).parent.parent / "monad_ops" / "dashboard" / "static"
        / "vendor" / "chart.umd.min.js"
    )
    assert chart_js_path.exists(), "chart.umd.min.js fixture missing"

    html = render_static_html(
        data, node_name="test-node", chart_js_path=chart_js_path,
    )
    assert "<!doctype html>" in html
    # Chart.js inlined (look for a fingerprint string from the lib).
    assert "Chart" in html
    assert len(html) > 100_000, "expected chart.js bytes inlined"
    # Data inlined.
    assert "REPLAY" in html
    assert "rounds_total" in html
    assert "test-node" in html
    # No external script src.
    assert "<script src=" not in html.lower()


def test_render_static_html_escapes_script_terminator(
    tmp_path: Path,
    seeded_storage: Storage,
) -> None:
    """A </script> sequence inside inline JSON would close the wrapping
    <script> tag and break the page. JSON has no </ by definition but
    the renderer escapes it defensively. This test guards the escape."""
    data = assemble_window_data(seeded_storage, _T0_MS, _T0_MS + _MIN_MS)
    # Inject a </script>-shaped string into an aggregate field via a
    # synthetic key the renderer copies verbatim.
    data["consensus"]["__synthetic_marker"] = "danger</script>tag"
    html = render_static_html(
        data, node_name="test-node",
        chart_js_path=Path(__file__).parent.parent / "monad_ops" / "dashboard"
        / "static" / "vendor" / "chart.umd.min.js",
    )
    # The literal sequence must NOT appear in the rendered HTML.
    assert "</script>tag" not in html
    # The escaped form should appear instead.
    assert "<\\/script>tag" in html


def test_assemble_window_data_empty_window_returns_zeros(tmp_path: Path) -> None:
    """A window with no data → well-formed dict with zero counts, not
    None / missing keys (otherwise the HTML viewer would render NaN)."""
    storage = Storage(tmp_path / "state.db")
    data = assemble_window_data(storage, _T0_MS, _T0_MS + _MIN_MS)
    assert data["consensus"]["rounds_total"] == 0
    assert data["base_fee"]["samples"] == 0
    assert data["series"]["bft"] == []
    assert data["series"]["base_fee"] == []
