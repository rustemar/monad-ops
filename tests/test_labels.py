"""Tests for the ContractLabels loader."""

from __future__ import annotations

import json
from pathlib import Path

from monad_ops.labels import ContractLabels, Label


def test_load_missing_file_returns_empty(tmp_path: Path) -> None:
    labels = ContractLabels.load(tmp_path / "does_not_exist.json")
    assert len(labels) == 0
    assert labels.get("0x1111111111111111111111111111111111111111") is None


def test_load_none_path_returns_empty() -> None:
    labels = ContractLabels.load(None)
    assert len(labels) == 0


def test_load_valid_entries(tmp_path: Path) -> None:
    p = tmp_path / "labels.json"
    p.write_text(json.dumps({
        "schema_version": 1,
        "labels": {
            "0xAaaa000000000000000000000000000000000000": {
                "name": "Test Protocol: Router",
                "category": "dex",
            },
            "0xbbbb000000000000000000000000000000000000": "Short form name",
        },
    }))
    labels = ContractLabels.load(p)
    assert len(labels) == 2

    # normalization: case-insensitive lookup
    hit = labels.get("0xAAAA000000000000000000000000000000000000")
    assert hit is not None
    assert hit.name == "Test Protocol: Router"
    assert hit.category == "dex"

    # short-form (plain string) defaults category to "unknown"
    hit2 = labels.get("0xbbbb000000000000000000000000000000000000")
    assert hit2 == Label(name="Short form name", category="unknown")


def test_load_skips_bad_entries(tmp_path: Path) -> None:
    p = tmp_path / "labels.json"
    p.write_text(json.dumps({
        "labels": {
            "not_an_address": {"name": "X"},
            "0xdeadbeef": {"name": "too short"},
            "0x1111111111111111111111111111111111111111": {},   # missing name
            "0x2222222222222222222222222222222222222222": 42,   # bad spec
            "0x3333333333333333333333333333333333333333": {"name": "OK"},
        },
    }))
    labels = ContractLabels.load(p)
    assert len(labels) == 1
    assert labels.get("0x3333333333333333333333333333333333333333").name == "OK"


def test_load_malformed_json_returns_empty(tmp_path: Path) -> None:
    p = tmp_path / "labels.json"
    p.write_text("not { valid json")
    labels = ContractLabels.load(p)
    assert len(labels) == 0


def test_as_dict_serialization(tmp_path: Path) -> None:
    p = tmp_path / "labels.json"
    p.write_text(json.dumps({
        "labels": {
            "0xabcd000000000000000000000000000000000000": {
                "name": "A", "category": "system",
            },
        },
    }))
    labels = ContractLabels.load(p)
    out = labels.as_dict()
    assert out == {
        "0xabcd000000000000000000000000000000000000": {"name": "A", "category": "system"},
    }
