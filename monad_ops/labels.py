"""Contract label registry.

A small, community-editable JSON file maps known testnet addresses to
human-readable names and categories. Consumed by the API layer so the
dashboard can show "Some Dex: router" instead of
``0xa2b0067002df61df015c11b2cf0be4f34fc41cf8``.

Intentionally conservative: we ship an empty label map and let
contributors add entries as they verify addresses on the explorer.
Inventing labels poisons the dataset — better to show a bare address
than a wrong name.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import structlog


log = structlog.stdlib.get_logger()


@dataclass(frozen=True, slots=True)
class Label:
    name: str
    category: str = "unknown"


class ContractLabels:
    """Normalized address → Label lookup.

    Addresses are normalized to lowercase at load time; ``get`` accepts
    any case. Missing keys return None.
    """

    def __init__(self, labels: dict[str, Label]) -> None:
        self._labels = labels

    @classmethod
    def load(cls, path: Path | str | None) -> "ContractLabels":
        """Load labels from a JSON file. Missing/unreadable file → empty map."""
        if path is None:
            return cls({})
        p = Path(path)
        if not p.exists():
            log.warning("labels.file_missing", path=str(p))
            return cls({})
        try:
            raw = json.loads(p.read_text())
        except (json.JSONDecodeError, OSError) as e:
            log.error("labels.load_failed", path=str(p), err=str(e))
            return cls({})

        mapping: dict[str, Label] = {}
        entries = raw.get("labels", {}) if isinstance(raw, dict) else {}
        if not isinstance(entries, dict):
            log.error("labels.bad_shape", path=str(p))
            return cls({})

        for addr, spec in entries.items():
            if not isinstance(addr, str):
                continue
            key = addr.strip().lower()
            if not _looks_like_addr(key):
                log.warning("labels.bad_addr", addr=addr)
                continue
            if isinstance(spec, str):
                mapping[key] = Label(name=spec)
            elif isinstance(spec, dict):
                name = spec.get("name")
                if not isinstance(name, str) or not name:
                    log.warning("labels.missing_name", addr=addr)
                    continue
                category = spec.get("category", "unknown")
                if not isinstance(category, str):
                    category = "unknown"
                mapping[key] = Label(name=name, category=category)
            else:
                log.warning("labels.bad_spec", addr=addr, kind=type(spec).__name__)

        log.info("labels.loaded", count=len(mapping), path=str(p))
        return cls(mapping)

    def get(self, addr: str | None) -> Label | None:
        if addr is None:
            return None
        return self._labels.get(addr.strip().lower())

    def as_dict(self) -> dict[str, dict[str, str]]:
        return {
            addr: {"name": lbl.name, "category": lbl.category}
            for addr, lbl in self._labels.items()
        }

    def __len__(self) -> int:
        return len(self._labels)


def _looks_like_addr(s: str) -> bool:
    return (
        len(s) == 42
        and s.startswith("0x")
        and all(c in "0123456789abcdef" for c in s[2:])
    )
