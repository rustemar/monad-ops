"""Unit tests for collector/version.py — Packages.gz parsing + status logic.

dpkg shell-out is patched out so the suite runs without dpkg installed
(CI runners + macOS). The Debian comparison rules we exercise are simple
enough that lexicographic ordering matches dpkg for the in-line
test fixtures.
"""

from __future__ import annotations

import gzip
from collections.abc import Iterable

import pytest

from monad_ops.collector import version as version_mod
from monad_ops.collector.version import (
    fetch_version_status,
)


def _packages_blob(entries: Iterable[tuple[str, str]]) -> bytes:
    """Build a synthetic Debian Packages.gz body.

    Each entry is (Package, Version). Multiple Version blocks for the
    same Package are emitted as separate paragraphs — matches what the
    Category Labs apt repo does when several monad releases are
    available simultaneously.
    """
    paragraphs = []
    for pkg, ver in entries:
        paragraphs.append(f"Package: {pkg}\nVersion: {ver}\nArchitecture: amd64\n")
    body = "\n".join(paragraphs).encode("utf-8")
    return gzip.compress(body)


@pytest.fixture
def patch_dpkg(monkeypatch):
    """Replace dpkg-compare-versions with a python lexicographic compare.

    All test versions are simple "0.X.Y" strings where the lexicographic
    and dpkg orderings agree, so this is a faithful stand-in.
    """
    async def _cmp(a: str, b: str) -> int:
        return (a > b) - (a < b)
    monkeypatch.setattr(version_mod, "_dpkg_compare", _cmp)
    return _cmp


@pytest.fixture
def patch_installed(monkeypatch):
    """Patch the dpkg-query lookup with a settable holder."""
    holder = {"value": None}

    async def _get(_pkg: str) -> str | None:
        return holder["value"]
    monkeypatch.setattr(version_mod, "_installed_version", _get)
    return holder


@pytest.fixture
def patch_blob(monkeypatch):
    """Patch the HTTP fetch with a settable holder.

    Setting holder["error"] to an Exception triggers an exception path
    so we can exercise the unknown/error branch deterministically.
    """
    holder = {"value": b"", "error": None}

    async def _fetch(_url: str, _timeout: float) -> bytes:
        if holder["error"] is not None:
            raise holder["error"]
        return holder["value"]
    monkeypatch.setattr(version_mod, "_fetch_packages_blob", _fetch)
    return holder


@pytest.mark.asyncio
async def test_up_to_date(patch_dpkg, patch_installed, patch_blob):
    patch_installed["value"] = "0.14.2"
    patch_blob["value"] = _packages_blob([("monad", "0.14.1"), ("monad", "0.14.2")])
    s = await fetch_version_status(
        package="monad", packages_url="x://test",
    )
    assert s.status == "up_to_date"
    assert s.installed == "0.14.2"
    assert s.latest == "0.14.2"
    assert s.extras_newer == ()


@pytest.mark.asyncio
async def test_update_available(patch_dpkg, patch_installed, patch_blob):
    patch_installed["value"] = "0.14.1"
    patch_blob["value"] = _packages_blob([
        ("monad", "0.14.1"), ("monad", "0.14.2"), ("monad", "0.14.3"),
    ])
    s = await fetch_version_status(package="monad", packages_url="x://test")
    assert s.status == "update_available"
    assert s.installed == "0.14.1"
    assert s.latest == "0.14.3"
    # Extras descending, both newer-than-installed entries present.
    assert s.extras_newer == ("0.14.3", "0.14.2")


@pytest.mark.asyncio
async def test_skips_debug_and_preview(patch_dpkg, patch_installed, patch_blob):
    """Higher-than-stable debug variants must not be proposed as upgrades.

    Specifically the "0.14.3-debug" string sorts above "0.14.2" under
    plain lexicographic comparison; the skip-substring filter is what
    prevents it from becoming the picked latest. If this test ever
    starts to fail it likely means SKIP_VERSION_SUBSTRINGS regressed.
    """
    patch_installed["value"] = "0.14.2"
    patch_blob["value"] = _packages_blob([
        ("monad", "0.14.2"),
        ("monad", "0.14.3-debug"),
        ("monad", "0.14.3-preview"),
    ])
    s = await fetch_version_status(package="monad", packages_url="x://test")
    assert s.status == "up_to_date"
    assert s.latest == "0.14.2"


@pytest.mark.asyncio
async def test_unknown_when_not_installed(patch_dpkg, patch_installed, patch_blob):
    patch_installed["value"] = None
    patch_blob["value"] = _packages_blob([("monad", "0.14.2")])
    s = await fetch_version_status(package="monad", packages_url="x://test")
    assert s.status == "unknown"
    assert s.installed is None
    assert "not installed" in (s.error or "")


@pytest.mark.asyncio
async def test_unknown_on_repo_fetch_failure(patch_dpkg, patch_installed, patch_blob):
    patch_installed["value"] = "0.14.2"
    patch_blob["error"] = TimeoutError("upstream slow")
    s = await fetch_version_status(package="monad", packages_url="x://test")
    assert s.status == "unknown"
    assert s.installed == "0.14.2"
    assert "repo fetch failed" in (s.error or "")


@pytest.mark.asyncio
async def test_unknown_when_no_versions_in_repo(patch_dpkg, patch_installed, patch_blob):
    """Repo unreachable for our package (e.g. wrong dist) — surface gracefully."""
    patch_installed["value"] = "0.14.2"
    patch_blob["value"] = _packages_blob([("not-monad", "1.0")])
    s = await fetch_version_status(package="monad", packages_url="x://test")
    assert s.status == "unknown"


@pytest.mark.asyncio
async def test_filters_other_packages_in_repo(patch_dpkg, patch_installed, patch_blob):
    """Real Packages.gz contains many packages — only the configured one matters."""
    patch_installed["value"] = "0.14.1"
    patch_blob["value"] = _packages_blob([
        ("python3-requests", "2.31.0"),
        ("monad", "0.14.2"),
        ("libfoo", "1.2.3"),
    ])
    s = await fetch_version_status(package="monad", packages_url="x://test")
    assert s.status == "update_available"
    assert s.latest == "0.14.2"
