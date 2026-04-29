"""Pure data fetcher for monad package version comparison.

Reads the Category Labs apt repo's ``Packages.gz`` over HTTPS, parses
the metadata for the configured package, and compares the highest
"stable-looking" version against the locally installed one (via
``dpkg-query``). Returns a single ``VersionStatus`` snapshot — no
alert emission, no state, no I/O beyond the two reads.

The transition logic (when to alert, when to remind, when to declare
"upgraded") lives in ``monad_ops.rules.version`` and consumes these
snapshots. Splitting the two keeps this module easy to test offline
(mock the HTTP fetch and ``dpkg`` shell-out) and lets the rule run
deterministically against synthesized status sequences.
"""

from __future__ import annotations

import asyncio
import gzip
import urllib.request
from collections.abc import Iterable
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class VersionStatus:
    """Snapshot of installed-vs-available for one package.

    ``status`` is one of:
      * ``"up_to_date"`` — installed >= latest stable in the repo
      * ``"update_available"`` — latest stable is strictly newer
      * ``"unknown"`` — could not determine (probe error / package not
        installed / repo unreachable). ``error`` carries the reason.
    """
    package: str
    installed: str | None
    latest: str | None
    extras_newer: tuple[str, ...]   # all repo versions strictly newer than installed
    status: str                      # "up_to_date" | "update_available" | "unknown"
    error: str | None = None


# Repo publishes both the production line and debug/preview/rc variants
# under the same package name. We never propose those as upgrades.
_DEFAULT_SKIP_SUBSTRINGS: tuple[str, ...] = ("-debug", "-preview", "~preview", "~rc", "-rc")


async def _dpkg_compare(a: str, b: str) -> int:
    """Return -1/0/1 for dpkg's a<b/a==b/a>b. Falls back to 0 on error."""
    for op, ret in (("lt", -1), ("eq", 0), ("gt", 1)):
        proc = await asyncio.create_subprocess_exec(
            "dpkg", "--compare-versions", a, op, b,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        rc = await proc.wait()
        if rc == 0:
            return ret
    return 0


async def _installed_version(package: str) -> str | None:
    """Query dpkg for the installed version. None if not installed."""
    try:
        proc = await asyncio.create_subprocess_exec(
            "dpkg-query", "-W", "-f=${Version}", package,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        out, _ = await asyncio.wait_for(proc.communicate(), timeout=5.0)
    except (TimeoutError, FileNotFoundError):
        return None
    if proc.returncode != 0:
        return None
    return (out.decode(errors="replace").strip()) or None


def _parse_packages_gz(blob: bytes, package: str) -> list[str]:
    """Parse Debian Packages.gz blob, return all versions for ``package``."""
    text = gzip.decompress(blob).decode("utf-8", errors="replace")
    versions: list[str] = []
    current: dict[str, str] = {}
    for line in text.splitlines():
        if not line.strip():
            if current.get("Package") == package and "Version" in current:
                versions.append(current["Version"])
            current = {}
            continue
        if ":" in line and not line.startswith(" "):
            k, _, v = line.partition(":")
            current[k.strip()] = v.strip()
    if current.get("Package") == package and "Version" in current:
        versions.append(current["Version"])
    return versions


async def _fetch_packages_blob(url: str, timeout_sec: float) -> bytes:
    """Fetch the Packages.gz blob in a worker thread.

    urllib is sync; running in a thread keeps the event loop responsive
    if the repo CDN is slow. The fetch itself is rare (hourly probe),
    so we don't bother adding aiohttp to the dependency set just for
    one URL.
    """
    def _do() -> bytes:
        req = urllib.request.Request(url, headers={"User-Agent": "monad-ops/version-watch"})
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:  # noqa: S310
            return resp.read()
    return await asyncio.to_thread(_do)


async def _pick_latest_stable(
    versions: Iterable[str],
    skip_substrings: tuple[str, ...],
) -> str | None:
    candidates = [v for v in versions if not any(s in v for s in skip_substrings)]
    if not candidates:
        candidates = list(versions)
    if not candidates:
        return None
    best = candidates[0]
    for v in candidates[1:]:
        if await _dpkg_compare(v, best) > 0:
            best = v
    return best


async def fetch_version_status(
    *,
    package: str,
    packages_url: str,
    skip_substrings: tuple[str, ...] = _DEFAULT_SKIP_SUBSTRINGS,
    timeout_sec: float = 20.0,
) -> VersionStatus:
    """One-shot fetch + comparison. Never raises — encodes failures
    into ``status="unknown"`` + ``error`` so a transient repo blip
    doesn't kill the calling loop.
    """
    installed = await _installed_version(package)
    if installed is None:
        return VersionStatus(
            package=package, installed=None, latest=None,
            extras_newer=(), status="unknown",
            error=f"package {package!r} not installed",
        )

    try:
        blob = await _fetch_packages_blob(packages_url, timeout_sec)
    except Exception as e:  # noqa: BLE001
        return VersionStatus(
            package=package, installed=installed, latest=None,
            extras_newer=(), status="unknown",
            error=f"repo fetch failed: {e}",
        )

    try:
        all_versions = _parse_packages_gz(blob, package)
    except Exception as e:  # noqa: BLE001
        return VersionStatus(
            package=package, installed=installed, latest=None,
            extras_newer=(), status="unknown",
            error=f"repo parse failed: {e}",
        )

    if not all_versions:
        return VersionStatus(
            package=package, installed=installed, latest=None,
            extras_newer=(), status="unknown",
            error="no versions parsed from repo",
        )

    latest = await _pick_latest_stable(all_versions, skip_substrings)
    if latest is None:
        return VersionStatus(
            package=package, installed=installed, latest=None,
            extras_newer=(), status="unknown",
            error="no stable versions in repo",
        )

    # Newer-than-installed list (deduped, descending). Used for the
    # dashboard popup so an operator can see what they'd actually be
    # picking up — useful when several patch releases stack up.
    newer: list[str] = []
    seen: set[str] = set()
    for v in all_versions:
        if v in seen:
            continue
        seen.add(v)
        if await _dpkg_compare(v, installed) > 0:
            newer.append(v)
    # Sort descending by dpkg rules. Bubble-sort is fine for ≤ tens of items.
    for i in range(len(newer)):
        for j in range(i + 1, len(newer)):
            if await _dpkg_compare(newer[j], newer[i]) > 0:
                newer[i], newer[j] = newer[j], newer[i]

    cmp = await _dpkg_compare(latest, installed)
    if cmp <= 0:
        return VersionStatus(
            package=package, installed=installed, latest=latest,
            extras_newer=(), status="up_to_date", error=None,
        )
    return VersionStatus(
        package=package, installed=installed, latest=latest,
        extras_newer=tuple(newer), status="update_available", error=None,
    )
