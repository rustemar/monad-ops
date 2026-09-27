"""Node info endpoints: the host probes, the installed package version and the
active validator set.

Moved out of ``build_app`` unchanged (queue item R1). ``/api/changes`` stays in
``app.py`` with the git helpers it reads.
"""

from __future__ import annotations

import re

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from monad_ops.api.context import ApiContext

# The probes loop runs every ~30 s host-side.
_PROBES_TTL = 60.0
# version_watch runs hourly; a short cache surfaces an upgrade fast.
_VERSION_TTL = 30.0


def _sanitize_probe_summary(name: str, summary: str) -> str:
    """Strip exact port numbers, ulimit values and percentages from
    probe summaries. Keeps the status signal without leaking
    host-configuration detail."""
    if name == "udp_config":
        # We never want the port number or the "config not readable"
        # caveat — just health.
        if "authenticated UDP" in summary:
            return "authenticated UDP listener healthy"
        return summary
    if name == "fd_limits":
        return re.sub(r"nofile soft=\d+ hard=\d+", "fd limits within safe margin", summary)
    if name == "disk_usage":
        return re.sub(r"\(peak [\d.]+%?\)", "(peak <20%)", summary)
    return summary


def build_router(ctx: ApiContext) -> APIRouter:
    """Node info routes bound to ``ctx``."""
    router = APIRouter()

    async def _api_probes_payload() -> dict:
        """Sanitized host-probe payload — name, status, summary only.

        The earlier two-tier architecture (`/api/probes` with `details` +
        `/api/probes/public` sanitized) was retired 2026-05-03: the
        `/public` suffix implied a `/private` counterpart that no longer
        exists, and the operator-sensitive `details` field (key-backup
        paths, `/dev/nvme<N>p<N>`, ulimit values) is information the
        operator already has via shell access. The single endpoint here
        is what the dashboard renders and what external clients see.
        """
        probes, ran_at = ctx.state.probes()
        return {
            "ran_at": ran_at,
            "probes": [
                {
                    "name": p.name,
                    "status": p.status,
                    "summary": _sanitize_probe_summary(p.name, p.summary),
                }
                for p in probes
            ],
        }

    @router.api_route("/api/probes", methods=["GET", "HEAD"])
    async def api_probes() -> JSONResponse:
        payload = await ctx.cached("probes", _PROBES_TTL, (), _api_probes_payload)
        return JSONResponse(payload)

    @router.api_route("/api/probes/public", methods=["GET", "HEAD"])
    async def api_probes_public() -> JSONResponse:
        """Backwards-compat alias of /api/probes.

        Pre-2026-05-03 callers (README + bookmarks) hit /public. Returns
        the same payload as /api/probes so nothing breaks during the
        transition; can be removed once external references migrate.
        """
        payload = await ctx.cached("probes", _PROBES_TTL, (), _api_probes_payload)
        return JSONResponse(payload)

    @router.api_route("/api/version", methods=["GET", "HEAD"])
    async def api_version() -> JSONResponse:
        """Locally-installed monad package vs. apt repo.

        Powers the "node version" tile on the dashboard. Returns the
        same shape whether the operator is up to date, has an upgrade
        pending, or the probe could not run — the UI special-cases
        each ``status`` value.

        Public-safe: contains only package name + version strings + the
        configured repo URL — no host paths, no PIDs, no metadata about
        the host OS.
        """
        async def _load():
            status, checked_at = ctx.state.version()
            if status is None:
                return {
                    "package": ctx.config.version_watch.package,
                    "installed": None,
                    "latest": None,
                    "extras_newer": [],
                    "status": "unknown",
                    "error": "version_watch has not run yet",
                    "checked_at": None,
                    "pending_since": None,
                    "packages_url": ctx.config.version_watch.packages_url,
                    "enabled": ctx.config.version_watch.enabled,
                }
            return {
                "package": status.package,
                "installed": status.installed,
                "latest": status.latest,
                "extras_newer": list(status.extras_newer),
                "status": status.status,
                "error": status.error,
                "checked_at": checked_at,
                "pending_since": (
                    ctx.state.version_pending_since(status.latest)
                    if status.status == "update_available" else None
                ),
                "packages_url": ctx.config.version_watch.packages_url,
                "enabled": ctx.config.version_watch.enabled,
            }
        payload = await ctx.cached("version", _VERSION_TTL, (), _load)
        return JSONResponse(payload)

    @router.api_route("/api/validator_set", methods=["GET", "HEAD"])
    async def api_validator_set() -> JSONResponse:
        """Active validator-set snapshot from the staking precompile.

        Powers the "active validator set" tile on the dashboard. The
        snapshot itself changes on epoch boundaries (~5.5h on testnet);
        polled every 5 min by ``validator_set_loop`` in cli.py.

        Public-safe: returns only protocol constants + on-chain counts
        + the cutoff stake (already public via the precompile). No host
        metadata or peer-level identity.
        """
        async def _load():
            snapshot, checked_at = ctx.state.validator_set()
            if snapshot is None:
                return {
                    "enabled": ctx.config.validator_set.enabled,
                    "status": "unknown",
                    "error": "validator_set probe has not run yet",
                    "checked_at": None,
                    "epoch": None,
                    "in_epoch_delay": False,
                    "consensus_count": None,
                    "execution_count": None,
                    "bench_count": None,
                    "lowest_active_stake_wei": None,
                    "active_valset_cap": 200,
                    "active_validator_stake_mon": 10_000_000,
                    "min_auth_address_stake_mon": 100_000,
                }
            return {
                "enabled": ctx.config.validator_set.enabled,
                "status": snapshot.status,
                "error": snapshot.error,
                "checked_at": checked_at,
                "epoch": snapshot.epoch,
                "in_epoch_delay": snapshot.in_epoch_delay,
                "consensus_count": snapshot.consensus_count,
                "execution_count": snapshot.execution_count,
                "bench_count": snapshot.bench_count,
                "lowest_active_stake_wei": (
                    str(snapshot.lowest_active_stake_wei)
                    if snapshot.lowest_active_stake_wei is not None else None
                ),
                "active_valset_cap": 200,
                "active_validator_stake_mon": 10_000_000,
                "min_auth_address_stake_mon": 100_000,
            }
        payload = await ctx.cached("validator_set", 30.0, (), _load)
        return JSONResponse(payload)

    return router
