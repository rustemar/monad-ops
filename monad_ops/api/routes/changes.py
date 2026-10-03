"""Recent checkout commits and the version running in this process.

Moved out of ``build_app`` unchanged (queue item R1). The shared checkout
module owns git helpers and process snapshots; the app owns the cache.
"""

from __future__ import annotations

import asyncio

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from monad_ops.api import checkout
from monad_ops.api.context import ApiContext


def build_router(ctx: ApiContext) -> APIRouter:
    """Changes route bound to ``ctx`` and shared checkout metadata."""
    router = APIRouter()
    _CHANGES_TTL = 60.0       # git log of the checkout; changes once a day at most

    @router.api_route("/api/changes", methods=["GET", "HEAD"])
    async def api_changes() -> JSONResponse:
        """Recent commits of this monad-ops checkout and the commit the process runs.

        Powers the "recent changes" card: a visitor sees the dashboard is
        maintained, the operator sees when the checkout has moved past what
        is running. Public-safe: short hashes, commit times and subjects.
        ``enabled`` is false when the code is not a git checkout.
        """
        def _read_git() -> tuple[list[dict] | None, str | None, str | None]:
            return checkout._git_recent_commits(8), checkout._git_head(), checkout._git_head_full()

        async def _load():
            commits, head, head_full = await asyncio.to_thread(_read_git)
            repo_url = (
                (ctx.config.operator.public_repo or None) if ctx.config.operator.enabled else None
            )
            return {
                "enabled": commits is not None,
                "error": None if commits is not None else "not a git checkout",
                "repo_url": repo_url,
                "running": {"commit": checkout._RUNNING_COMMIT, "started_at": checkout._STARTED_AT},
                "head": head,
                # Full hashes: short ones can grow as the object count does.
                "restart_pending": bool(
                    head_full and checkout._RUNNING_COMMIT_FULL
                    and head_full != checkout._RUNNING_COMMIT_FULL
                ),
                "commits": commits or [],
            }
        payload = await ctx.cached("changes", _CHANGES_TTL, (), _load)
        return JSONResponse(payload)

    return router
