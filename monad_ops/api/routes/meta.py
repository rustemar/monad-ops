"""Site metadata endpoints: health check, manifest, robots, sitemap, security.txt, favicon.

Moved out of ``build_app`` unchanged (queue item R1, slice 5). They need nothing
from the app but the directory the files live in, so that is all they take.
"""

from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter
from fastapi.responses import FileResponse, JSONResponse


def build_router(static_dir: Path) -> APIRouter:
    """Metadata routes serving files from ``static_dir``."""
    router = APIRouter()

    @router.api_route("/healthz", methods=["GET", "HEAD"])
    async def healthz() -> JSONResponse:
        return JSONResponse({"ok": True})

    @router.api_route("/manifest.json", methods=["GET", "HEAD"])
    async def manifest_json() -> FileResponse:
        return FileResponse(
            static_dir / "manifest.json", media_type="application/manifest+json"
        )

    @router.api_route("/robots.txt", methods=["GET", "HEAD"])
    async def robots_txt() -> FileResponse:
        return FileResponse(static_dir / "robots.txt", media_type="text/plain")

    @router.api_route("/sitemap.xml", methods=["GET", "HEAD"])
    async def sitemap_xml() -> FileResponse:
        return FileResponse(static_dir / "sitemap.xml", media_type="application/xml")

    @router.api_route("/.well-known/security.txt", methods=["GET", "HEAD"])
    async def security_txt() -> FileResponse:
        return FileResponse(
            static_dir / ".well-known" / "security.txt",
            media_type="text/plain",
        )

    @router.api_route("/favicon.ico", methods=["GET", "HEAD"])
    async def favicon() -> FileResponse:
        # Serve the SVG for the legacy /favicon.ico path so browsers that
        # preflight it before parsing the HTML <link> don't log a 404.
        return FileResponse(
            static_dir / "favicon.svg", media_type="image/svg+xml"
        )

    return router
