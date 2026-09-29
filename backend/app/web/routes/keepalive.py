from __future__ import annotations

from typing import Any

from fastapi import APIRouter

from app.services.keepalive import KeepAliveService


def build_keepalive_router(keepalive: KeepAliveService) -> APIRouter:
    """Expose keep-alive state and a manual tick.

    Keep-alive evidence is operational only: it is kept out of the probe
    dashboards and never contributes to account scoring, so these endpoints
    report raw counts rather than health verdicts.
    """

    router = APIRouter(prefix="/keepalive")

    @router.get("/status")
    def get_status() -> dict[str, Any]:
        return keepalive.summary()

    @router.post("/run")
    async def run_once() -> dict[str, Any]:
        # Useful after changing the interval window: operators do not have to
        # wait for the next randomized due time to see the new settings work.
        return await keepalive.tick()

    return router
