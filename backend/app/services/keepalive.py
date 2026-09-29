"""Randomized keep-alive warming for upstream grok_build accounts.

Purpose
-------
Keep dormant accounts from looking idle to the upstream scheduler by issuing
low-cost chat traffic at randomized times with randomized content.

Why this is not the probe pipeline
----------------------------------
A probe run is evidence. Its sample feeds degradation scoring, account health,
risk rules, and the probe dashboards, and it must carry a deterministic marker
so a degraded model can be detected. Keep-alive has none of those
requirements, so mixing the two would corrupt scoring and make a warm request
indistinguishable from a real check. This service therefore owns its own
tables, its own prompts, and its own lifecycle.

Randomization
-------------
Three independent axes, each configurable off:
  * timing   - every account is rescheduled at a uniform random point inside
               ``[keepalive_min_interval_seconds, keepalive_max_interval_seconds]``
  * content  - topic, phrasing, opener, closer, and a nonce all vary per request
  * params   - temperature is sampled per request

The randomized per-account due time is persisted, so the spread survives a
restart instead of collapsing into one burst on the first tick.
"""

from __future__ import annotations

import asyncio
import logging
import random
from typing import Any

from app.core.clock import utc_now
from app.core.config import Settings
from app.integrations.grok2api.client import Grok2APIClient, IntegrationError
from app.persistence.keepalive_repository import KeepAliveRepository

from .keepalive_prompts import render_prompt

logger = logging.getLogger(__name__)

# Never keep more than this many in-flight upstream requests. Keep-alive is
# best-effort; it must not compete with real user traffic for the same accounts.
_MAX_IN_FLIGHT = 32


class KeepAliveService:
    """Schedules and issues keep-alive traffic on a randomized cadence."""

    def __init__(
        self,
        *,
        settings: Settings,
        repository: KeepAliveRepository,
        client: Grok2APIClient,
    ):
        self.settings = settings
        self.repository = repository
        self.client = client
        self._task: asyncio.Task[None] | None = None
        self._wake = asyncio.Event()
        self._stopping = False
        self._semaphore = asyncio.Semaphore(_MAX_IN_FLIGHT)

    # ---------------------------------------------------------------- lifecycle
    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._stopping = False
        self._task = asyncio.create_task(self._loop(), name="keepalive")

    async def stop(self) -> None:
        self._stopping = True
        self._wake.set()
        task = self._task
        self._task = None
        if task is None:
            return
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    def wake(self) -> None:
        """Ask the loop to re-evaluate immediately after a settings change."""

        self._wake.set()

    async def _loop(self) -> None:
        while not self._stopping:
            try:
                await self.tick()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("keepalive tick failed")
            delay = self._tick_delay()
            self._wake.clear()
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=delay)
            except TimeoutError:
                pass

    def _tick_delay(self) -> float:
        return max(10, int(self.settings.keepalive_tick_seconds))

    # ------------------------------------------------------------------ tick
    async def tick(self) -> dict[str, Any]:
        """Run one scheduling pass. Safe to call directly for a manual run."""

        if not self.settings.keepalive_enabled:
            return {"skipped": "disabled", "attempted": 0, "succeeded": 0, "failed": 0}
        accounts = await self._enabled_accounts()
        if not accounts:
            return {"skipped": "no_enabled_accounts", "attempted": 0, "succeeded": 0, "failed": 0}
        sync = self.repository.sync_accounts(
            accounts,
            min_interval_seconds=self.settings.keepalive_min_interval_seconds,
            max_interval_seconds=self.settings.keepalive_max_interval_seconds,
        )
        due = self.repository.claim_due(limit=self.settings.keepalive_batch_size)
        due = [row for row in due if not self._is_backed_off(row)]
        if not due:
            return {"skipped": "none_due", "tracked": sync["tracked"], "attempted": 0, "succeeded": 0, "failed": 0}
        results = await self._run_batch(due)
        logger.info(
            "keepalive tick tracked=%s added=%s removed=%s due=%s ok=%s failed=%s",
            sync["tracked"],
            sync["added"],
            sync["removed"],
            len(due),
            results["succeeded"],
            results["failed"],
        )
        return {**results, "tracked": sync["tracked"]}

    async def _enabled_accounts(self) -> list[dict[str, Any]]:
        """List enabled upstream accounts, tolerating a grok2api outage.

        A failed listing must not wipe the tracked set: dropping every row
        would silently stop keep-alive until the next successful sync, and the
        rows themselves hold the randomized due times.
        """

        try:
            accounts = await self.client.list_all_accounts()
        except Exception:
            logger.exception("keepalive could not list upstream accounts; keeping existing schedule")
            return []
        return [
            account
            for account in accounts
            if bool(account.get("enabled")) and int(account.get("id") or 0) > 0
        ]

    @staticmethod
    def _is_backed_off(row: Any) -> bool:
        skip_until = getattr(row, "skip_until", None)
        if skip_until is None:
            return False
        return bool(skip_until > utc_now())

    async def _run_batch(self, due: list[Any]) -> dict[str, Any]:
        concurrency = max(1, int(self.settings.keepalive_worker_concurrency))
        limiter = asyncio.Semaphore(concurrency)

        async def worker(row: Any) -> bool:
            async with limiter:
                async with self._semaphore:
                    return await self._warm_one(row)

        outcomes = await asyncio.gather(*(worker(row) for row in due))
        succeeded = sum(1 for value in outcomes if value)
        return {
            "attempted": len(outcomes),
            "succeeded": succeeded,
            "failed": len(outcomes) - succeeded,
        }

    # -------------------------------------------------------------- one request
    async def _warm_one(self, row: Any) -> bool:
        account_id = int(row.account_id)
        account_name = str(row.account_name or "")
        account_email = str(row.account_email or "")
        # Re-arm before the request so a slow or hanging call cannot cause the
        # account to be picked up again by the next tick.
        self.repository.mark_sent(
            account_id,
            min_interval_seconds=self.settings.keepalive_min_interval_seconds,
            max_interval_seconds=self.settings.keepalive_max_interval_seconds,
        )
        drawn = render_prompt(
            random.Random(),
            max_output_tokens=self.settings.keepalive_max_output_tokens,
        )
        route_id = ""
        client_key_id = ""
        try:
            route_id, public_model = await self.client.create_probe_route(
                account_id=account_id,
                upstream_model=self.settings.keepalive_model,
                # Keep-alive targets accounts that may be cooling or dormant;
                # that is precisely the state it exists to warm, so an
                # unavailable route must not abort the attempt.
                allow_temporarily_unavailable=True,
                bind_account=True,
            )
            client_key_id, api_key = await self.client.create_probe_client_key(route_id)
            result = await self.client.chat_probe(
                api_key=api_key,
                public_model=public_model,
                account_id=account_id,
                system_prompt=drawn.system_prompt,
                prompt=drawn.prompt,
                # No marker: keep-alive is not a correctness check.
                expected="",
                max_output_tokens=drawn.max_output_tokens,
                temperature=drawn.temperature,
                extra_body={},
            )
        except IntegrationError as exc:
            self.repository.record_run(
                account_id=account_id,
                account_name=account_name,
                account_email=account_email,
                request_id=getattr(exc, "request_id", "") or "",
                status="failed",
                status_code=int(getattr(exc, "status_code", 0) or 0),
                error_code=str(getattr(exc, "error_code", "") or ""),
                prompt=drawn.prompt,
                temperature=drawn.temperature,
                error=str(exc)[:2000],
            )
            self.repository.record_failure(
                account_id,
                str(exc),
                backoff_seconds=self._backoff_for(exc),
            )
            logger.info(
                "keepalive failed account=%s code=%s status=%s",
                account_id,
                getattr(exc, "error_code", "") or "-",
                getattr(exc, "status_code", 0),
            )
            return False
        except Exception as exc:
            self.repository.record_run(
                account_id=account_id,
                account_name=account_name,
                account_email=account_email,
                request_id="",
                status="failed",
                prompt=drawn.prompt,
                temperature=drawn.temperature,
                error=str(exc)[:2000],
            )
            self.repository.record_failure(
                account_id,
                str(exc),
                backoff_seconds=self.settings.keepalive_failure_backoff_seconds,
            )
            logger.exception("keepalive unexpected failure account=%s", account_id)
            return False
        finally:
            # Both the route and the client key are per-request resources.
            # Leaking either would grow grok2api's model and client-key tables
            # without bound, because keep-alive runs continuously.
            if client_key_id:
                try:
                    await self.client.delete_probe_client_key(client_key_id)
                except Exception:
                    logger.warning(
                        "keepalive could not delete client key for account=%s",
                        account_id,
                        exc_info=True,
                    )
            if route_id:
                try:
                    await self.client.delete_probe_route(route_id)
                except Exception:
                    # A leaked route only leaves an idle model row; it must not
                    # fail an otherwise successful warm request.
                    logger.warning("keepalive could not delete route %s", route_id, exc_info=True)
        self.repository.record_run(
            account_id=account_id,
            account_name=account_name,
            account_email=account_email,
            request_id=result.request_id,
            audit_id=result.audit_id,
            status="succeeded",
            status_code=result.status_code,
            prompt=drawn.prompt,
            temperature=drawn.temperature,
            output_tokens=result.output_tokens,
            duration_ms=result.duration_ms,
        )
        self.repository.record_success(account_id)
        return True

    def _backoff_for(self, error: IntegrationError) -> int:
        """Back off longer when the upstream told us when to retry."""

        configured = int(self.settings.keepalive_failure_backoff_seconds)
        try:
            upstream = max(0.0, float(getattr(error, "retry_after_seconds", 0.0) or 0.0))
        except (TypeError, ValueError):
            upstream = 0.0
        if upstream <= 0:
            return configured
        return min(int(upstream) + 30, 24 * 3600)

    def summary(self) -> dict[str, Any]:
        return {
            "enabled": self.settings.keepalive_enabled,
            "tickSeconds": self._tick_delay(),
            **self.repository.summary(),
        }
