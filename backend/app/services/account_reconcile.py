"""Reconcile GrokIQ verdicts against the live grok2api account set.

grok2api is the source of truth for which accounts exist. GrokIQ stores its
verdict under a bare integer key with no foreign key, so an account deleted
upstream leaves a verdict behind. Those rows are not merely stale: every
operator action on one (restore, isolate, re-probe) fails with HTTP 404, and
the isolation-zone listing counts them as live problems.

The reconcile pass deletes GrokIQ-owned state for accounts that no longer
exist. It deliberately keeps probe samples and request-audit rows, which are
historical evidence for trend analysis and are never presented as a live
account.
"""

from __future__ import annotations

import logging
from typing import Any

from app.integrations.grok2api.client import Grok2APIClient
from app.persistence.account_repository import AccountRepository

logger = logging.getLogger(__name__)

# Providers GrokIQ assesses. grok_console is included so a console account is
# not mistaken for a deleted grok_build account.
_RECONCILED_PROVIDERS = ("grok_build", "grok_web", "grok_console")


class AccountReconcileService:
    """Removes GrokIQ verdicts for accounts that no longer exist upstream."""

    def __init__(
        self,
        *,
        client: Grok2APIClient,
        accounts: AccountRepository,
    ) -> None:
        self.client = client
        self.accounts = accounts

    async def reconcile(self) -> dict[str, Any]:
        """Run one reconcile pass.

        Returns ``skipped`` rather than deleting anything when the upstream
        listing cannot be read. An empty list is indistinguishable from "the
        whole pool was deleted", and treating it as authoritative would wipe
        every verdict during a grok2api outage.
        """

        live: set[int] = set()
        seen_providers = 0
        for provider in _RECONCILED_PROVIDERS:
            try:
                rows = await self.client.list_all_accounts(provider=provider)
            except Exception:
                logger.exception("account reconcile could not list provider %s", provider)
                return {
                    "ok": False,
                    "skipped": "upstream_unavailable",
                    "liveAccounts": len(live),
                    "removed": 0,
                }
            if not isinstance(rows, list):
                return {
                    "ok": False,
                    "skipped": "upstream_unavailable",
                    "liveAccounts": len(live),
                    "removed": 0,
                }
            seen_providers += 1
            live.update(
                int(row.get("id") or 0)
                for row in rows
                if isinstance(row, dict) and int(row.get("id") or 0) > 0
            )

        if seen_providers == 0 or not live:
            return {
                "ok": False,
                "skipped": "upstream_empty",
                "liveAccounts": 0,
                "removed": 0,
            }

        ghosts = self.accounts.ghost_account_ids(live)
        if not ghosts:
            return {
                "ok": True,
                "liveAccounts": len(live),
                "ghostAccounts": 0,
                "removed": 0,
                "ghostAccountIds": [],
            }

        removed = self.accounts.purge_accounts(ghosts)
        logger.info(
            "account reconcile removed %s verdicts for %s upstream-missing accounts",
            removed,
            len(ghosts),
        )
        return {
            "ok": True,
            "liveAccounts": len(live),
            "ghostAccounts": len(ghosts),
            "removed": removed,
            "ghostAccountIds": ghosts[:200],
        }
