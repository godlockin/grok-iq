"""Persistence for keep-alive scheduling state and request evidence.

Deliberately separate from :mod:`app.persistence.probe_repository` so a
keep-alive request can never be mistaken for probe evidence by the scoring,
dashboard, or retention paths that read probe tables.
"""

from __future__ import annotations

import random
import uuid
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import func, select

from app.core.clock import utc_now

from .database import Database
from .models import KeepAliveAccount, KeepAliveRun


class KeepAliveRepository:
    """Owns keep-alive rows. All mutating calls run in one transaction."""

    def __init__(self, database: Database):
        self.database = database

    def sync_accounts(
        self,
        accounts: list[dict[str, Any]],
        *,
        min_interval_seconds: int,
        max_interval_seconds: int,
    ) -> dict[str, int]:
        """Reconcile tracked accounts with the current enabled upstream set.

        Accounts disabled or removed upstream disappear from the table so they
        stop consuming tick slots. Newly eligible accounts are inserted with a
        randomized first due time instead of firing immediately, which would
        otherwise produce one synchronized burst after every restart.
        """

        now = utc_now()
        tracked = {int(value.get("id") or 0) for value in accounts if int(value.get("id") or 0) > 0}
        with self.database.transaction() as session:
            existing = {
                int(row.account_id): row for row in session.scalars(select(KeepAliveAccount)).all()
            }
            removed = set(existing) - tracked
            for account_id in removed:
                session.delete(existing[account_id])
            added = 0
            for account in accounts:
                account_id = int(account.get("id") or 0)
                if account_id <= 0 or account_id in existing:
                    continue
                session.add(
                    KeepAliveAccount(
                        account_id=account_id,
                        account_name=str(account.get("name") or ""),
                        account_email=str(account.get("email") or ""),
                        next_due_at=self._spread_due(
                            now,
                            min_interval_seconds=min_interval_seconds,
                            max_interval_seconds=max_interval_seconds,
                        ),
                    )
                )
                added += 1
        return {"tracked": len(tracked), "added": added, "removed": len(removed)}

    def claim_due(self, *, limit: int, now: datetime | None = None) -> list[KeepAliveAccount]:
        """Return accounts whose randomized due time has arrived.

        The caller re-arms ``next_due_at`` after each attempt, so an account that
        fails is naturally pushed into its next randomized window instead of
        being retried on the following tick.
        """

        moment = now or utc_now()
        with self.database.session() as session:
            statement = (
                select(KeepAliveAccount)
                .where(KeepAliveAccount.next_due_at <= moment)
                .order_by(KeepAliveAccount.next_due_at.asc())
                .limit(max(1, int(limit)))
            )
            return list(session.scalars(statement).all())

    def mark_sent(
        self,
        account_id: int,
        *,
        min_interval_seconds: int,
        max_interval_seconds: int,
    ) -> None:
        """Re-arm an account into a new random slot after a request attempt."""

        now = utc_now()
        with self.database.transaction() as session:
            row = session.get(KeepAliveAccount, account_id)
            if row is None:
                return
            row.last_sent_at = now
            row.next_due_at = self._spread_due(
                now,
                min_interval_seconds=min_interval_seconds,
                max_interval_seconds=max_interval_seconds,
            )

    def record_success(self, account_id: int) -> None:
        with self.database.transaction() as session:
            row = session.get(KeepAliveAccount, account_id)
            if row is None:
                return
            row.success_count = int(row.success_count or 0) + 1
            row.last_error = ""
            row.skip_until = None

    def record_failure(
        self,
        account_id: int,
        message: str,
        *,
        backoff_seconds: int,
    ) -> None:
        """Record a failure and hold the account out for a backoff period.

        Cooling or disabled accounts would otherwise be retried every tick,
        burning the tick budget and the account's remaining quota.
        """

        with self.database.transaction() as session:
            row = session.get(KeepAliveAccount, account_id)
            if row is None:
                return
            row.failure_count = int(row.failure_count or 0) + 1
            row.last_error = str(message)[:2000]
            row.skip_until = utc_now() + timedelta(seconds=max(0, int(backoff_seconds)))

    def record_run(
        self,
        *,
        account_id: int,
        account_name: str,
        account_email: str,
        request_id: str,
        status: str,
        status_code: int = 0,
        error_code: str = "",
        audit_id: int | None = None,
        prompt: str = "",
        temperature: float = 0.0,
        output_tokens: int = 0,
        duration_ms: int = 0,
        error: str = "",
    ) -> str:
        run_id = uuid.uuid4().hex
        with self.database.transaction() as session:
            session.add(
                KeepAliveRun(
                    id=run_id,
                    account_id=account_id,
                    account_name=account_name,
                    account_email=account_email,
                    request_id=request_id,
                    audit_id=audit_id,
                    status=status,
                    status_code=status_code,
                    error_code=error_code,
                    prompt=prompt,
                    temperature=temperature,
                    output_tokens=output_tokens,
                    duration_ms=duration_ms,
                    error=error,
                )
            )
        return run_id

    def summary(self, *, lookback_hours: int = 24) -> dict[str, Any]:
        since = utc_now() - timedelta(hours=max(1, int(lookback_hours)))
        with self.database.session() as session:
            tracked = int(
                session.scalar(select(func.count(KeepAliveAccount.account_id))) or 0
            )
            rows = session.execute(
                select(KeepAliveRun.status, func.count(KeepAliveRun.id))
                .where(KeepAliveRun.created_at >= since)
                .group_by(KeepAliveRun.status)
            ).all()
        counts = {str(status or "unknown"): int(total or 0) for status, total in rows}
        succeeded = sum(total for status, total in counts.items() if status == "succeeded")
        failed = sum(total for status, total in counts.items() if status != "succeeded")
        return {
            "trackedAccounts": tracked,
            "lookbackHours": max(1, int(lookback_hours)),
            "succeeded": succeeded,
            "failed": failed,
            "byStatus": counts,
        }

    @staticmethod
    def _spread_due(
        now: datetime,
        *,
        min_interval_seconds: int,
        max_interval_seconds: int,
    ) -> datetime:
        """Pick a due time uniformly inside ``[min, max]`` from now.

        Uniform sampling over a wide band is what breaks the synchronized
        pattern a fixed interval produces. The band is clamped so a bad
        configuration cannot collapse to a busy loop or stall for a day.
        """

        low = max(30, int(min_interval_seconds))
        high = max(low, int(max_interval_seconds))
        return now + timedelta(seconds=random.randint(low, high))
