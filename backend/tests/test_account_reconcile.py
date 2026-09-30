from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from app.core.clock import utc_now
from app.persistence.account_repository import AccountRepository
from app.persistence.database import Database
from app.persistence.models import (
    AccountAssessment,
    Alert,
    ProbeProfile,
    ProbeRun,
    ProbeSample,
)
from app.services.account_reconcile import AccountReconcileService


class ReconcileClient:
    """Stands in for the grok2api admin client."""

    def __init__(self, accounts: dict[str, list[dict[str, Any]]] | None = None) -> None:
        self.accounts = accounts or {}
        self.fail_providers: set[str] = set()

    async def list_all_accounts(self, provider: str = "", **_: Any) -> list[dict[str, Any]]:
        if provider in self.fail_providers:
            raise RuntimeError(f"{provider} unavailable")
        return list(self.accounts.get(provider, []))


def _seed(database: Database, account_ids: list[int]) -> None:
    with database.transaction() as session:
        for account_id in account_ids:
            session.add(
                AccountAssessment(
                    account_id=account_id,
                    monitor_status="quarantined",
                    risk_score=80.0,
                )
            )


def _live(*account_ids: int) -> dict[str, list[dict[str, Any]]]:
    return {
        "grok_build": [{"id": str(value)} for value in account_ids],
        "grok_web": [],
        "grok_console": [],
    }


@pytest.mark.asyncio
async def test_reconcile_removes_only_missing_verdicts(tmp_path: Path) -> None:
    database = Database(tmp_path / "grokiq.db")
    database.initialize()
    _seed(database, [1, 2, 3, 4])

    repository = AccountRepository(database)
    service = AccountReconcileService(
        client=ReconcileClient(_live(1, 2)),  # type: ignore[arg-type]
        accounts=repository,
    )

    result = await service.reconcile()

    assert result["ok"] is True
    assert result["ghostAccounts"] == 2
    assert result["removed"] == 2
    assert repository.get_assessment(1) is not None
    assert repository.get_assessment(2) is not None
    assert repository.get_assessment(3) is None
    assert repository.get_assessment(4) is None
    database.dispose()


@pytest.mark.asyncio
async def test_reconcile_keeps_probe_evidence(tmp_path: Path) -> None:
    database = Database(tmp_path / "grokiq.db")
    database.initialize()
    _seed(database, [7])
    now = utc_now()
    with database.transaction() as session:
        # probe_runs.profile_id is a foreign key, so the profile must exist.
        session.add(
            ProbeProfile(
                id="quality-marker",
                name="指令遵循基线",
                model="grok-4.5",
                prompt="只输出指定标记",
            )
        )
        run = ProbeRun(
            id="run-ghost",
            account_id=7,
            profile_id="quality-marker",
            status="completed",
            rounds=1,
            proxy_targets=[],
            total_steps=1,
        )
        session.add(run)
        session.add(
            ProbeSample(
                id="sample-ghost",
                run_id="run-ghost",
                account_id=7,
                round_number=1,
                target_key="current",
                target_kind="current",
                status="done",
                created_at=now,
            )
        )
        session.add(
            Alert(
                id="alert-ghost",
                account_id=7,
                kind="auto_quarantine",
                severity="critical",
                title="账号已被自动停用",
            )
        )

    repository = AccountRepository(database)
    service = AccountReconcileService(
        client=ReconcileClient(_live(9)),  # type: ignore[arg-type]
        accounts=repository,
    )
    result = await service.reconcile()

    assert result["ghostAccountIds"] == [7]
    assert repository.get_assessment(7) is None
    with database.session() as session:
        # The alert describes state that no longer exists, so it goes.
        assert session.query(Alert).filter(Alert.account_id == 7).count() == 0
        # Probe history is evidence and is never presented as a live account,
        # so the run and its samples both survive.
        assert session.get(ProbeRun, "run-ghost") is not None
        assert session.query(ProbeSample).filter(ProbeSample.account_id == 7).count() == 1
    database.dispose()


@pytest.mark.asyncio
async def test_reconcile_is_noop_when_everything_exists(tmp_path: Path) -> None:
    database = Database(tmp_path / "grokiq.db")
    database.initialize()
    _seed(database, [1, 2])

    repository = AccountRepository(database)
    service = AccountReconcileService(
        client=ReconcileClient(_live(1, 2)),  # type: ignore[arg-type]
        accounts=repository,
    )
    result = await service.reconcile()

    assert result["ok"] is True
    assert result["ghostAccounts"] == 0
    assert result["removed"] == 0
    database.dispose()


@pytest.mark.asyncio
async def test_reconcile_skips_when_upstream_unavailable(tmp_path: Path) -> None:
    """A grok2api outage must never look like "the whole pool was deleted"."""

    database = Database(tmp_path / "grokiq.db")
    database.initialize()
    _seed(database, [1, 2, 3])

    client = ReconcileClient(_live(1))
    client.fail_providers.add("grok_build")
    repository = AccountRepository(database)
    service = AccountReconcileService(client=client, accounts=repository)  # type: ignore[arg-type]

    result = await service.reconcile()

    assert result["ok"] is False
    assert result["skipped"] == "upstream_unavailable"
    assert result["removed"] == 0
    for account_id in (1, 2, 3):
        assert repository.get_assessment(account_id) is not None
    database.dispose()


@pytest.mark.asyncio
async def test_reconcile_skips_on_empty_upstream(tmp_path: Path) -> None:
    database = Database(tmp_path / "grokiq.db")
    database.initialize()
    _seed(database, [1, 2])

    repository = AccountRepository(database)
    service = AccountReconcileService(
        client=ReconcileClient({"grok_build": [], "grok_web": [], "grok_console": []}),  # type: ignore[arg-type]
        accounts=repository,
    )
    result = await service.reconcile()

    assert result["ok"] is False
    assert result["skipped"] == "upstream_empty"
    assert repository.get_assessment(1) is not None
    database.dispose()


@pytest.mark.asyncio
async def test_reconcile_spans_all_providers(tmp_path: Path) -> None:
    """An account must not be judged missing because it lives on another provider."""

    database = Database(tmp_path / "grokiq.db")
    database.initialize()
    _seed(database, [10, 11, 12])

    repository = AccountRepository(database)
    service = AccountReconcileService(
        client=ReconcileClient(  # type: ignore[arg-type]
            {
                "grok_build": [{"id": "10"}],
                "grok_web": [{"id": "11"}],
                "grok_console": [{"id": "12"}],
            }
        ),
        accounts=repository,
    )
    result = await service.reconcile()

    assert result["ghostAccounts"] == 0
    database.dispose()
