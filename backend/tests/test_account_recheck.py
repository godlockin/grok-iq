"""Re-verification of isolated accounts: verdict, revive, backoff, and the
"only accounts that qualify" gate.
"""

from __future__ import annotations

import random
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

from app.core.clock import utc_now
from app.core.config import Settings
from app.persistence.account_repository import AccountRepository
from app.persistence.database import Database
from app.persistence.models import AccountAssessment, ProbeRun
from app.persistence.probe_repository import ProbeRepository
from app.services.account_recheck import SKIP_RETRY_MINUTES, AccountRecheckService
from app.services.account_service import AccountService
from app.services.recheck_schedule import recheck_delay_minutes


class FakeClient:
    """Minimal grok2api admin client for the revive path."""

    def __init__(self, accounts: list[dict[str, Any]]) -> None:
        self.accounts = {int(item["id"]): dict(item) for item in accounts}
        self.recovered: list[tuple[int, bool]] = []

    async def get_account(self, account_id: int) -> dict[str, Any]:
        return dict(self.accounts[account_id])

    async def list_all_accounts(self, **_params: Any) -> list[dict[str, Any]]:
        return [dict(item) for item in self.accounts.values()]

    async def set_account_enabled(
        self, account_id: int, enabled: bool
    ) -> dict[str, Any]:
        account = self.accounts[account_id]
        account["enabled"] = enabled
        return dict(account)

    async def recover_account_at_priority(
        self, account_id: int, *, priority: int
    ) -> dict[str, Any]:
        account = self.accounts[account_id]
        account["enabled"] = True
        account["priority"] = priority
        self.recovered.append((account_id, True))
        return dict(account)

    async def set_account_priority(self, account_id: int, priority: int) -> dict[str, Any]:
        self.accounts[account_id]["priority"] = priority
        return dict(self.accounts[account_id])


def _build(tmp_path: Path, **overrides: Any):
    database = Database(tmp_path / "grokiq.db")
    database.initialize()
    accounts = AccountRepository(database)
    probes = ProbeRepository(database)
    # probe_runs.profile_id is a real foreign key, so the built-in profile must
    # exist before a run row can reference it.
    probes.seed_defaults()
    settings = Settings(
        database_path=tmp_path / "grokiq.db",
        quarantine_recheck_enabled=True,
        quarantine_recheck_minutes=120,
        quarantine_recheck_max_minutes=360,
        quarantine_recheck_batch=10,
    )
    client = FakeClient(
        [
            {
                "id": account_id,
                "name": f"account-{account_id}",
                "email": f"account-{account_id}@example.test",
                "enabled": False,
                "authStatus": "active",
            }
            for account_id in range(1, 40)
        ]
    )
    service = AccountService(
        settings=settings,
        client=client,  # type: ignore[arg-type]
        accounts=accounts,
        probes=probes,
    )
    recheck = AccountRecheckService(
        settings=settings,
        accounts=accounts,
        probes=probes,
        account_service=service,
        enqueue=_no_enqueue,
        thresholds=service_thresholds(),
    )
    return database, accounts, probes, recheck, settings


def service_thresholds():
    from app.analyzer import thresholds_from_settings

    return thresholds_from_settings(
        Settings(probe_tps_override_mode="off", reasoning_zero_risk_enabled=False)
    )


async def _no_enqueue(**_kwargs: Any) -> str:
    return "run"


def _isolate(
    database: Database,
    account_id: int,
    *,
    due_in_minutes: int | None,
    source: str = "request_audit",
    failures: int = 0,
) -> None:
    """Insert an isolated account directly.

    Written as raw rows so a test never has to make an isolation decision
    depend on the risk pipeline it is supposed to be testing against.
    """

    with database.transaction() as session:
        session.add(
            AccountAssessment(
                account_id=account_id,
                monitor_status="quarantined",
                risk_score=85.0,
                disabled_by_monitor=True,
                previous_upstream_enabled=True,
                recheck_due_at=(
                    utc_now() + timedelta(minutes=due_in_minutes)
                    if due_in_minutes is not None
                    else None
                ),
                recheck_failures=failures,
                manual_note="request audit high risk",
                disposition={
                    "source": source,
                    "action": "isolate",
                    "reason": "request audit high risk",
                },
            )
        )


def _add_run(
    database: Database,
    account_id: int,
    *,
    classifications: list[tuple[str, str]],
    status: str = "completed",
) -> str:
    """Persist one recheck run with the given ``(status, classification)`` samples.

    ``expected_matched`` defaults to the marker-miss rule's own meaning: a
    ``marker_miss`` sample missed the marker, and anything else returned it.
    Tests that care about the marker pass it explicitly.
    """

    run_id = f"recheck-{account_id}"
    with database.transaction() as session:
        session.add(
            ProbeRun(
                id=run_id,
                account_id=account_id,
                profile_id="quality-marker",
                status=status,
                trigger="recheck",
                automatic=True,
                priority=120,
                execution_mode="chat",
                rounds=len(classifications),
                proxy_targets=[{"kind": "direct", "id": None, "name": "x"}],
                total_steps=len(classifications),
                completed_steps=len(classifications),
                summary={},
            )
        )
        for index, (sample_status, classification) in enumerate(classifications, start=1):
            session.add(
                _sample(
                    run_id=run_id,
                    account_id=account_id,
                    round_number=index,
                    classification=classification,
                    status=sample_status,
                    # An empty stream reports expected_matched=False only because
                    # nothing came back, which is not a format miss.
                    expected_matched=(
                        False
                        if classification in {"marker_miss", "empty_response"}
                        else True
                    ),
                )
            )
    return run_id


def _sample(**kwargs: Any):
    from app.persistence.models import ProbeSample

    return ProbeSample(
        id=f"{kwargs['run_id']}-{kwargs['round_number']}",
        run_id=kwargs["run_id"],
        account_id=kwargs["account_id"],
        round_number=kwargs["round_number"],
        target_key="direct",
        target_kind="direct",
        status=kwargs["status"],
        classification=kwargs["classification"],
        expected_matched=kwargs["expected_matched"],
        upstream_tps=60.0,
    )


# --------------------------------------------------------------- due selection
def test_due_rechecks_excludes_null_and_future(tmp_path: Path) -> None:
    database, accounts, _probes, _recheck, _settings = _build(tmp_path)
    _isolate(database, 1, due_in_minutes=-1)  # due
    _isolate(database, 2, due_in_minutes=60)  # not yet
    _isolate(database, 3, due_in_minutes=None)  # never: manual semantics

    due = accounts.due_rechecks(limit=10)

    assert [item["account_id"] for item in due] == [1]
    database.dispose()


def test_due_rechecks_respects_batch_limit(tmp_path: Path) -> None:
    database, accounts, _probes, _recheck, _settings = _build(tmp_path)
    for account_id in (1, 2, 3):
        _isolate(database, account_id, due_in_minutes=-account_id)

    due = accounts.due_rechecks(limit=2)

    assert [item["account_id"] for item in due] == [3, 2]
    database.dispose()


def test_due_rechecks_only_returns_isolated_accounts(tmp_path: Path) -> None:
    database, accounts, _probes, _recheck, _settings = _build(tmp_path)
    with database.transaction() as session:
        session.add(
            AccountAssessment(
                account_id=9,
                monitor_status="healthy",
                recheck_due_at=utc_now() - timedelta(hours=1),
            )
        )

    assert accounts.due_rechecks(limit=10) == []
    database.dispose()


# -------------------------------------------------------------------- backoff
def test_recheck_delay_is_jittered_within_bounds() -> None:
    drawn = {
        recheck_delay_minutes(
            failures=0, min_minutes=120, max_minutes=360, rng=random.Random(seed)
        )
        for seed in range(40)
    }

    assert drawn
    assert min(drawn) >= 120
    assert max(drawn) <= 360
    # A single fixed window would defeat the purpose of the jitter.
    assert len(drawn) > 1


def test_recheck_delay_backs_off_exponentially_and_caps() -> None:
    first = recheck_delay_minutes(
        failures=0, min_minutes=10, max_minutes=10, rng=random.Random(0)
    )
    third = recheck_delay_minutes(
        failures=3, min_minutes=10, max_minutes=10, rng=random.Random(0)
    )
    capped = recheck_delay_minutes(
        failures=99, min_minutes=10, max_minutes=10, rng=random.Random(0)
    )

    assert first == 10
    assert third == 80
    assert capped == 10 * 2**5


# -------------------------------------------------------------------- verdict
def test_evaluate_recovers_on_normal_samples(tmp_path: Path) -> None:
    database, _accounts, _probes, recheck, _settings = _build(tmp_path)
    _isolate(database, 1, due_in_minutes=-1)
    run_id = _add_run(database, 1, classifications=[("done", "normal")] * 2)

    verdict = recheck.evaluate_run(account_id=1, run_id=run_id)

    assert verdict["outcome"] == "recovered"
    database.dispose()


@pytest.mark.parametrize("classification", ["buffered_hard", "buffered_soft", "elevated", "fast_risk"])
def test_evaluate_recovers_when_a_tps_band_fires_but_the_marker_matched(
    tmp_path: Path, classification: str
) -> None:
    """A throughput spike must not keep an account isolated.

    Every TPS-band classification measured a 0% marker-miss rate in production,
    so treating one as degradation is what produced the 155 false isolations.
    The marker is the only instruction-following evidence, so it alone decides.
    """

    database, _accounts, _probes, recheck, _settings = _build(tmp_path)
    _isolate(database, 1, due_in_minutes=-1, failures=3)
    run_id = _add_run(
        database, 1, classifications=[("done", classification), ("done", "normal")]
    )

    verdict = recheck.evaluate_run(account_id=1, run_id=run_id)

    assert verdict["outcome"] == "recovered"
    assert verdict["detail"]["markerMisses"] == 0
    assert classification in verdict["detail"]["classifications"]
    database.dispose()


def test_evaluate_confirms_degraded_only_on_a_marker_miss(tmp_path: Path) -> None:
    database, _accounts, _probes, recheck, _settings = _build(tmp_path)
    _isolate(database, 1, due_in_minutes=-1, failures=2)
    run_id = _add_run(
        database, 1, classifications=[("done", "normal"), ("done", "marker_miss")]
    )

    verdict = recheck.evaluate_run(account_id=1, run_id=run_id)

    assert verdict["outcome"] == "degraded"
    assert verdict["failures"] == 3
    assert verdict["detail"]["markerMisses"] == 1
    assert "标记" in verdict["reason"]
    database.dispose()


def test_evaluate_is_inconclusive_without_samples(tmp_path: Path) -> None:
    database, _accounts, _probes, recheck, _settings = _build(tmp_path)
    _isolate(database, 1, due_in_minutes=-1, failures=1)
    run_id = _add_run(database, 1, classifications=[])

    verdict = recheck.evaluate_run(account_id=1, run_id=run_id)

    # An empty round must not count as a failure, or an upstream outage would
    # permanently bury healthy accounts.
    assert verdict["outcome"] == "inconclusive"
    assert verdict["failures"] == 1
    database.dispose()


def test_evaluate_ignores_unusable_samples(tmp_path: Path) -> None:
    database, _accounts, _probes, recheck, _settings = _build(tmp_path)
    _isolate(database, 1, due_in_minutes=-1, failures=2)
    run_id = _add_run(
        database, 1, classifications=[("done", "insufficient"), ("error", "error")]
    )

    verdict = recheck.evaluate_run(account_id=1, run_id=run_id)

    assert verdict["outcome"] == "inconclusive"
    assert verdict["failures"] == 2
    database.dispose()


def test_evaluate_ignores_failed_samples(tmp_path: Path) -> None:
    database, _accounts, _probes, recheck, _settings = _build(tmp_path)
    _isolate(database, 1, due_in_minutes=-1)
    run_id = _add_run(
        database, 1, classifications=[("failed", "error")], status="failed"
    )

    verdict = recheck.evaluate_run(account_id=1, run_id=run_id)

    assert verdict["outcome"] == "inconclusive"
    database.dispose()


def test_evaluate_is_inconclusive_when_every_sample_is_an_empty_stream(
    tmp_path: Path,
) -> None:
    """An all-empty round measured nothing, so it must not produce a verdict.

    Live evidence: these accounts answered normally on the other round of the
    same run. Counting the timeout as a failure would keep them isolated for a
    transient upstream condition; clearing on it would revive an account that
    was never actually measured.
    """

    database, _accounts, _probes, recheck, _settings = _build(tmp_path)
    _isolate(database, 1, due_in_minutes=-1, failures=0)
    run_id = _add_run(
        database, 1, classifications=[("done", "empty_response")] * 2
    )

    verdict = recheck.evaluate_run(account_id=1, run_id=run_id)

    assert verdict["outcome"] == "inconclusive"
    # The failure count is preserved: an inconclusive round is not evidence
    # either for or against the account.
    assert verdict["failures"] == 0
    assert verdict["detail"]["emptyResponses"] == 2
    database.dispose()


def test_evaluate_recovers_when_an_empty_stream_is_mixed_with_a_good_reply(
    tmp_path: Path,
) -> None:
    """One dead stream plus one measured reply is enough to judge.

    The account did answer, so the round is usable: the empty sample is skipped
    and the reply decides.
    """

    database, _accounts, _probes, recheck, _settings = _build(tmp_path)
    _isolate(database, 1, due_in_minutes=-1)
    run_id = _add_run(
        database,
        1,
        classifications=[("done", "empty_response"), ("done", "normal")],
    )

    verdict = recheck.evaluate_run(account_id=1, run_id=run_id)

    assert verdict["outcome"] == "recovered"
    assert verdict["detail"]["emptyResponses"] == 1
    # The empty sample's expected_matched=0 must not be reported as a miss.
    assert verdict["detail"]["markerMisses"] == 0
    database.dispose()


# ---------------------------------------------------------------------- settle
@pytest.mark.asyncio
async def test_settle_revives_recovered_account(tmp_path: Path) -> None:
    database, accounts, _probes, recheck, _settings = _build(tmp_path)
    _isolate(database, 1, due_in_minutes=-1, failures=4)
    run_id = _add_run(database, 1, classifications=[("done", "normal")] * 2)

    result = await recheck.settle(account_id=1, run_id=run_id)

    assert result["outcome"] == "recovered"
    assert result["revived"] is True
    stored = accounts.get_assessment(1)
    assert stored is not None
    assert stored["monitor_status"] == "healthy"
    assert stored["quarantine_until"] is None
    assert stored["disabled_by_monitor"] is False
    # A revived account must start a fresh history, not immediately re-isolate
    # after a single bad round. A healthy account is not in the re-check queue
    # at all, so its due time stays NULL until something isolates it again.
    assert stored["recheck_failures"] == 0
    assert stored["recheck_due_at"] is None
    database.dispose()


@pytest.mark.asyncio
async def test_settle_keeps_degraded_account_and_spaces_it_out(tmp_path: Path) -> None:
    database, accounts, _probes, recheck, settings = _build(tmp_path)
    _isolate(database, 1, due_in_minutes=-1, failures=0)
    run_id = _add_run(database, 1, classifications=[("done", "marker_miss")])

    result = await recheck.settle(account_id=1, run_id=run_id)

    assert result["outcome"] == "degraded"
    stored = accounts.get_assessment(1)
    assert stored is not None
    assert stored["monitor_status"] == "quarantined"
    assert stored["recheck_failures"] == 1
    due_at = stored["recheck_due_at"]
    assert due_at is not None
    assert due_at > utc_now() + timedelta(
        minutes=settings.quarantine_recheck_minutes - 1
    )
    database.dispose()


@pytest.mark.asyncio
async def test_settle_reschedules_inconclusive_without_changing_verdict(
    tmp_path: Path,
) -> None:
    database, accounts, _probes, recheck, settings = _build(tmp_path)
    _isolate(database, 1, due_in_minutes=-1, failures=1)
    run_id = _add_run(database, 1, classifications=[])

    result = await recheck.settle(account_id=1, run_id=run_id)

    assert result["outcome"] == "inconclusive"
    assert result["rescheduled"] is True
    stored = accounts.get_assessment(1)
    assert stored is not None
    assert stored["monitor_status"] == "quarantined"
    assert stored["recheck_failures"] == 1
    assert stored["recheck_due_at"] is not None
    database.dispose()


# -------------------------------------------------------------------- arming
def test_arm_existing_isolations_skips_manual_ones(tmp_path: Path) -> None:
    database, accounts, _probes, recheck, _settings = _build(tmp_path)
    _isolate(database, 1, due_in_minutes=None, source="request_audit")
    _isolate(database, 2, due_in_minutes=None, source="manual")
    _isolate(database, 3, due_in_minutes=None, source="probe")

    armed = recheck.arm_existing_isolations()

    # A manual isolation must stay out of the auto-revive queue permanently.
    assert armed == 2
    assert accounts.get_assessment(1)["recheck_due_at"] is not None
    assert accounts.get_assessment(2)["recheck_due_at"] is None
    assert accounts.get_assessment(3)["recheck_due_at"] is not None
    database.dispose()


def test_arm_existing_isolations_is_idempotent(tmp_path: Path) -> None:
    database, accounts, _probes, recheck, _settings = _build(tmp_path)
    _isolate(database, 1, due_in_minutes=None, source="request_audit")

    first = recheck.arm_existing_isolations()
    due_after_first = accounts.get_assessment(1)["recheck_due_at"]
    second = recheck.arm_existing_isolations()

    assert first == 1
    assert second == 0
    assert accounts.get_assessment(1)["recheck_due_at"] == due_after_first
    database.dispose()


def test_arm_existing_isolations_respects_disabled_switch(tmp_path: Path) -> None:
    database, accounts, _probes, recheck, settings = _build(tmp_path)
    settings.quarantine_recheck_enabled = False
    _isolate(database, 1, due_in_minutes=None, source="request_audit")

    assert recheck.arm_existing_isolations() == 0
    assert accounts.get_assessment(1)["recheck_due_at"] is None
    database.dispose()


# ----------------------------------------------------------------------- scan
@pytest.mark.asyncio
async def test_scan_only_enqueues_due_accounts(tmp_path: Path) -> None:
    database, accounts, _probes, recheck, _settings = _build(tmp_path)
    _isolate(database, 1, due_in_minutes=-1)
    _isolate(database, 2, due_in_minutes=600)
    _isolate(database, 3, due_in_minutes=None, source="manual")
    enqueued: list[int] = []

    async def record(**kwargs: Any) -> str:
        enqueued.append(int(kwargs["account_id"]))
        return "run"

    recheck.enqueue = record  # type: ignore[assignment]

    result = await recheck.scan()

    assert result["ok"] is True
    assert result["candidates"] == 1
    assert result["enqueued"] == 1
    assert enqueued == [1]
    database.dispose()


@pytest.mark.asyncio
async def test_scan_skips_when_disabled(tmp_path: Path) -> None:
    database, _accounts, _probes, recheck, settings = _build(tmp_path)
    settings.quarantine_recheck_enabled = False
    _isolate(database, 1, due_in_minutes=-1)

    result = await recheck.scan()

    assert result["skipped"] == "disabled"
    assert result["enqueued"] == 0
    database.dispose()


@pytest.mark.asyncio
async def test_scan_survives_a_failing_account(tmp_path: Path) -> None:
    database, accounts, _probes, recheck, _settings = _build(tmp_path)
    _isolate(database, 1, due_in_minutes=-1)
    _isolate(database, 2, due_in_minutes=-1)

    async def flaky(**kwargs: Any) -> str:
        if int(kwargs["account_id"]) == 1:
            raise RuntimeError("account has no bound egress")
        return "run"

    recheck.enqueue = flaky  # type: ignore[assignment]

    result = await recheck.scan()

    assert result["candidates"] == 2
    assert result["enqueued"] == 1
    assert result["skippedCount"] == 1
    database.dispose()


@pytest.mark.asyncio
async def test_operational_skip_is_retried_soon_not_after_a_full_window(
    tmp_path: Path,
) -> None:
    """A short lock must not cost the account a full re-verification window.

    Leaving the already-expired due time in place keeps the account in the
    queue but retries it only after 120-360 minutes, so a five-minute conflict
    would cost hours of isolation.
    """

    database, accounts, _probes, recheck, _settings = _build(tmp_path)
    _isolate(database, 1, due_in_minutes=-1)

    async def blocked(**_kwargs: Any) -> str:
        return ""

    recheck.enqueue = blocked  # type: ignore[assignment]

    before = utc_now()
    await recheck.scan()

    due_at = accounts.get_assessment(1)["recheck_due_at"]
    assert due_at is not None
    # Due again within minutes, and no longer in the past.
    assert due_at >= before + timedelta(minutes=SKIP_RETRY_MINUTES - 1)
    assert due_at < before + timedelta(minutes=15)
    assert accounts.due_rechecks(limit=10) == []
    database.dispose()


@pytest.mark.asyncio
async def test_failed_enqueue_is_also_retried_soon(tmp_path: Path) -> None:
    database, accounts, _probes, recheck, _settings = _build(tmp_path)
    _isolate(database, 1, due_in_minutes=-1)

    async def boom(**_kwargs: Any) -> str:
        raise RuntimeError("该账号存在未完成的原设置恢复")

    recheck.enqueue = boom  # type: ignore[assignment]

    before = utc_now()
    result = await recheck.scan()

    assert result["skippedCount"] == 1
    due_at = accounts.get_assessment(1)["recheck_due_at"]
    assert due_at is not None
    assert due_at < before + timedelta(minutes=15)
    database.dispose()


# ----------------------------------------------- newly isolated accounts qualify
@pytest.mark.asyncio
async def test_auto_isolation_schedules_first_recheck(tmp_path: Path) -> None:
    database, accounts, _probes, recheck, settings = _build(tmp_path)
    _isolate(database, 1, due_in_minutes=-1)
    _add_run(database, 1, classifications=[("done", "marker_miss")])
    await recheck.settle(account_id=1, run_id="recheck-1")

    # A freshly isolated account must be a re-check candidate, otherwise the
    # recovery loop would only ever apply to pre-existing isolations.
    assert accounts.get_assessment(1)["recheck_due_at"] is not None
    database.dispose()


@pytest.mark.asyncio
async def test_temporary_quarantine_does_not_get_a_recheck(tmp_path: Path) -> None:
    """A time-based quarantine recovers on its own and must not be double-managed."""

    database, accounts, _probes, recheck, _settings = _build(tmp_path)
    with database.transaction() as session:
        from app.persistence.models import AccountAssessment

        session.add(
            AccountAssessment(
                account_id=1,
                monitor_status="quarantined",
                quarantine_until=utc_now() + timedelta(minutes=30),
                disabled_by_monitor=True,
                recheck_due_at=None,
                disposition={"source": "probe", "action": "quarantine"},
            )
        )

    assert accounts.due_rechecks(limit=10) == []
    database.dispose()
