from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from unittest.mock import MagicMock

from app.core.clock import utc_now
from app.core.config import Settings
from app.persistence.database import Database
from app.persistence.models import RequestAuditRecord
from app.persistence.request_audit_repository import RequestAuditRepository
from app.services.request_audit_service import RequestAuditService


def build_service() -> RequestAuditService:
    return RequestAuditService(
        settings=MagicMock(),
        client=MagicMock(),
        repository=MagicMock(),
    )


def test_request_audit_window_includes_1h_and_3h():
    service = build_service()
    one = service.resolve_window(window_preset="1h")
    three = service.resolve_window(window_preset="3h")
    assert one["preset"] == "1h"
    assert one["label"] == "最近 1 小时"
    assert one["end"] - one["start"] == timedelta(hours=1)
    assert three["preset"] == "3h"
    assert three["label"] == "最近 3 小时"
    assert three["end"] - three["start"] == timedelta(hours=3)


def test_custom_window_allows_end_after_now():
    service = build_service()
    now = utc_now()
    window = service.resolve_window(
        window_preset="custom",
        start_at=now - timedelta(hours=1),
        end_at=now + timedelta(hours=2),
    )
    assert window["preset"] == "custom"
    assert window["end"] > now


def test_repeated_reasoning_zero_keeps_strong_tps_as_primary_rule():
    service = RequestAuditService(
        settings=Settings(_env_file=None),
        client=MagicMock(),
        repository=MagicMock(),
    )
    now = utc_now()
    records = [
        {
            "upstream_id": str(index),
            "account_id": 7,
            "status_code": 200,
            "output_tokens": 600,
            "reasoning_tokens": 0,
            "reasoning_tokens_reported": True,
            "first_token_ms": 100,
            "duration_ms": 1100,
            "tps": 600,
            "model_upstream_model": "Build/grok-4.6",
            "model_public_id": "grok-4.6",
            "operation": "chat",
            "media_input_images": 0,
            "created_at": now + timedelta(seconds=index),
        }
        for index in (1, 2)
    ]

    evaluations = service._audit_risk_evaluations(records)
    latest = evaluations["2"]

    assert latest.classification.name == "high"
    assert latest.classification.rule_id == "fast_risk"
    assert "reasoning_zero" in latest.classification.rule_ids
    assert latest.reasoning_streak == 2


def test_media_input_observe_does_not_auto_disable_for_reasoning_zero():
    service = RequestAuditService(
        settings=Settings(_env_file=None),
        client=MagicMock(),
        repository=MagicMock(),
    )
    now = utc_now()
    records = [
        {
            "upstream_id": str(index),
            "account_id": 5433,
            "status_code": 200,
            "output_tokens": 155,
            "reasoning_tokens": 0,
            "reasoning_tokens_reported": True,
            "first_token_ms": 100,
            "duration_ms": 1100,
            "tps": 1700 + index,
            "model_upstream_model": "Build/grok-4.6",
            "model_public_id": "grok-4.6",
            "operation": "responses",
            "media_input_images": 3,
            "created_at": now + timedelta(seconds=index),
        }
        for index in (1, 2, 3, 4)
    ]

    evaluations = service._audit_risk_evaluations(records)
    latest = evaluations["4"]
    candidates = service._pre_disable_candidates(records, evaluations=evaluations)

    assert latest.classification.rule_id == "media_input_observe"
    assert latest.classification.name == "watch"
    assert latest.classification.hard is False
    assert "reasoning_zero" in latest.classification.rule_ids
    assert latest.reasoning_streak == 0
    assert candidates == []


def test_grokiq_own_probe_request_cannot_trigger_a_quarantine():
    """A probe measures an account; it must not be able to condemn it.

    In production a re-verification probe returned the marker but ran at 958
    TPS, and the request-audit scanner classified that as ``fast_risk`` and
    re-isolated account 106 fourteen seconds after the re-verification had
    correctly cleared it. The probe's own audit must therefore be invisible to
    the mutation path.
    """

    probes = MagicMock()
    probes.probe_audit_ids.return_value = {4}
    service = RequestAuditService(
        settings=Settings(_env_file=None),
        client=MagicMock(),
        repository=MagicMock(),
        probes=probes,
    )
    now = utc_now()
    # Only one high-risk row, and it is the probe's own audit.
    records = [
        {
            "upstream_id": "4",
            "account_id": 7,
            "status_code": 200,
            "output_tokens": 1500,
            "reasoning_tokens": 1499,
            "reasoning_tokens_reported": True,
            "first_token_ms": 100,
            "duration_ms": 2000,
            "tps": 900,
            "model_upstream_model": "Build/grok-4.6",
            "model_public_id": "grok-4.6",
            "operation": "chat",
            "media_input_images": 0,
            "fetched_at": now,
            "created_at": now,
        }
    ]
    evaluations = service._audit_risk_evaluations(records)

    assert evaluations["4"].classification.name == "high"

    trigger = service._new_risk_account_ids(
        records,
        discovered_after=now - timedelta(minutes=5),
        evaluations=evaluations,
    )

    assert trigger == set()


def test_real_user_traffic_still_triggers_a_quarantine():
    """The probe filter must not weaken protection against genuine traffic."""

    probes = MagicMock()
    probes.probe_audit_ids.return_value = {999}
    service = RequestAuditService(
        settings=Settings(_env_file=None),
        client=MagicMock(),
        repository=MagicMock(),
        probes=probes,
    )
    now = utc_now()
    records = [
        {
            "upstream_id": str(index),
            "account_id": 7,
            "status_code": 200,
            "output_tokens": 1500,
            "reasoning_tokens": 0,
            "reasoning_tokens_reported": True,
            "first_token_ms": 100,
            "duration_ms": 2000,
            "tps": 900,
            "model_upstream_model": "Build/grok-4.6",
            "model_public_id": "grok-4.6",
            "operation": "chat",
            "media_input_images": 0,
            "fetched_at": now,
            "created_at": now,
        }
        for index in (1, 2, 3, 4)
    ]
    evaluations = service._audit_risk_evaluations(records)

    trigger = service._new_risk_account_ids(
        records,
        discovered_after=now - timedelta(minutes=5),
        evaluations=evaluations,
    )

    assert trigger == {7}


def test_probe_client_key_name_is_excluded_before_the_sample_exists():
    """The client-key filter must work with no timing window at all.

    Filtering on the linked probe sample only works after the run finishes.
    A scan that lands while the probe is still in flight sees the audit before
    the sample exists, and that audit used to quarantine the account. Measured
    live: 14 of 24 probe audits were unlinked at scan time, and all 14 led to
    a quarantine.
    """

    probes = MagicMock()
    # Nothing is linked yet: the probe is still running.
    probes.probe_audit_ids.return_value = set()
    service = RequestAuditService(
        settings=Settings(_env_file=None, probe_route_prefix="grokiq-probe"),
        client=MagicMock(),
        repository=MagicMock(),
        probes=probes,
    )
    now = utc_now()
    records = [
        {
            "upstream_id": str(index),
            "account_id": 7,
            "client_key_name": f"grokiq-probe-{index:012x}",
            "status_code": 200,
            "output_tokens": 1500,
            "reasoning_tokens": 0,
            "reasoning_tokens_reported": True,
            "first_token_ms": 100,
            "duration_ms": 2000,
            "tps": 900,
            "model_upstream_model": "Build/grok-4.6",
            "model_public_id": "grok-4.6",
            "operation": "chat",
            "media_input_images": 0,
            "fetched_at": now,
            "created_at": now,
        }
        for index in (1, 2, 3, 4)
    ]
    evaluations = service._audit_risk_evaluations(records)
    assert evaluations["4"].classification.name == "high"

    trigger = service._new_risk_account_ids(
        records,
        discovered_after=now - timedelta(minutes=5),
        evaluations=evaluations,
    )

    assert trigger == set()


def test_renaming_the_probe_prefix_keeps_the_filter_aligned():
    probes = MagicMock()
    probes.probe_audit_ids.return_value = set()
    service = RequestAuditService(
        settings=Settings(_env_file=None, probe_route_prefix="custom-probe"),
        client=MagicMock(),
        repository=MagicMock(),
        probes=probes,
    )
    now = utc_now()
    record = {
        "upstream_id": "1",
        "account_id": 7,
        "client_key_name": "custom-probe-abc123",
        "status_code": 200,
        "output_tokens": 1500,
        "reasoning_tokens": 0,
        "reasoning_tokens_reported": True,
        "first_token_ms": 100,
        "duration_ms": 2000,
        "tps": 900,
        "model_upstream_model": "Build/grok-4.6",
        "model_public_id": "grok-4.6",
        "operation": "chat",
        "media_input_images": 0,
        "fetched_at": now,
        "created_at": now,
    }
    evaluations = service._audit_risk_evaluations([record])

    assert (
        service._new_risk_account_ids(
            [record],
            discovered_after=now - timedelta(minutes=5),
            evaluations=evaluations,
        )
        == set()
    )
    # A user key that merely shares a substring must still be judged.
    other = {**record, "upstream_id": "2", "client_key_name": "Default User Key"}
    other_evaluations = service._audit_risk_evaluations([other])
    assert service._new_risk_account_ids(
        [other],
        discovered_after=now - timedelta(minutes=5),
        evaluations=other_evaluations,
    ) == {7}


def test_retryable_backlog_excludes_probe_originated_verdicts(tmp_path: Path):
    """A probe verdict must never be re-applied from the retry backlog.

    ``retryable_verification_account_ids`` is unioned into the trigger set
    *after* the per-scan filter, so a backlog row bypasses the client-key
    check entirely and is re-applied with ``force=True, permanent=True``.
    Measured live: 47 accounts were re-isolated that way while the scan-side
    filter reported zero probe traffic.
    """

    database = Database(tmp_path / "grokiq.db")
    database.initialize()
    repository = RequestAuditRepository(database)
    with database.transaction() as session:
        session.add(
            RequestAuditRecord(
                upstream_id="5001",
                request_id="req-5001",
                day_key="2026-09-30",
                provider="grok_build",
                operation="chat",
                model_public_id="grok-4.7",
                model_upstream_model="Build/grok-4.6",
                account_id=11,
                client_key_id="1",
                client_key_name="grokiq-probe-abc123",
                status_code=200,
                output_tokens=1500,
                reasoning_tokens=1000,
                tps=1500.0,
                created_at=utc_now(),
            )
        )
        session.add(
            RequestAuditRecord(
                upstream_id="5002",
                request_id="req-5002",
                day_key="2026-09-30",
                provider="grok_build",
                operation="chat",
                model_public_id="grok-4.7",
                model_upstream_model="Build/grok-4.6",
                account_id=12,
                client_key_id="1",
                client_key_name="Default User Key",
                status_code=200,
                output_tokens=1500,
                reasoning_tokens=1000,
                tps=1500.0,
                created_at=utc_now(),
            )
        )
    for account_id, upstream_id in ((11, "5001"), (12, "5002")):
        repository.create_verification(
            {
                "account_id": account_id,
                "audit_upstream_id": upstream_id,
                "audit_created_at": utc_now(),
                "audit_tps": 1500.0,
                "status": "flagged",
                "action_status": "pending",
            }
        )

    unfiltered = repository.retryable_verification_account_ids()
    filtered = repository.retryable_verification_account_ids(
        internal_client_key_prefix="grokiq-probe-"
    )

    assert unfiltered == {11, 12}
    # The probe-originated verdict must not be re-applied; real traffic still is.
    assert filtered == {12}
    # An empty prefix keeps the previous behaviour for callers that opt out.
    assert repository.retryable_verification_account_ids(
        internal_client_key_prefix=""
    ) == {11, 12}
    database.dispose()


def _audit_records(*, operation: str, images: int, tps: float, count: int = 4):
    now = utc_now()
    return [
        {
            "upstream_id": str(index),
            "account_id": 7,
            "status_code": 200,
            "output_tokens": 155,
            "reasoning_tokens": 0,
            "reasoning_tokens_reported": True,
            "first_token_ms": 100,
            "duration_ms": 1100,
            "tps": tps,
            "model_upstream_model": "Build/grok-4.6",
            "model_public_id": "grok-4.6",
            "operation": operation,
            "media_input_images": images,
            "created_at": now + timedelta(seconds=index),
        }
        for index in range(1, count + 1)
    ]


def test_required_text_reasoning_zero_still_auto_disables():
    service = RequestAuditService(
        settings=Settings(_env_file=None),
        client=MagicMock(),
        repository=MagicMock(),
    )
    records = _audit_records(operation="chat", images=0, tps=40)
    evaluations = service._audit_risk_evaluations(records)
    latest = evaluations["4"]
    candidates = service._pre_disable_candidates(records, evaluations=evaluations)

    assert latest.classification.rule_id == "reasoning_zero"
    assert latest.classification.name == "high"
    assert latest.classification.hard is True
    assert latest.reasoning_streak == 4
    assert [item.get("_risk_rule_id") for item in candidates] == ["reasoning_zero"]


def test_required_media_input_reasoning_zero_does_not_auto_disable():
    service = RequestAuditService(
        settings=Settings(_env_file=None),
        client=MagicMock(),
        repository=MagicMock(),
    )
    records = _audit_records(operation="chat", images=3, tps=40)
    evaluations = service._audit_risk_evaluations(records)
    latest = evaluations["4"]
    candidates = service._pre_disable_candidates(records, evaluations=evaluations)

    assert latest.classification.name == "watch"
    assert latest.classification.hard is False
    assert latest.classification.rule_id == "reasoning_zero"
    assert "reasoning_zero" in latest.classification.rule_ids
    assert latest.reasoning_streak == 0
    assert any("不作为隔离或停用依据" in reason for reason in latest.classification.reasons)
    assert candidates == []

