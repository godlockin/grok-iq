"""Re-verify isolated accounts with real probe evidence, and revive what recovers.

Why this exists
---------------
Every automatic isolation path in GrokIQ is permanent: ``quarantine_until`` is
NULL, so nothing ever restores the account. That is the correct default for a
confirmed degradation, but it is also the correct default for a *misjudged*
one. The risk score that drives isolation aggregates marker misses, TPS bands
and anomaly streaks over a rolling window, and a single bad window is enough
to push an otherwise healthy account over the threshold and lock it forever.

How recovery is decided
-----------------------
Not by the risk score, and not by "time has passed". A re-verification run
issues the same real probe the scheduled checks use, and the verdict is driven
by the *marker*, not by throughput.

The measured reason is decisive. Across 2564 usable samples in this deployment:

  ==============  ======  ============
  classification     n     marker misses
  ==============  ======  ============
  normal           1434    0 (0.0%)
  buffered_hard    1032    0 (0.0%)
  buffered_soft      44    0 (0.0%)
  elevated           13    0 (0.0%)
  fast_risk           1    0 (0.0%)
  marker_miss        40   40 (100%)
  ==============  ======  ============

Every TPS-band classification had a **zero** marker-miss rate, so none of them
carries evidence of a model that stopped following instructions. ``normal``,
``buffered_hard`` and ``fast_risk`` are the same healthy model measured at
different points in its output cycle. Treating a throughput band as
degradation is what produced the 155 false isolations this feature exists to
undo, and it would keep producing them on every revival.

So: a marker miss keeps the account isolated, and nothing else does. Throughput
is still recorded in the alert, because it is useful context, but it can no
longer decide a verdict on its own.

A round that produced no usable sample (upstream error, cancelled run, empty
queue) is *inconclusive*, not a failure: it must neither revive the account nor
count against it, otherwise a grok2api outage would permanently bury healthy
accounts.

Backoff
-------
``recheck_due_at`` is drawn uniformly from
``[quarantine_recheck_minutes, quarantine_recheck_max_minutes]`` and widened
by ``2 ** failures`` (capped). The jitter keeps a large isolation batch from
re-probing in one burst; the backoff stops a permanently degraded account from
being probed forever. A successful re-verification resets the counter.
"""

from __future__ import annotations

import logging
import random
from datetime import timedelta
from typing import Any

from app.analyzer import Thresholds
from app.core.clock import utc_now
from app.core.config import Settings
from app.persistence.account_repository import AccountRepository
from app.persistence.probe_repository import ProbeRepository
from app.services.recheck_schedule import recheck_delay_minutes

logger = logging.getLogger(__name__)

# The re-verification run must not starve normal scheduled checks, and it must
# not wait behind an operator's explicit request either: cron plans use
# priority 200, manual runs 100, register probes 150. A stuck account is worth
# more than one more scheduled sample, but less than a manual diagnosis.
RECHECK_RUN_PRIORITY = 120
RECHECK_ROUNDS = 2
RECHECK_EXECUTION_MODE = "chat"
# An isolated account is upstream-disabled on purpose, so a "current egress"
# target is both unavailable and wrong: it would measure the account's
# temporary diagnostic activation instead of its real routing path.
RECHECK_PROXY_TARGET = {"kind": "direct", "id": None, "name": "上游调度（复检）"}
# These classifications describe an unusable sample, not a degraded one. They
# are excluded from the verdict so a timeout never revives and never condemns.
NON_VERDICT_CLASSIFICATIONS = frozenset({"insufficient", "unmeasurable", "error"})
# The upstream accepted the request but streamed nothing: grok2api's read
# timeout cut the stream before the first token. Measured live, the same
# accounts answered normally on the other round of the same run, so this round
# is a retry, not a verdict. Counting it as a failure would condemn healthy
# accounts for a transient upstream condition, and clearing on it would revive
# an account we never actually measured.
EMPTY_RESPONSE_CLASSIFICATION = "empty_response"
# The only classification that carries evidence of a model that stopped
# following instructions. Every TPS-band classification measured a 0% marker-miss
# rate, so a throughput spike cannot keep an account isolated.
DEGRADED_CLASSIFICATIONS = frozenset({"marker_miss"})
# How soon to retry an account that could not be probed for operational reasons.
# Kept below the drain interval so a short lock does not cost a full window.
SKIP_RETRY_MINUTES = 5

__all__ = [
    "AccountRecheckService",
    "RECHECK_RUN_PRIORITY",
    "recheck_delay_minutes",
]


class AccountRecheckService:
    """Issues real probes for due isolations and revises the verdict."""

    def __init__(
        self,
        *,
        settings: Settings,
        accounts: AccountRepository,
        probes: ProbeRepository,
        account_service: Any,
        enqueue: Any,
        thresholds: Thresholds,
    ) -> None:
        self.settings = settings
        self.accounts = accounts
        self.probes = probes
        self.account_service = account_service
        # Injected rather than imported: ProbeManager already depends on the
        # account services, so importing it here would close that cycle.
        self.enqueue = enqueue
        self.thresholds = thresholds

    # ------------------------------------------------------------------ scan
    async def scan(self) -> dict[str, Any]:
        """Run one re-verification pass and queue probes for due isolations."""

        if not self.settings.quarantine_recheck_enabled:
            return _result(ok=True, skipped="disabled", reason="隔离账号复检已关闭")
        if not self._profile_ids():
            return _result(ok=True, skipped="no_profile", reason="未配置复检探针方案")
        due = self.accounts.due_rechecks(
            limit=self.settings.quarantine_recheck_batch
        )
        if not due:
            return _result(ok=True, reason="none_due")
        enqueued = 0
        skipped: list[dict[str, Any]] = []
        for assessment in due:
            account_id = int(assessment["account_id"])
            try:
                created = await self.enqueue_recheck_probe(account_id)
            except Exception as exc:
                # One unprobeable account must not abort the batch, and it must
                # not be recorded as degraded for a purely operational reason.
                logger.warning(
                    "recheck enqueue failed account=%s error=%s", account_id, exc
                )
                self._retry_soon(account_id)
                skipped.append({"accountId": account_id, "reason": str(exc)})
                continue
            if created:
                enqueued += 1
            else:
                self._retry_soon(account_id)
                skipped.append({"accountId": account_id, "reason": "not_probeable"})
        logger.info(
            "recheck scan candidates=%s enqueued=%s skipped=%s",
            len(due),
            enqueued,
            len(skipped),
        )
        return _result(
            ok=True,
            reason="enqueued" if enqueued else "all_skipped",
            candidates=len(due),
            enqueued=enqueued,
            skipped_accounts=skipped,
        )

    # ----------------------------------------------------------------- startup
    def arm_existing_isolations(self) -> int:
        """Give automatic isolations created before re-verification a due time.

        A NULL ``recheck_due_at`` means "never re-check". Every isolation that
        predates the column carries that NULL, so without this pass the entire
        existing isolation zone would be permanently exempt from recovery.
        Manual isolations are excluded inside the repository, because opting
        those into automatic revival is exactly what the NULL encodes.
        """

        if not self.settings.quarantine_recheck_enabled:
            return 0
        return self.accounts.arm_unscheduled_rechecks(due_at=self._next_due(failures=0))

    # ---------------------------------------------------------------- enqueue
    async def enqueue_recheck_probe(self, account_id: int) -> bool:
        """Queue one re-verification probe. False when the account is unprobeable.

        The account stays isolated while the probe runs: the executor already
        refuses to re-enable an isolated account, and the verdict only changes
        once real evidence arrives.
        """

        profile_id = self._profile_ids()[0]
        return bool(
            await self.enqueue(
                account_id=account_id,
                profile_id=profile_id,
                rounds=RECHECK_ROUNDS,
                proxy_targets=[dict(RECHECK_PROXY_TARGET)],
                execution_mode=RECHECK_EXECUTION_MODE,
            )
        )

    # ----------------------------------------------------------------- verdict
    def evaluate_run(self, *, account_id: int, run_id: str) -> dict[str, Any]:
        """Classify one re-verification run from its own samples only.

        Reading the account's rolling window here would re-apply the same
        aggregate that produced the original isolation, so recovery could never
        happen. The verdict therefore uses only the samples this run produced.
        """

        failures = self._stored_failures(account_id)
        detail = self.probes.run_detail(run_id) or {}
        samples = [
            sample
            for sample in detail.get("samples") or []
            if str(sample.get("status") or "") == "done"
        ]
        if not samples:
            return _verdict(
                "inconclusive",
                failures=failures,
                reason="本次复检没有可用样本",
            )
        judged = [
            sample
            for sample in samples
            if str(sample.get("classification") or "")
            not in NON_VERDICT_CLASSIFICATIONS
        ]
        if not judged:
            return _verdict(
                "inconclusive",
                failures=failures,
                reason="本次复检样本均不可用于判定",
            )
        # An empty response is a retry trigger, not a verdict. If every usable
        # sample is an empty stream, the upstream never answered and the account
        # itself was never measured. Treating this as a pass would revive an
        # account on no evidence; treating it as a failure would bury a healthy
        # one for a transient timeout.
        empty = [
            sample
            for sample in judged
            if str(sample.get("classification") or "")
            == EMPTY_RESPONSE_CLASSIFICATION
        ]
        if len(empty) == len(judged):
            return _verdict(
                "inconclusive",
                failures=failures,
                reason=f"上游连续 {len(empty)} 次未返回内容，等待重试",
                detail={"emptyResponses": len(empty)},
            )
        degraded = [
            sample
            for sample in judged
            if str(sample.get("classification") or "") in DEGRADED_CLASSIFICATIONS
        ]
        # Counted over the samples that actually produced a reply: an empty
        # stream trivially "fails" the marker check, and counting it would
        # report a format miss that never happened.
        marker_misses = sum(
            1
            for sample in judged
            if sample.get("expected_matched") is False
            and str(sample.get("classification") or "")
            not in {EMPTY_RESPONSE_CLASSIFICATION, *NON_VERDICT_CLASSIFICATIONS}
        )
        throughput = [
            float(sample.get("upstream_tps") or sample.get("tps") or 0.0)
            for sample in judged
            if str(sample.get("classification") or "")
            != EMPTY_RESPONSE_CLASSIFICATION
        ]
        summary = {
            "samples": len(judged),
            "markerMisses": marker_misses,
            "emptyResponses": len(empty),
            "classifications": sorted(
                {str(sample["classification"]) for sample in judged}
            ),
            "maxUpstreamTps": round(max(throughput), 1) if throughput else 0.0,
        }
        if degraded:
            return _verdict(
                "degraded",
                failures=failures + 1,
                reason=f"复检预期标记仍然缺失 {len(degraded)} 次",
                detail=summary,
            )
        return _verdict(
            "recovered",
            failures=0,
            reason="复检预期标记全部命中",
            detail=summary,
        )

    def _stored_failures(self, account_id: int) -> int:
        assessment = self.accounts.get_assessment(account_id) or {}
        return int(assessment.get("recheck_failures") or 0)

    # ------------------------------------------------------------------ action
    async def settle(self, *, account_id: int, run_id: str) -> dict[str, Any]:
        """Apply one finished re-verification run to the account verdict."""

        verdict = self.evaluate_run(account_id=account_id, run_id=run_id)
        outcome = verdict["outcome"]
        if outcome == "inconclusive":
            self._reschedule(account_id, failures=int(verdict["failures"]))
            return {**verdict, "rescheduled": True}
        if outcome == "recovered":
            return await self.revive(account_id, verdict)
        return await self.confirm_degraded(account_id, verdict, run_id=run_id)

    async def revive(
        self, account_id: int, verdict: dict[str, Any]
    ) -> dict[str, Any]:
        """Restore an account that a real probe just cleared."""

        try:
            result = await self.account_service.action(
                account_id=account_id,
                action="restore",
                note=f"隔离复检通过：{verdict['reason']}",
                propagate=True,
                quarantine_minutes=None,
            )
        except Exception as exc:
            logger.exception("recheck revive failed account=%s", account_id)
            # A failed upstream restore is not evidence about the account, so
            # keep the isolation and retry on the normal cadence.
            self._reschedule(account_id, failures=int(verdict["failures"]))
            return {**verdict, "outcome": "revive_failed", "error": str(exc)}
        # ``mark_restored`` clears the due time: a healthy account is not in the
        # re-check queue at all until something isolates it again.
        self.accounts.mark_restored(
            account_id,
            recovery_guarded=bool(result.get("propagated")),
        )
        self.accounts.create_alert(
            account_id=account_id,
            kind="recheck_recovered",
            severity="info",
            title="隔离账号复检通过并已恢复",
            detail={"reason": verdict["reason"], **(verdict.get("detail") or {})},
        )
        logger.info(
            "recheck revived account=%s reason=%s", account_id, verdict["reason"]
        )
        return {**verdict, "revived": True}

    async def confirm_degraded(
        self,
        account_id: int,
        verdict: dict[str, Any],
        *,
        run_id: str,
    ) -> dict[str, Any]:
        """Keep a still-degraded account isolated and space the next attempt."""

        failures = max(int(verdict.get("failures") or 1), 1)
        due_at = self._next_due(failures=failures - 1)
        stored = self.accounts.record_recheck_failure(account_id, due_at=due_at)
        run = self.probes.get_run(run_id) or {}
        self.accounts.create_alert(
            account_id=account_id,
            kind="recheck_degraded",
            severity="warning",
            title="隔离账号复检仍为降智，维持隔离",
            detail={
                "reason": verdict["reason"],
                "runStatus": str(run.get("status") or ""),
                "recheckFailures": stored,
                "nextCheckAt": due_at.isoformat(),
            },
        )
        logger.info(
            "recheck kept account=%s isolated failures=%s next_at=%s reason=%s",
            account_id,
            stored,
            due_at.isoformat(),
            verdict["reason"],
        )
        return {**verdict, "recheckFailures": stored, "nextCheckAt": due_at.isoformat()}

    def _reschedule(self, account_id: int, *, failures: int) -> None:
        """Retry later without changing the verdict.

        Used for inconclusive rounds. The stored failure count is preserved so
        an account that keeps hitting upstream errors is still spaced out.
        """

        self.accounts.schedule_recheck(
            account_id, due_at=self._next_due(failures=failures)
        )

    def _retry_soon(self, account_id: int) -> None:
        """Retry shortly when the account could not be probed for operational reasons.

        A probe blocked by a concurrent run, an unfinished settings restore, or
        a missing egress says nothing about the account itself. Leaving the
        original (already expired) due time in place would keep it in the queue
        but retry it only after a full re-verification window, so a five-minute
        lock would cost the account two to six hours of isolation.
        """

        self.accounts.schedule_recheck(
            account_id, due_at=utc_now() + timedelta(minutes=SKIP_RETRY_MINUTES)
        )

    def _next_due(self, *, failures: int):
        delay = recheck_delay_minutes(
            failures=failures,
            min_minutes=self.settings.quarantine_recheck_minutes,
            max_minutes=self.settings.quarantine_recheck_max_minutes,
            rng=random.Random(),
        )
        return utc_now() + timedelta(minutes=delay)

    def _profile_ids(self) -> list[str]:
        return [
            str(value).strip()
            for value in self.settings.quarantine_recheck_profile_ids
            if str(value).strip()
        ]


def _verdict(
    outcome: str,
    *,
    failures: int,
    reason: str,
    detail: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "outcome": outcome,
        "failures": int(failures),
        "reason": reason,
        "detail": detail or {},
    }


def _result(
    *,
    ok: bool,
    skipped: str = "",
    reason: str = "",
    candidates: int = 0,
    enqueued: int = 0,
    skipped_accounts: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    items = skipped_accounts or []
    return {
        "ok": ok,
        "skipped": skipped,
        "reason": reason,
        "candidates": candidates,
        "enqueued": enqueued,
        "skippedCount": len(items),
        "skippedAccounts": items,
    }
