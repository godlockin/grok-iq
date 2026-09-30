from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger

from app.core.clock import to_app_timezone, utc_now
from app.core.config import Settings
from app.persistence.probe_repository import ProbeRepository

from .probe_manager import ProbeManager

RequestAuditCallback = Callable[[], Awaitable[dict[str, Any]]]
QualityRetryCallback = Callable[[], Awaitable[dict[str, Any]]]
AccountReconcileCallback = Callable[[], Awaitable[dict[str, Any]]]
AccountRecheckCallback = Callable[[], Awaitable[dict[str, Any]]]

logger = logging.getLogger(__name__)


class SchedulerService:
    def __init__(
        self,
        *,
        settings: Settings,
        repository: ProbeRepository,
        probes: ProbeManager,
        recovery_callback: Callable[[], Awaitable[dict[str, Any]]],
        request_audit_callback: RequestAuditCallback | None = None,
        quality_retry_callback: QualityRetryCallback | None = None,
        account_reconcile_callback: AccountReconcileCallback | None = None,
        account_recheck_callback: AccountRecheckCallback | None = None,
    ):
        self.settings = settings
        self.repository = repository
        self.probes = probes
        self.recovery_callback = recovery_callback
        self.request_audit_callback = request_audit_callback
        self.quality_retry_callback = quality_retry_callback
        self.account_reconcile_callback = account_reconcile_callback
        self.account_recheck_callback = account_recheck_callback
        self.scheduler = AsyncIOScheduler(timezone=settings.scheduler_timezone)

    async def start(self) -> None:
        self.scheduler = AsyncIOScheduler(timezone=self.settings.scheduler_timezone)
        self.scheduler.start()
        self.reload()

    async def stop(self) -> None:
        if self.scheduler.running:
            self.scheduler.shutdown(wait=False)

    async def reconfigure(self) -> None:
        """Rebuild APScheduler so timezone and enablement changes apply now."""

        await self.stop()
        await self.start()

    @staticmethod
    def validate_cron(expression: str, timezone: str) -> None:
        try:
            zone = ZoneInfo(timezone)
        except ZoneInfoNotFoundError as exc:
            raise ValueError("时区名称无效") from exc
        try:
            CronTrigger.from_crontab(expression, timezone=zone)
        except ValueError as exc:
            raise ValueError(f"Cron 表达式无效: {exc}") from exc

    def reload(self) -> None:
        if not self.scheduler.running:
            return
        for job in list(self.scheduler.get_jobs()):
            self.scheduler.remove_job(job.id)
        if self.settings.scheduler_enabled:
            for plan in self.repository.list_plans():
                if plan["enabled"]:
                    self._add_plan_job(plan)
        if self.settings.quarantine_recovery_enabled:
            self.scheduler.add_job(
                self._run_recovery,
                CronTrigger.from_crontab(
                    self.settings.recovery_cron,
                    timezone=ZoneInfo(self.settings.scheduler_timezone),
                ),
                id="system:quarantine-recovery",
                name="隔离恢复检查",
                replace_existing=True,
                coalesce=True,
                max_instances=1,
                misfire_grace_time=self.settings.scheduler_misfire_grace_seconds,
            )
        if self.settings.account_reconcile_enabled and self.account_reconcile_callback is not None:
            self.scheduler.add_job(
                self._run_account_reconcile,
                CronTrigger.from_crontab(
                    self.settings.account_reconcile_cron,
                    timezone=ZoneInfo(self.settings.scheduler_timezone),
                ),
                id="system:account-reconcile",
                name="账号对账清理",
                replace_existing=True,
                coalesce=True,
                max_instances=1,
                misfire_grace_time=self.settings.scheduler_misfire_grace_seconds,
            )
        if self._request_audit_schedule_enabled():
            # A one-shot job is re-armed after every scan.  The scan result
            # carries a busy/normal/idle recommendation, so upstream traffic
            # controls the next interval instead of a fixed five-minute cron.
            self._schedule_request_audit(5)
        if self._quality_retry_schedule_enabled():
            self._schedule_quality_retry(5)
        if self._account_recheck_schedule_enabled():
            # Re-verification is a one-shot timer, not a cron: each account
            # carries its own randomized due time, and this job only drains the
            # accounts that have come due since the last pass.
            self._schedule_account_recheck(10)

    def _request_audit_schedule_enabled(self) -> bool:
        return bool(
            self.settings.scheduler_enabled
            and self.settings.request_audit_enabled
            and self.settings.request_audit_auto_scan_enabled
            and self.request_audit_callback is not None
        )

    def _quality_retry_schedule_enabled(self) -> bool:
        return bool(
            self.settings.quality_retry_isolation_enabled
            and self.quality_retry_callback is not None
        )

    def _quality_retry_delay(self) -> int:
        return max(
            15,
            min(int(self.settings.quality_retry_isolation_interval_seconds), 600),
        )

    def _account_recheck_schedule_enabled(self) -> bool:
        return bool(
            self.settings.scheduler_enabled
            and self.settings.quarantine_recheck_enabled
            and self.account_recheck_callback is not None
        )

    def _account_recheck_delay(self) -> int:
        """Seconds until the next drain of the due-recheck queue.

        This is the smallest configured re-check window, because a due account
        must not wait longer than its own earliest possible schedule. A larger
        interval would silently coarsen every account's window, and a shorter
        one would only re-query an empty queue. The 60s floor keeps the job from
        becoming a busy loop under a very small configured window.
        """

        smallest = min(
            max(int(self.settings.quarantine_recheck_minutes), 1),
            max(int(self.settings.quarantine_recheck_max_minutes), 1),
        )
        return max(60, min(smallest * 60, 24 * 60 * 60))

    def _schedule_account_recheck(self, delay_seconds: int) -> None:
        if not self.scheduler.running or not self._account_recheck_schedule_enabled():
            return
        delay = max(30, min(int(delay_seconds), 24 * 60 * 60))
        self.scheduler.add_job(
            self._run_account_recheck,
            DateTrigger(
                run_date=utc_now() + timedelta(seconds=delay),
                timezone=ZoneInfo(self.settings.scheduler_timezone),
            ),
            id="system:account-recheck",
            name="隔离账号复检与复活",
            replace_existing=True,
            coalesce=True,
            max_instances=1,
            misfire_grace_time=self.settings.scheduler_misfire_grace_seconds,
        )

    def _schedule_request_audit(self, delay_seconds: int) -> None:
        if not self.scheduler.running or not self._request_audit_schedule_enabled():
            return
        delay = max(5, min(int(delay_seconds), 24 * 60 * 60))
        self.scheduler.add_job(
            self._run_request_audit_scan,
            DateTrigger(
                run_date=utc_now() + timedelta(seconds=delay),
                timezone=ZoneInfo(self.settings.scheduler_timezone),
            ),
            id="system:request-audit-scan",
            name="请求审计风险扫描",
            replace_existing=True,
            coalesce=True,
            max_instances=1,
            misfire_grace_time=self.settings.scheduler_misfire_grace_seconds,
        )

    def _schedule_quality_retry(self, delay_seconds: int) -> None:
        if not self.scheduler.running or not self._quality_retry_schedule_enabled():
            return
        delay = max(5, min(int(delay_seconds), 24 * 60 * 60))
        self.scheduler.add_job(
            self._run_quality_retry_isolation,
            DateTrigger(
                run_date=utc_now() + timedelta(seconds=delay),
                timezone=ZoneInfo(self.settings.scheduler_timezone),
            ),
            id="system:quality-retry-isolation",
            name="grok2api 降智停用同步",
            replace_existing=True,
            coalesce=True,
            max_instances=1,
            misfire_grace_time=self.settings.scheduler_misfire_grace_seconds,
        )

    def _request_audit_delay(self, result: dict[str, Any]) -> int:
        if not self.settings.request_audit_adaptive_scan_enabled:
            return self.settings.request_audit_scan_interval_minutes * 60
        recommended = result.get("recommendedIntervalSeconds")
        try:
            return int(recommended)
        except (TypeError, ValueError, OverflowError):
            return self.settings.request_audit_normal_scan_interval_seconds

    def _add_plan_job(self, plan: dict[str, Any]) -> None:
        trigger = CronTrigger.from_crontab(
            str(plan["cron_expression"]),
            timezone=ZoneInfo(str(plan["timezone"])),
        )
        self.scheduler.add_job(
            self._run_plan,
            trigger,
            args=[str(plan["id"])],
            id=f"plan:{plan['id']}",
            name=str(plan["name"]),
            replace_existing=True,
            coalesce=True,
            max_instances=1,
            misfire_grace_time=self.settings.scheduler_misfire_grace_seconds,
        )

    async def run_plan_now(self, plan_id: str) -> dict[str, Any]:
        return await self._execute_plan(plan_id)

    async def run_plans_now(self, plan_ids: list[str]) -> dict[str, Any]:
        unique_ids = list(dict.fromkeys(value for value in plan_ids if value))
        summary: dict[str, Any] = {
            "requested": len(unique_ids),
            "processed": 0,
            "created": 0,
            "skipped": 0,
            "failed": 0,
            "restoreBlocked": 0,
            "failures": [],
        }
        for plan_id in unique_ids:
            try:
                result = await self.run_plan_now(plan_id)
            except Exception as exc:
                summary["failed"] += 1
                summary["failures"].append({"id": plan_id, "message": str(exc)})
                continue
            summary["processed"] += 1
            summary["created"] += int(result.get("created", 0))
            summary["skipped"] += int(result.get("skipped", 0))
            summary["restoreBlocked"] += len(
                result.get("restoreBlockedAccountIds", [])
            )
        return summary

    async def _run_plan(self, plan_id: str) -> None:
        await self._execute_plan(plan_id)

    async def _execute_plan(self, plan_id: str) -> dict[str, Any]:
        key = f"plan:{plan_id}"
        execution_id = self.repository.start_schedule_execution(key)
        try:
            plan = self.repository.get_plan(plan_id)
            if plan is None or not plan["enabled"]:
                result = {"created": 0, "skipped": 0, "reason": "plan_disabled_or_deleted"}
                self.repository.finish_schedule_execution(
                    execution_id, status="skipped", message="计划已停用或删除", detail=result
                )
                return result
            result = await self.probes.enqueue_plan(plan)
            status = "skipped" if result.get("created", 0) == 0 else "succeeded"
            restore_blocked = len(result.get("restoreBlockedAccountIds", []))
            if status == "skipped":
                message = f"{restore_blocked} 个账号等待原设置同步" if restore_blocked else "未创建新任务"
            else:
                message = f"创建 {result['created']} 个任务"
                if restore_blocked:
                    message += f"，{restore_blocked} 个账号等待原设置同步"
            self.repository.finish_schedule_execution(
                execution_id,
                status=status,
                message=message,
                detail=result,
            )
            return result
        except Exception as exc:
            self.repository.finish_schedule_execution(
                execution_id, status="failed", message=str(exc), detail={}
            )
            logger.exception("scheduled probe plan %s failed", plan_id)
            raise

    async def _run_account_reconcile(self) -> None:
        if self.account_reconcile_callback is None:
            return
        execution_id = self.repository.start_schedule_execution("system:account-reconcile")
        try:
            result = await self.account_reconcile_callback()
            removed = int(result.get("removed") or 0)
            skipped = result.get("skipped")
            if skipped:
                message = f"已跳过：{skipped}"
                status = "skipped"
            elif removed:
                message = f"清理 {removed} 个已不存在的账号评估"
                status = "succeeded"
            else:
                message = "无幽灵账号"
                status = "succeeded"
            self.repository.finish_schedule_execution(
                execution_id,
                status=status,
                message=message,
                detail=result,
            )
        except Exception as exc:
            self.repository.finish_schedule_execution(
                execution_id,
                status="failed",
                message=str(exc),
                detail={},
            )
            logger.exception("account reconcile failed")

    async def _run_recovery(self) -> None:
        execution_id = self.repository.start_schedule_execution("system:quarantine-recovery")
        try:
            result = await self.recovery_callback()
            guarded = int(result.get("guarded", 0))
            message = f"恢复 {result['restored']} 个账号"
            if guarded:
                message += f"，{guarded} 个已设为最低优先级"
            self.repository.finish_schedule_execution(
                execution_id,
                status="succeeded",
                message=message,
                detail=result,
            )
        except Exception as exc:
            self.repository.finish_schedule_execution(
                execution_id, status="failed", message=str(exc), detail={}
            )
            logger.exception("quarantine recovery failed")

    async def _run_request_audit_scan(self) -> None:
        if self.request_audit_callback is None:
            return
        execution_id = self.repository.start_schedule_execution("system:request-audit-scan")
        result: dict[str, Any] = {}
        try:
            result = await self.request_audit_callback()
            ok = bool(result.get("ok", True))
            skipped = bool(result.get("skipped"))
            if skipped:
                status = "skipped"
                message = str(result.get("error") or "请求审计扫描已跳过")
            elif ok:
                status = "succeeded"
                count = int(result.get("newRecords", 0))
                scan_state = result.get("state") or {}
                pending = isinstance(scan_state, dict) and not bool(
                    scan_state.get("initialComplete", True)
                )
                message = (
                    f"批量扫描读取 {count} 条，游标待续传"
                    if pending
                    else f"增量读取 {count} 条请求审计"
                )
            else:
                status = "failed"
                message = str(result.get("error") or "请求审计扫描失败")
            activity = result.get("activity") or {}
            if isinstance(activity, dict) and activity.get("label"):
                message += f"，当前{activity['label']}"
            next_seconds = result.get("recommendedIntervalSeconds")
            if next_seconds is not None:
                message += f"，{int(next_seconds)} 秒后再扫描"
            self.repository.finish_schedule_execution(
                execution_id,
                status=status,
                message=message,
                detail=result,
            )
        except Exception as exc:
            result = {
                "ok": False,
                "error": str(exc),
                "recommendedIntervalSeconds": (
                    self.settings.request_audit_normal_scan_interval_seconds
                ),
            }
            self.repository.finish_schedule_execution(
                execution_id,
                status="failed",
                message=str(exc),
                detail={},
            )
            logger.exception("request audit scan failed")
        finally:
            self._schedule_request_audit(self._request_audit_delay(result))

    async def _run_quality_retry_isolation(self) -> None:
        if self.quality_retry_callback is None:
            return
        execution_id = self.repository.start_schedule_execution(
            "system:quality-retry-isolation"
        )
        result: dict[str, Any] = {}
        try:
            result = await self.quality_retry_callback()
            skipped = bool(result.get("skipped"))
            ok = bool(result.get("ok", True))
            if skipped:
                status = "skipped"
                message = str(result.get("error") or "grok2api 降智停用同步已跳过")
            elif ok:
                status = "succeeded"
                isolated = int(result.get("isolated") or 0)
                already = int(result.get("alreadyIsolated") or 0)
                failed = int(result.get("failed") or 0)
                message = f"新隔离 {isolated} 个"
                if already:
                    message += f"，已在隔离区 {already} 个"
                if failed:
                    message += f"，失败 {failed} 个"
            else:
                status = "failed"
                message = str(result.get("error") or "grok2api 降智停用同步失败")
            self.repository.finish_schedule_execution(
                execution_id,
                status=status,
                message=message,
                detail=result,
            )
        except Exception as exc:
            self.repository.finish_schedule_execution(
                execution_id,
                status="failed",
                message=str(exc),
                detail={},
            )
            logger.exception("quality retry isolation scan failed")
        finally:
            self._schedule_quality_retry(self._quality_retry_delay())

    async def _run_account_recheck(self) -> None:
        if self.account_recheck_callback is None:
            return
        execution_id = self.repository.start_schedule_execution(
            "system:account-recheck"
        )
        result: dict[str, Any] = {}
        try:
            result = await self.account_recheck_callback()
            skipped = bool(result.get("skipped"))
            if skipped:
                status = "skipped"
                message = str(result.get("reason") or "隔离账号复检已跳过")
            else:
                status = "succeeded" if bool(result.get("ok", True)) else "failed"
                candidates = int(result.get("candidates") or 0)
                enqueued = int(result.get("enqueued") or 0)
                skipped_count = int(result.get("skippedCount") or 0)
                if candidates == 0:
                    message = "没有到期的隔离账号"
                else:
                    message = f"到期 {candidates} 个，复检 {enqueued} 个"
                    if skipped_count:
                        message += f"，跳过 {skipped_count} 个"
            self.repository.finish_schedule_execution(
                execution_id,
                status=status,
                message=message,
                detail=result,
            )
        except Exception as exc:
            self.repository.finish_schedule_execution(
                execution_id,
                status="failed",
                message=str(exc),
                detail={},
            )
            logger.exception("account recheck scan failed")
        finally:
            self._schedule_account_recheck(self._account_recheck_delay())

    def status(self) -> dict[str, Any]:
        jobs = {
            job.id: {
                "id": job.id,
                "name": job.name,
                "nextRunAt": to_app_timezone(job.next_run_time),
            }
            for job in self.scheduler.get_jobs()
        }
        plans = []
        for plan in self.repository.list_plans():
            plans.append({**plan, "job": jobs.get(f"plan:{plan['id']}")})
        return {
            "enabled": self.settings.scheduler_enabled,
            "plansEnabled": self.settings.scheduler_enabled,
            "systemRecoveryEnabled": self.settings.quarantine_recovery_enabled,
            "qualityRetryIsolationEnabled": (
                self.settings.quality_retry_isolation_enabled
            ),
            "accountRecheckEnabled": self._account_recheck_schedule_enabled(),
            "running": self.scheduler.running,
            "plans": plans,
            "systemJobs": [value for key, value in jobs.items() if key.startswith("system:")],
            "executions": self.repository.list_schedule_executions(limit=100),
        }
