from __future__ import annotations

import asyncio
import logging
import math
import os
import socket
import threading
from collections import deque
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

import psutil

from app.analyzer import Thresholds, thresholds_from_settings
from app.core.clock import account_created_at, app_isoformat, utc_now
from app.core.config import Settings, should_auto_isolate
from app.core.logging import (
    PROBE_LOG_FILE_NAME,
    PROBE_LOG_RETENTION_DAYS,
    probe_log_size,
    recent_log_lines,
)
from app.integrations.grok2api.client import Grok2APIClient, IntegrationError
from app.persistence.account_repository import AccountRepository
from app.persistence.probe_repository import (
    AccountSettingsSnapshot,
    ProbeRepository,
    RunExecutionContext,
    RunStateError,
)
from app.services.probe_call_runner import ProbeCallRunner
from app.services.probe_cleanup import ProbeCleanupCoordinator, UpstreamCleanupResult
from app.services.probe_plan_enqueuer import ProbePlanEnqueuer
from app.services.probe_run_executor import ProbeRunExecutor
from app.services.probe_runtime import AccountRestoreError, WorkerRuntime
from app.services.probe_target_validator import ProbeTargetValidator
from app.services.probe_worker_loop import ProbeWorkerLoop
from app.services.recheck_schedule import first_recheck_due_at
from app.services.wechat_notification import WeChatAccountNotificationService

if TYPE_CHECKING:
    from app.services.account_service import AccountService

logger = logging.getLogger(__name__)


class ProbeManager:
    """Persistent bounded queue for manual and Cron-created probe runs."""

    def __init__(
        self,
        *,
        settings: Settings,
        repository: ProbeRepository,
        accounts: AccountRepository,
        client: Grok2APIClient,
        thresholds: Thresholds,
        notifications: WeChatAccountNotificationService | None = None,
        account_service: AccountService | None = None,
        log_path: Path | None = None,
    ):
        self.settings = settings
        self.repository = repository
        self.accounts = accounts
        self.client = client
        self.thresholds = thresholds
        self.notifications = notifications
        self.account_service = account_service
        self.log_path = log_path or (settings.database_path.resolve().parent / "logs" / PROBE_LOG_FILE_NAME)
        self._process_started_at = utc_now()
        self._process = psutil.Process()
        self._process.cpu_percent(None)
        self._workers: dict[int, asyncio.Task[None]] = {}
        self._worker_runtime: dict[int, WorkerRuntime] = {}
        self._active_calls: dict[str, asyncio.Task[Any]] = {}
        self._event_loop_monitor: asyncio.Task[None] | None = None
        self._event_loop_lag_ms = 0.0
        self._recent_completions: deque[tuple[datetime, bool, float]] = deque()
        self._recent_completions_lock = threading.Lock()
        self._wake = asyncio.Event()
        self._stopping = False
        self._started = False
        self._desired_worker_concurrency = settings.probe_worker_concurrency
        self._claim_lock = asyncio.Lock()
        self._enqueue_lock = asyncio.Lock()
        self._current_probe_lock = asyncio.Lock()
        self._next_current_probe_at = 0.0
        self._target_validator = ProbeTargetValidator(client)
        self._cleanup_coordinator = ProbeCleanupCoordinator(repository, client, accounts)
        self._call_runner = ProbeCallRunner(self, logger)
        self._run_executor = ProbeRunExecutor(self, logger)
        self._worker_loop = ProbeWorkerLoop(self, logger)
        self._plan_enqueuer = ProbePlanEnqueuer(
            settings=settings,
            repository=repository,
            accounts=accounts,
            client=client,
            target_validator=self._target_validator,
            enqueue_lock=self._enqueue_lock,
            wake=self._wake,
        )
        self.register_integration = None
        # Set by application composition. The re-verification service needs
        # this manager to enqueue probes, and this manager needs the service to
        # settle a verdict, so the edge is wired from the outside.
        self.account_recheck = None

    async def start(self) -> None:
        if self._started:
            return
        self.repository.seed_defaults()
        await self._recover_interrupted_runs()
        self._stopping = False
        self._started = True
        self._desired_worker_concurrency = self.settings.probe_worker_concurrency
        self._workers = {}
        for index in range(1, self._desired_worker_concurrency + 1):
            self._spawn_worker(index)
        self._event_loop_monitor = asyncio.create_task(
            self._monitor_event_loop(), name="probe-event-loop-monitor"
        )
        logger.info(
            "worker pool started pid=%s concurrency=%s",
            os.getpid(),
            self._desired_worker_concurrency,
        )
        self._wake.set()

    async def stop(self) -> None:
        self._stopping = True
        for task in list(self._active_calls.values()):
            task.cancel()
        if self._event_loop_monitor is not None:
            self._event_loop_monitor.cancel()
        self._wake.set()
        if self._workers:
            await asyncio.gather(*self._workers.values(), return_exceptions=True)
        if self._event_loop_monitor is not None:
            await asyncio.gather(self._event_loop_monitor, return_exceptions=True)
            self._event_loop_monitor = None
        self._workers.clear()
        self._started = False
        logger.info("worker pool stopped pid=%s", os.getpid())

    async def reconfigure(self) -> None:
        """Hot-apply thresholds and resize the bounded worker pool."""

        self.thresholds = thresholds_from_settings(self.settings)
        self._desired_worker_concurrency = self.settings.probe_worker_concurrency
        if not self._started:
            return
        for index, runtime in self._worker_runtime.items():
            if index > self._desired_worker_concurrency and runtime.status != "stopped":
                self._set_worker_status(runtime, "stopping")
        for index in range(1, self._desired_worker_concurrency + 1):
            task = self._workers.get(index)
            if task is None or task.done():
                self._spawn_worker(index)
        logger.info(
            "worker pool reconfigured desired_concurrency=%s live_workers=%s",
            self._desired_worker_concurrency,
            sum(not task.done() for task in self._workers.values()),
        )
        self._wake.set()

    async def enqueue_manual(
        self,
        *,
        account_id: int,
        profile_id: str,
        rounds: int,
        proxy_targets: list[dict[str, Any]],
        execution_mode: str = "chat",
    ) -> str:
        self._ensure_account_restore_ready(account_id)
        account = await self.client.get_account(account_id)
        targets = await self.validate_targets(proxy_targets, execution_mode=execution_mode)
        self._validate_account_for_targets(account, targets)
        async with self._enqueue_lock:
            run_id = self.repository.create_run(
                account_id=account_id,
                account_name=str(account.get("name") or f"account-{account_id}"),
                account_email=str(account.get("email") or ""),
                account_created_at=account_created_at(account),
                profile_id=profile_id,
                execution_mode=execution_mode,
                rounds=rounds,
                proxy_targets=targets,
                trigger="manual",
                priority=100,
                queue_limit=self.settings.probe_queue_limit,
            )
        self._wake.set()
        return run_id

    async def enqueue_manual_batch(
        self,
        *,
        account_ids: list[int],
        profile_id: str,
        rounds: int,
        proxy_targets: list[dict[str, Any]],
        execution_mode: str = "chat",
        profile_ids: list[str] | None = None,
    ) -> dict[str, Any]:
        selected_profile_ids = list(
            dict.fromkeys(str(value).strip() for value in (profile_ids or [profile_id]) if str(value).strip())
        )
        requested_ids = {int(account_id) for account_id in account_ids if int(account_id) > 0}
        upstream_accounts, targets = await asyncio.gather(
            self.client.list_all_accounts(requested_ids),
            self.validate_targets(proxy_targets, execution_mode=execution_mode),
        )
        by_id = {int(account.get("id") or 0): account for account in upstream_accounts}
        missing = sorted(requested_ids - by_id.keys())
        invalid: list[dict[str, Any]] = []
        valid_accounts: list[dict[str, Any]] = []
        diagnostic: list[int] = []
        for account_id in sorted(requested_ids & by_id.keys()):
            account = by_id[account_id]
            try:
                self._validate_account_for_targets(account, targets)
            except ValueError as exc:
                invalid.append({"id": account_id, "reason": str(exc)})
                continue
            valid_accounts.append(account)
            if not bool(account.get("enabled")):
                diagnostic.append(account_id)

        async with self._enqueue_lock:
            result = self.repository.create_manual_runs_batch(
                accounts=valid_accounts,
                profile_ids=selected_profile_ids,
                rounds=rounds,
                proxy_targets=targets,
                execution_mode=execution_mode,
                priority=100,
                queue_limit=self.settings.probe_queue_limit,
            )
        created = len(result["runIds"])
        if created:
            self._wake.set()
        skipped_ids = set(missing)
        skipped_ids.update(item["id"] for item in invalid)
        skipped_ids.update(result["activeAccountIds"])
        skipped_ids.update(result["restoreBlockedAccountIds"])
        invalid_reasons = {item["id"]: item["reason"] for item in invalid}
        skipped_accounts = []
        for account_id in sorted(skipped_ids):
            account = by_id.get(account_id, {})
            if account_id in missing:
                code = "missing"
                reason = f"账号 {account_id} 已不在 grok2api 账号列表中"
            elif account_id in result["restoreBlockedAccountIds"]:
                code = "restore_blocked"
                reason = f"账号 {account_id} 存在未完成的原设置恢复，请先在历史任务中同步"
            elif account_id in result["activeAccountIds"]:
                code = "active_run"
                reason = f"账号 {account_id} 已有排队或执行中的探针任务，请等待其结束"
            else:
                code = "invalid"
                reason = invalid_reasons.get(account_id, f"账号 {account_id} 不满足创建条件")
            skipped_accounts.append(
                {
                    "id": account_id,
                    "name": str(account.get("name") or ""),
                    "email": str(account.get("email") or ""),
                    "code": code,
                    "reason": reason,
                }
            )
        return {
            "requested": len(requested_ids),
            "requestedTasks": len(requested_ids) * len(selected_profile_ids),
            "profileIds": result["profileIds"],
            "created": created,
            "skipped": len(skipped_ids),
            "missingAccountIds": missing,
            "invalidAccounts": invalid,
            "activeAccountIds": result["activeAccountIds"],
            "restoreBlockedAccountIds": result["restoreBlockedAccountIds"],
            "diagnosticAccountIds": diagnostic,
            "skippedAccounts": skipped_accounts,
            "runIds": result["runIds"],
        }

    async def enqueue_register_event(
        self,
        *,
        source_event_id: str,
        account: dict[str, Any],
        profile_ids: list[str],
        execution_mode: str,
        rounds: int | dict[str, int],
        proxy_targets: list[dict[str, Any]],
    ) -> dict[str, Any]:
        account_id = int(account.get("id") or 0)
        self._ensure_account_restore_ready(account_id)
        targets = await self.validate_targets(proxy_targets, execution_mode=execution_mode)
        self._validate_account_for_targets(account, targets)
        async with self._enqueue_lock:
            result = self.repository.create_register_runs(
                source_event_id=source_event_id,
                account=account,
                profile_ids=profile_ids,
                execution_mode=execution_mode,
                rounds=rounds,
                proxy_targets=targets,
                priority=150,
                queue_limit=self.settings.probe_queue_limit,
            )
        if result["runIds"]:
            self._wake.set()
        return result

    async def enqueue_plan(self, plan: dict[str, Any]) -> dict[str, Any]:
        return await self._plan_enqueuer.enqueue(plan)

    async def enqueue_recheck(
        self,
        *,
        account_id: int,
        profile_id: str,
        rounds: int,
        proxy_targets: list[dict[str, Any]],
        execution_mode: str = "chat",
        priority: int = 100,
    ) -> str | None:
        """Queue one GrokIQ-owned re-verification probe.

        Returns the run ID, or ``None`` when the account cannot be probed
        right now (already covered by an active run, or a settings restore is
        still pending). Callers treat ``None`` as "try again later" rather
        than as evidence about the account.

        ``create_run`` is used instead of the manual batch writer because the
        trigger must be ``recheck``: it marks the run as automatic so the
        operator task list does not present GrokIQ's own housekeeping as a
        manual request, and it lets the verdict be applied after the run.
        """

        self._ensure_account_restore_ready(account_id)
        if self.repository.has_active_run(account_id=account_id):
            return None
        account = await self.client.get_account(account_id)
        self._validate_account_for_targets(account, proxy_targets)
        targets = await self.validate_targets(proxy_targets, execution_mode=execution_mode)
        self._validate_account_for_targets(account, targets)
        async with self._enqueue_lock:
            run_id = self.repository.create_run(
                account_id=account_id,
                account_name=str(account.get("name") or f"account-{account_id}"),
                account_email=str(account.get("email") or ""),
                account_created_at=account_created_at(account),
                profile_id=profile_id,
                execution_mode=execution_mode,
                rounds=rounds,
                proxy_targets=targets,
                trigger="recheck",
                priority=int(priority),
                queue_limit=self.settings.probe_queue_limit,
            )
        self._wake.set()
        return run_id

    async def retry(self, run_id: str) -> str:
        values = self.repository.retry_values(run_id)
        self._ensure_account_restore_ready(int(values["account_id"]))
        account = await self.client.get_account(int(values["account_id"]))
        self._validate_account_for_targets(account, list(values["proxy_targets"]))
        values["account_created_at"] = account_created_at(account) or values.get(
            "account_created_at"
        )
        new_id = self.repository.create_run(
            **values,
            trigger="retry",
            priority=100,
            queue_limit=self.settings.probe_queue_limit,
        )
        self._wake.set()
        return new_id

    async def maybe_restore_register_priority_hold(self, run: dict[str, Any]) -> None:
        service = self.register_integration
        if service is None:
            return
        await service.maybe_restore_priority_hold(run)

    async def maybe_switch_register_probe_egress(
        self,
        run: dict[str, Any],
        finished: dict[str, Any],
    ) -> str | None:
        """Rebind a degraded register probe onto another healthy egress."""

        if not self.settings.register_probe_switch_on_degradation:
            return None
        if self.account_service is None:
            return None
        if str(run.get("trigger") or "") != "register":
            return None
        if str(finished.get("status") or "") not in {"completed", "completed_with_errors"}:
            return None
        summary = finished.get("summary") if isinstance(finished.get("summary"), dict) else {}
        if int(summary.get("anomaly_count") or 0) <= 0:
            return None
        run_id = str(run.get("id") or "")
        if not run_id or self.repository.has_child_run(run_id):
            return None
        account_id = int(run.get("account_id") or 0)
        if account_id <= 0:
            return None
        if self.repository.has_blocking_account_restore(account_id=account_id):
            logger.info(
                "register probe egress switch skipped run=%s account=%s reason=restore_pending",
                run_id,
                account_id,
            )
            return None
        account = await self.client.get_account(account_id)
        self._validate_account_for_targets(
            account, list(run.get("proxy_targets") or [])
        )
        tried_node_ids = self.repository.register_tried_egress_node_ids(run)
        current_node_id = int(account.get("egressNodeId") or 0)
        if current_node_id > 0:
            tried_node_ids.add(current_node_id)
        rebound = await self.account_service.rebind_account_egress(
            account,
            exclude_node_ids=tried_node_ids,
        )
        if rebound is None:
            logger.info(
                "register probe egress switch exhausted run=%s account=%s tried=%s",
                run_id,
                account_id,
                sorted(tried_node_ids),
            )
            return None
        self._validate_account_for_targets(rebound, list(run.get("proxy_targets") or []))
        async with self._enqueue_lock:
            follow_up_id = self.repository.create_run(
                account_id=account_id,
                account_name=str(
                    rebound.get("name") or run.get("account_name") or f"account-{account_id}"
                ),
                account_email=str(rebound.get("email") or run.get("account_email") or ""),
                account_created_at=account_created_at(rebound)
                or run.get("account_created_at"),
                profile_id=str(run.get("profile_id") or ""),
                execution_mode=str(run.get("execution_mode") or "chat"),
                rounds=int(run.get("rounds") or 1),
                proxy_targets=list(run.get("proxy_targets") or []),
                trigger="register",
                priority=100,
                queue_limit=self.settings.probe_queue_limit,
                parent_run_id=run_id,
                source_event_id=str(run.get("source_event_id") or "") or None,
            )
        self._wake.set()
        logger.info(
            "register probe egress switched run=%s follow_up=%s account=%s from=%s to=%s",
            run_id,
            follow_up_id,
            account_id,
            current_node_id or None,
            int(rebound.get("egressNodeId") or 0) or None,
        )
        return follow_up_id

    async def cancel(self, run_id: str) -> str:
        status = self.repository.request_cancel(run_id)
        active = self._active_calls.get(run_id)
        if active:
            active.cancel()
        self._wake.set()
        return status

    async def cancel_many(self, run_ids: list[str]) -> dict[str, int]:
        result, interrupt_ids = self.repository.request_cancel_many(run_ids)
        for run_id in interrupt_ids:
            active = self._active_calls.get(run_id)
            if active:
                active.cancel()
        self._wake.set()
        return result

    async def restore_many(self, run_ids: list[str]) -> dict[str, Any]:
        """Replay persisted account snapshots for many runs, one at a time.

        Each restore mutates shared upstream account state behind the claim
        lock, so the runs are processed sequentially and a failing run only
        reports itself instead of aborting the batch.
        """

        requested = list(dict.fromkeys(run_id for run_id in run_ids if run_id))
        restored = 0
        failures: list[dict[str, str]] = []
        for run_id in requested:
            try:
                await self.restore_run_account_settings(run_id)
                restored += 1
            except Exception as exc:
                failures.append({"id": run_id, "error": str(exc)})
        return {
            "requested": len(requested),
            "restored": restored,
            "failed": len(failures),
            "failedRunIds": [failure["id"] for failure in failures],
            "failures": failures,
        }

    async def validate_targets(
        self, targets: list[dict[str, Any]], *, execution_mode: str = "chat"
    ) -> list[dict[str, Any]]:
        return await self._target_validator.validate(targets, execution_mode=execution_mode)

    @staticmethod
    def _validate_probe_account(account: dict[str, Any]) -> None:
        ProbeTargetValidator.validate_account(account)

    @classmethod
    def _validate_account_for_targets(cls, account: dict[str, Any], targets: list[dict[str, Any]]) -> None:
        ProbeTargetValidator.validate_account_for_targets(account, targets)

    def _ensure_account_restore_ready(self, account_id: int) -> None:
        if self.repository.has_blocking_account_restore(account_id=account_id):
            raise RunStateError("该账号存在未完成的原设置恢复，请先在历史任务中人工同步")

    def status(self) -> dict[str, Any]:
        queue = self.repository.worker_queue_stats()
        now = utc_now()
        activity = self._activity_stats(now, queue["oldestQueueWaitSeconds"])
        workers: list[dict[str, Any]] = []
        for index in sorted(set(self._worker_runtime) | set(self._workers)):
            runtime = self._worker_runtime.get(index)
            if runtime is None:
                runtime = self._new_worker_runtime(index)
                self._worker_runtime[index] = runtime
            task = self._workers.get(index)
            status = runtime.status
            if task is not None and task.done() and status not in {"stopped", "restarting"}:
                status = "error"
            elif status == "idle" and queue["queued"] > 0 and queue["eligible"] == 0:
                status = "blocked"
            workers.append(
                {
                    "id": runtime.worker_id,
                    "index": runtime.index,
                    "status": status,
                    "desired": index <= self._desired_worker_concurrency,
                    "taskAlive": bool(task and not task.done()),
                    "startedAt": app_isoformat(runtime.started_at),
                    "stateChangedAt": app_isoformat(runtime.state_changed_at),
                    "lastHeartbeatAt": app_isoformat(runtime.last_heartbeat_at),
                    "completedRuns": runtime.completed_runs,
                    "failedRuns": runtime.failed_runs,
                    "lastError": runtime.last_error,
                    "currentRun": (
                        {
                            "id": runtime.current_run_id,
                            "accountId": runtime.current_account_id,
                            "accountName": runtime.current_account_name,
                            "profileId": runtime.current_profile_id,
                            "profileName": runtime.current_profile_name,
                            "executionMode": runtime.current_execution_mode,
                            "round": runtime.current_round,
                            "targetKey": runtime.current_target_key,
                            "startedAt": (
                                app_isoformat(runtime.current_run_started_at)
                                if runtime.current_run_started_at
                                else None
                            ),
                            "elapsedSeconds": (
                                max(
                                    0,
                                    int(
                                        (
                                            now - runtime.current_run_started_at
                                        ).total_seconds()
                                    ),
                                )
                                if runtime.current_run_started_at
                                else 0
                            ),
                        }
                        if runtime.current_run_id
                        else None
                    ),
                }
            )

        live_workers = sum(bool(task and not task.done()) for task in self._workers.values())
        busy_workers = sum(bool(item["currentRun"]) for item in workers)
        resources = self._process_resources()
        return {
            "process": {
                "pid": os.getpid(),
                "hostname": socket.gethostname(),
                "startedAt": app_isoformat(self._process_started_at),
                "uptimeSeconds": max(0, int((now - self._process_started_at).total_seconds())),
                "model": "single_process_asyncio",
                "resources": resources,
            },
            "started": self._started,
            "stopping": self._stopping,
            "configuredConcurrency": self.settings.probe_worker_concurrency,
            "desiredConcurrency": self._desired_worker_concurrency,
            "liveWorkers": live_workers,
            "busyWorkers": busy_workers,
            "idleWorkers": max(0, live_workers - busy_workers),
            "queue": queue,
            "activity": {
                **activity,
                "activeCalls": len(self._active_calls),
            },
            "workers": workers,
            "policy": {
                "sameAccountSerial": True,
                "reason": (
                    "正常定检保持账号当前出口，诊断任务才会临时修改账号设置；"
                    "为避免同一账号的请求和诊断状态冲突，同账号任务始终顺序执行。"
                ),
            },
            "log": {
                "fileName": self.log_path.name,
                "retentionDays": PROBE_LOG_RETENTION_DAYS,
                "sizeBytes": probe_log_size(self.log_path),
            },
        }

    def _process_resources(self) -> dict[str, int | float | None]:
        try:
            memory = self._process.memory_info()
            open_files = (
                self._process.num_fds()
                if hasattr(self._process, "num_fds")
                else len(self._process.open_files())
            )
            return {
                "cpuPercent": self._process.cpu_percent(None),
                "rssBytes": memory.rss,
                "threads": self._process.num_threads(),
                "openFiles": open_files,
                "eventLoopLagMs": round(self._event_loop_lag_ms, 1),
            }
        except (psutil.Error, OSError):
            logger.debug("process resource sampling failed", exc_info=True)
            return {
                "cpuPercent": None,
                "rssBytes": None,
                "threads": None,
                "openFiles": None,
                "eventLoopLagMs": round(self._event_loop_lag_ms, 1),
            }

    def _activity_stats(
        self, now: datetime, oldest_queue_wait_seconds: int
    ) -> dict[str, int | float]:
        window_seconds = 60
        cutoff = now - timedelta(seconds=window_seconds)
        with self._recent_completions_lock:
            while self._recent_completions and self._recent_completions[0][0] < cutoff:
                self._recent_completions.popleft()
            completed = len(self._recent_completions)
            failed = sum(item[1] for item in self._recent_completions)
            total_duration = sum(item[2] for item in self._recent_completions)
        return {
            "windowSeconds": window_seconds,
            "completed": completed,
            "failed": failed,
            "failureRate": failed / completed if completed else 0,
            "averageDurationSeconds": total_duration / completed if completed else 0,
            "oldestQueueWaitSeconds": oldest_queue_wait_seconds,
        }

    async def _monitor_event_loop(self) -> None:
        loop = asyncio.get_running_loop()
        interval = 1.0
        expected = loop.time() + interval
        while True:
            await asyncio.sleep(interval)
            observed = loop.time()
            self._event_loop_lag_ms = max(0.0, (observed - expected) * 1_000)
            expected = observed + interval

    def logs(self, limit: int) -> dict[str, Any]:
        return {
            "items": recent_log_lines(self.log_path, limit),
            "limit": limit,
            "fileName": self.log_path.name,
            "retentionDays": PROBE_LOG_RETENTION_DAYS,
            "sizeBytes": probe_log_size(self.log_path),
        }

    def _new_worker_runtime(self, index: int) -> WorkerRuntime:
        now = utc_now()
        return WorkerRuntime(
            index=index,
            worker_id=f"worker-{index}",
            status="starting",
            started_at=now,
            state_changed_at=now,
            last_heartbeat_at=now,
        )

    def _spawn_worker(self, index: int) -> None:
        runtime = self._new_worker_runtime(index)
        self._worker_runtime[index] = runtime
        task = asyncio.create_task(self._worker(index), name=f"probe-worker-{index}")
        self._workers[index] = task
        task.add_done_callback(
            lambda finished, worker_index=index: self._handle_worker_exit(worker_index, finished)
        )

    def _handle_worker_exit(self, index: int, task: asyncio.Task[None]) -> None:
        if self._workers.get(index) is not task:
            return
        runtime = self._worker_runtime.get(index)
        if self._stopping or index > self._desired_worker_concurrency:
            if runtime is not None:
                self._set_worker_status(runtime, "stopped")
            return
        error = ""
        if task.cancelled():
            error = "Worker task cancelled unexpectedly"
        else:
            exception = task.exception()
            if exception is not None:
                error = str(exception)
        if runtime is not None:
            runtime.last_error = error
            self._set_worker_status(runtime, "restarting")
        logger.error(
            "worker exited unexpectedly worker=%s error=%s; restarting",
            f"worker-{index}",
            error or "task returned",
        )
        self._spawn_worker(index)

    @staticmethod
    def _set_worker_status(runtime: WorkerRuntime, status: str) -> None:
        now = utc_now()
        if runtime.status != status:
            runtime.status = status
            runtime.state_changed_at = now
        runtime.last_heartbeat_at = now

    @staticmethod
    def _clear_worker_run(runtime: WorkerRuntime) -> None:
        runtime.current_run_id = ""
        runtime.current_account_id = None
        runtime.current_account_name = ""
        runtime.current_profile_id = ""
        runtime.current_profile_name = ""
        runtime.current_execution_mode = ""
        runtime.current_round = None
        runtime.current_target_key = ""
        runtime.current_run_started_at = None

    async def _worker(self, index: int) -> None:
        await self._worker_loop.run(index)

    async def _execute(
        self,
        context: RunExecutionContext,
        runtime: WorkerRuntime,
    ) -> dict[str, Any]:
        return await self._run_executor.execute(context, runtime)

    async def _run_probe_call(
        self,
        *,
        run_id: str,
        account_id: int,
        snapshot: AccountSettingsSnapshot,
        factory: Callable[[], Awaitable[Any]],
    ) -> Any:
        return await self._call_runner.run(
            run_id=run_id,
            account_id=account_id,
            snapshot=snapshot,
            factory=factory,
        )

    async def _transient_retry_delay(
        self,
        *,
        account_id: int,
        error: IntegrationError,
        attempt: int,
    ) -> float:
        """Choose a retry delay without confusing quota with availability.

        grok2api normally sends ``Retry-After`` for cooling responses.  The
        account-scoped 503 used by a pinned temporary route may not include
        that header, so consult the live account record as a second source and
        finally use a bounded exponential fallback.  The configured maximum
        only caps that local fallback: an explicit upstream cooldown must be
        honored even when it is longer.
        """

        base = max(
            0.1,
            float(getattr(self.settings, "probe_transient_retry_base_seconds", 5.0)),
        )
        maximum = max(
            base,
            float(getattr(self.settings, "probe_transient_retry_max_seconds", 30.0)),
        )
        fallback = min(maximum, base * (2**attempt))
        retry_after = self._positive_delay(error.retry_after_seconds)
        account_cooldown = await self._account_cooldown_remaining(account_id)
        delay = max(fallback, retry_after, account_cooldown)
        logger.info(
            "probe retry delay account=%s code=%s retry_after=%.1fs "
            "account_cooldown=%.1fs local_fallback=%.1fs local_max=%.1fs chosen=%.1fs",
            account_id,
            error.error_code or "-",
            retry_after,
            account_cooldown,
            fallback,
            maximum,
            delay,
        )
        return max(0.1, delay)

    async def _wait_for_account_cooldown(
        self,
        run_id: str,
        account_id: int,
        error: IntegrationError,
    ) -> None:
        """Let a final transient failure settle before the next target.

        A failed proxy can cool the account globally in grok2api.  Advancing
        immediately to the next proxy or round would then produce a cascade of
        429/503 samples that say nothing about that next step.  Use either the
        response's ``Retry-After`` value or the live account cooldown, whichever
        is longer.
        """

        if not error.transient:
            return
        retry_after = self._positive_delay(error.retry_after_seconds)
        account_cooldown = await self._account_cooldown_remaining(account_id)
        delay = max(retry_after, account_cooldown)
        if delay <= 0:
            return
        logger.info(
            "probe final transient cooldown run=%s account=%s code=%s "
            "retry_after=%.1fs account_cooldown=%.1fs chosen=%.1fs",
            run_id,
            account_id,
            error.error_code or "-",
            retry_after,
            account_cooldown,
            delay,
        )
        await self._sleep_probe_delay(run_id, delay)

    async def _account_cooldown_remaining(self, account_id: int) -> float:
        """Return the upstream account's remaining cooldown when available."""

        try:
            account = await self.client.get_account(account_id)
            cooldown_until = account.get("cooldownUntil")
            if not cooldown_until:
                return 0.0
            value = datetime.fromisoformat(str(cooldown_until).replace("Z", "+00:00"))
            if value.tzinfo is None:
                value = value.replace(tzinfo=UTC)
            remaining = (value.astimezone(UTC) - datetime.now(UTC)).total_seconds()
            return self._positive_delay(remaining)
        except asyncio.CancelledError:
            raise
        except Exception:
            # Retry-After and the local fallback remain usable when the admin
            # endpoint is briefly unavailable or returns an invalid timestamp.
            return 0.0

    @staticmethod
    def _positive_delay(value: Any) -> float:
        try:
            delay = float(value or 0.0)
        except (TypeError, ValueError, OverflowError):
            return 0.0
        return delay if math.isfinite(delay) and delay > 0 else 0.0

    async def _sleep_probe_delay(self, run_id: str, seconds: float) -> None:
        """Sleep in short slices so cancellation and shutdown stay responsive."""

        deadline = asyncio.get_running_loop().time() + max(0.0, seconds)
        while True:
            if self._stopping or self.repository.is_cancel_requested(run_id):
                raise asyncio.CancelledError
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                return
            await asyncio.sleep(min(0.5, remaining))

    async def _wait_for_current_probe_slot(self, run_id: str) -> None:
        """Rate-limit normal checks across all workers without affecting diagnostics."""

        interval = max(
            0.0,
            float(
                getattr(
                    self.settings,
                    "probe_current_egress_interval_seconds",
                    10.0,
                )
            ),
        )
        if interval <= 0:
            return
        async with self._current_probe_lock:
            loop = asyncio.get_running_loop()
            delay = self._next_current_probe_at - loop.time()
            if delay > 0:
                await self._sleep_probe_delay(run_id, delay)
            self._next_current_probe_at = loop.time() + interval

    async def _restore_diagnostic_activation(
        self,
        *,
        run_id: str,
        account_id: int,
        snapshot: AccountSettingsSnapshot,
    ) -> None:
        await self._cleanup_coordinator.restore_diagnostic_activation(
            run_id=run_id,
            account_id=account_id,
            snapshot=snapshot,
        )

    async def _cleanup_upstream(
        self,
        *,
        run_id: str,
        account_id: int,
        snapshot: AccountSettingsSnapshot | None,
        legacy_original_node_id: int | None,
        legacy_original_mode: str,
        route_id: str,
        client_key_id: str,
        restore_egress: bool,
        source: str,
        account_restore_started: bool = False,
    ) -> UpstreamCleanupResult:
        return await self._cleanup_coordinator.cleanup(
            run_id=run_id,
            account_id=account_id,
            snapshot=snapshot,
            legacy_original_node_id=legacy_original_node_id,
            legacy_original_mode=legacy_original_mode,
            route_id=route_id,
            client_key_id=client_key_id,
            restore_egress=restore_egress,
            source=source,
            account_restore_started=account_restore_started,
        )

    async def _recover_interrupted_runs(self) -> None:
        for run in self.repository.interrupted_runs():
            run_id = str(run["id"])
            cleanup_result = await self._cleanup_upstream(
                run_id=run_id,
                account_id=int(run["account_id"]),
                snapshot=self.repository.account_settings_snapshot(run_id),
                legacy_original_node_id=run["original_egress_node_id"],
                legacy_original_mode=str(run["original_egress_assignment_mode"] or ""),
                route_id=str(run["temporary_route_id"] or ""),
                client_key_id=str(run["temporary_client_key_id"] or ""),
                restore_egress=self._run_changes_egress(run),
                source="startup",
            )
            if not cleanup_result.resource_errors:
                self.repository.clear_upstream_context(run_id)
            self.repository.finish_recovery(run_id, "; ".join(cleanup_result.errors))
        try:
            cleaned = await self.client.cleanup_stale_resources()
            if any(cleaned.values()):
                logger.info("cleaned stale grok2api probe resources: %s", cleaned)
        except Exception as exc:
            logger.warning("stale probe resource cleanup skipped: %s", exc)

    async def restore_run_account_settings(self, run_id: str) -> dict[str, Any]:
        """Replay a run's persisted snapshot as an operator compensation action."""

        run = self.repository.get_run(run_id)
        if run is None:
            raise ValueError("探针任务不存在")
        if run["status"] not in {"completed", "completed_with_errors", "failed", "cancelled"}:
            raise RunStateError("任务执行期间由自动恢复流程管理账号设置")
        account_id = int(run["account_id"])
        snapshot = self.repository.account_settings_snapshot(run_id)
        restore_egress = self._run_changes_egress(run)
        if snapshot is None and not restore_egress:
            raise RunStateError("该任务没有可同步的账号原设置记录")
        if (
            not restore_egress
            and not bool(run.get("diagnostic_activation_active"))
            and str(run.get("account_restore_status") or "") != "restore_failed"
        ):
            raise RunStateError("正常定检未修改账号设置，无需同步")

        async with self._claim_lock:
            if self.repository.has_executing_run(
                account_id=account_id,
                exclude_run_id=run_id,
            ):
                raise RunStateError("该账号存在执行中的探针任务")
            self.repository.begin_account_restore(run_id, "manual")

        cleanup_result = await self._cleanup_upstream(
            run_id=run_id,
            account_id=account_id,
            snapshot=snapshot,
            legacy_original_node_id=(
                snapshot.egress_node_id if snapshot is not None else run.get("original_egress_node_id")
            ),
            legacy_original_mode=(
                snapshot.egress_assignment_mode
                if snapshot is not None
                else str(run.get("original_egress_assignment_mode") or "")
            ),
            route_id=str(run.get("temporary_route_id") or ""),
            client_key_id=str(run.get("temporary_client_key_id") or ""),
            restore_egress=restore_egress,
            source="manual",
            account_restore_started=True,
        )
        if cleanup_result.account_errors:
            raise AccountRestoreError("; ".join(cleanup_result.account_errors))
        if cleanup_result.resource_errors:
            logger.warning(
                "account settings restored for run %s with resource cleanup warnings: %s",
                run_id,
                "; ".join(cleanup_result.resource_errors),
            )
        else:
            self.repository.clear_upstream_context(run_id)
        self._wake.set()
        return self.repository.get_run(run_id) or {}

    async def _apply_auto_quarantine(
        self,
        account_id: int,
        assessment: dict[str, Any],
    ) -> dict[str, Any]:
        status = str(assessment.get("monitor_status") or "")
        if should_auto_isolate(
            status,
            enabled=self.settings.auto_isolation_enabled,
            min_status=self.settings.auto_isolation_min_status,
        ):
            if self.account_service is not None:
                result = await self.account_service.isolate_account(
                    account_id,
                    note="风险周期达到自动隔离区阈值",
                    source="probe",
                    automatic=True,
                    detail={
                        "riskScore": float(assessment.get("risk_score") or 0),
                        "monitorStatus": status,
                        "minStatus": self.settings.auto_isolation_min_status,
                        "riskReasons": list(assessment.get("risk_reasons") or []),
                    },
                )
                return result.get("assessment") or assessment
            return await self._isolate_account_fallback(account_id, assessment)

        if not self.settings.auto_quarantine or status != "high_risk":
            return assessment
        if assessment.get("disabled_by_monitor"):
            return assessment
        if self.account_service is not None:
            result = await self.account_service.apply_auto_quarantine(
                account_id,
                source="probe",
                note="风险周期连续强异常达到自动隔离阈值",
                risk_score=float(assessment.get("risk_score") or 0),
            )
            return result.get("assessment") or assessment
        account = await self.client.get_account(account_id)
        was_enabled = bool(account.get("enabled"))
        if was_enabled:
            await self.client.set_account_enabled(account_id, False)
        until = (
            utc_now() + timedelta(minutes=self.settings.quarantine_minutes)
            if self.settings.auto_quarantine_recovery_enabled
            else None
        )
        quarantined = self.accounts.set_manual_status(
            account_id=account_id,
            status="quarantined",
            note="风险周期连续强异常达到自动隔离阈值",
            quarantine_until=until,
            previous_upstream_enabled=was_enabled,
            disabled_by_monitor=was_enabled,
            recovery_guarded=False,
            source="probe",
            evidence=list(assessment.get("risk_reasons") or []),
            recheck_due_at=None if until is not None else first_recheck_due_at(
                now=utc_now(),
                min_minutes=self.settings.quarantine_recheck_minutes,
                max_minutes=self.settings.quarantine_recheck_max_minutes,
            ),
            reset_recheck_failures=True,
        )
        self.accounts.create_alert(
            account_id=account_id,
            kind="auto_quarantine",
            severity="critical",
            title="账号已被自动停用",
            detail={
                "quarantineUntil": app_isoformat(until),
                "recoveryMode": (
                    "temporary"
                    if self.settings.auto_quarantine_recovery_enabled
                    else "permanent"
                ),
                "riskScore": assessment["risk_score"],
            },
        )
        return quarantined

    async def _isolate_account_fallback(
        self,
        account_id: int,
        assessment: dict[str, Any],
    ) -> dict[str, Any]:
        account = await self.client.get_account(account_id)
        was_enabled = bool(account.get("enabled"))
        if was_enabled:
            await self.client.set_account_enabled(account_id, False)
        isolated = self.accounts.set_manual_status(
            account_id=account_id,
            status="quarantined",
            note="风险周期达到自动隔离区阈值",
            quarantine_until=None,
            previous_upstream_enabled=was_enabled,
            disabled_by_monitor=was_enabled,
            recovery_guarded=False,
            source="probe",
            disposition_action="isolate",
            evidence=list(assessment.get("risk_reasons") or []),
            recheck_due_at=first_recheck_due_at(
                now=utc_now(),
                min_minutes=self.settings.quarantine_recheck_minutes,
                max_minutes=self.settings.quarantine_recheck_max_minutes,
            ),
            reset_recheck_failures=True,
        )
        self.accounts.create_alert(
            account_id=account_id,
            kind="auto_isolate",
            severity="critical",
            title="账号已被自动移入隔离区",
            detail={
                "quarantineUntil": None,
                "recoveryMode": "permanent",
                "riskScore": assessment.get("risk_score"),
            },
        )
        return isolated

    @staticmethod
    def _target_key(target: dict[str, Any]) -> str:
        kind = target.get("kind")
        if kind in {"current", "direct"}:
            return str(kind)
        return f"egress:{int(target.get('id') or 0)}"

    @staticmethod
    def _run_changes_egress(run: dict[str, Any]) -> bool:
        if str(run.get("execution_mode") or "chat") != "chat":
            return False
        return any(target.get("kind") in {"direct", "egress"} for target in run.get("proxy_targets") or [])

    @staticmethod
    def _verify_probe_egress(
        *,
        target: dict[str, Any],
        original_node_id: int | None,
        result: Any,
    ) -> None:
        kind = str(target.get("kind") or "")
        verified_node_id = result.verified_egress_node_id
        expected_node_id = original_node_id if kind == "current" else int(target.get("id") or 0) or None
        if kind == "direct" or expected_node_id == verified_node_id:
            return
        logger.warning(
            "probe egress differed expected=%s actual=%s kind=%s request=%s",
            expected_node_id,
            verified_node_id,
            kind,
            getattr(result, "request_id", ""),
        )

    def _error_sample(
        self,
        round_number: int,
        target: dict[str, Any],
        error: BaseException | str,
        *,
        current_node_id: int | None = None,
    ) -> dict[str, Any]:
        status_code = int(getattr(error, "status_code", 0) or 0)
        error_code = str(getattr(error, "error_code", "") or "")
        request_id = str(getattr(error, "request_id", "") or "")
        retry_after = float(getattr(error, "retry_after_seconds", 0.0) or 0.0)
        attempt_count = int(getattr(error, "attempt_count", 1) or 1)
        message = str(error)
        result = getattr(error, "probe_result", None)
        usage: dict[str, Any] = dict(result.usage or {}) if result is not None else {}
        if error_code or status_code or retry_after or attempt_count > 1:
            usage["probeError"] = {
                "code": error_code,
                "statusCode": status_code,
                "retryAfterSeconds": round(retry_after, 3),
                "attempts": attempt_count,
                "transient": bool(getattr(error, "transient", False)),
            }
        return {
            "round_number": round_number,
            "target_key": self._target_key(target),
            "target_kind": str(target.get("kind") or ""),
            "egress_node_id": (current_node_id if target.get("kind") == "current" else target.get("id")),
            "egress_name": str(target.get("name") or ""),
            "request_id": request_id,
            "audit_id": getattr(error, "audit_id", None),
            "verified_account_id": getattr(error, "verified_account_id", None),
            "verified_egress_node_id": getattr(error, "verified_egress_node_id", None),
            "status": "error",
            "status_code": result.status_code if result is not None else status_code,
            "error_code": error_code,
            "retry_count": max(0, attempt_count - 1),
            "retry_after_seconds": retry_after,
            "output_tokens": result.output_tokens if result is not None else 0,
            "reasoning_tokens": result.reasoning_tokens if result is not None else 0,
            "reasoning_tokens_reported": (
                bool(result.reasoning_tokens_reported) if result is not None else False
            ),
            "visible_tokens": result.visible_tokens if result is not None else 0,
            "chunk_count": result.chunk_count if result is not None else 0,
            "first_token_ms": result.first_token_ms if result is not None else 0,
            "duration_ms": result.duration_ms if result is not None else 0,
            "generation_ms": result.generation_ms if result is not None else 0,
            "first_token_share": result.first_token_share if result is not None else 0.0,
            "tps": result.tps if result is not None else 0.0,
            "expected_matched": result.expected_matched if result is not None else None,
            "response_sha256": result.response_sha256 if result is not None else "",
            "response_text": result.response_text if result is not None else "",
            "reasoning_text": result.reasoning_text if result is not None else "",
            "usage": usage,
            "classification": "error",
            "risk_rule_id": "http_error",
            "risk_rule_ids": ["http_error"],
            "risk_reasons": ["请求错误"],
            "severity": 1,
            "error": message[:4000],
            "created_at": utc_now(),
        }
