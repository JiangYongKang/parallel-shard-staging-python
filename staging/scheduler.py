"""调度器：并发执行分片提交，支持进度查询与取消。"""

from __future__ import annotations

import itertools
import logging
import threading
from concurrent.futures import ThreadPoolExecutor

from .models import (
    RejectionReason,
    ShardRequest,
    ShardResult,
    ShardStatus,
    StagingError,
    TaskInfo,
    TaskNotFoundError,
    TaskStatus,
)
from .store import StagingStore

logger = logging.getLogger("staging.scheduler")


class Scheduler:
    """把一次提交（多个分片）作为一个任务并发落盘。

    - max_workers 控制并发度，排队超过 max_pending 则拒绝（CONCURRENCY_LIMIT）
    - 同一 target 同一时刻只允许一个任务写入，否则拒绝（TARGET_BUSY）
    - 暂存空间不足时拒绝（CAPACITY_EXCEEDED）
    """

    def __init__(self, store: StagingStore, max_workers: int = 4,
                 max_pending: int = 8) -> None:
        self._store = store
        self._max_workers = max_workers
        self._max_pending = max_pending
        self._pool = ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="staging")
        self._tasks: dict[str, TaskInfo] = {}
        self._cancel_flags: dict[str, threading.Event] = {}
        self._target_locks: dict[str, threading.Lock] = {}
        self._inflight = 0          # 已接受但未完成的任务数
        self._ids = itertools.count(1)
        self._lock = threading.Lock()

    def submit(self, shards: list[ShardRequest]) -> str:
        """提交一批分片，返回 task_id；资源不足时抛对应异常。"""
        from .models import CapacityExceededError, ConcurrencyLimitError
        with self._lock:
            if self._inflight >= self._max_workers + self._max_pending:
                raise ConcurrencyLimitError(
                    f"inflight {self._inflight} >= "
                    f"{self._max_workers}+{self._max_pending}")
        # 预检容量：只统计暂存区里还没有的新指纹
        needed = sum(s.size for s in shards if not self._store.has(s.fingerprint))
        if needed > self._store.available_bytes:
            raise CapacityExceededError(
                f"need {needed}B new bytes, available "
                f"{self._store.available_bytes}B "
                f"(limit {self._store.max_bytes}B)")
        with self._lock:
            task_id = f"task-{next(self._ids)}"
            self._tasks[task_id] = TaskInfo(task_id=task_id, total=len(shards))
            self._cancel_flags[task_id] = threading.Event()
            self._inflight += 1
        logger.info("task=%s accepted, shards=%d", task_id, len(shards))
        self._pool.submit(self._run_task, task_id, shards)
        return task_id

    def get(self, task_id: str) -> TaskInfo:
        """查询任务进度。"""
        with self._lock:
            info = self._tasks.get(task_id)
            if info is None:
                raise TaskNotFoundError(task_id)
            # 返回快照，避免调用方改到内部状态
            return TaskInfo(
                task_id=info.task_id, status=info.status, total=info.total,
                finished=info.finished, rejection=info.rejection,
                error=info.error, results=list(info.results))

    def cancel(self, task_id: str) -> TaskInfo:
        """取消任务：未开始的分片不再执行，进行中的分片回滚。"""
        with self._lock:
            if task_id not in self._tasks:
                raise TaskNotFoundError(task_id)
            self._cancel_flags[task_id].set()
        logger.info("task=%s cancel requested", task_id)
        return self.get(task_id)

    def shutdown(self) -> None:
        self._pool.shutdown(wait=True)

    # ---- 内部 ----

    def _run_task(self, task_id: str, shards: list[ShardRequest]) -> None:
        with self._lock:
            self._tasks[task_id].status = TaskStatus.RUNNING
        try:
            for shard in shards:
                if self._cancel_flags[task_id].is_set():
                    self._record(task_id, ShardResult(
                        shard_id=shard.shard_id, target=shard.target,
                        status=ShardStatus.CANCELLED, detail="task cancelled"))
                    continue
                self._run_shard(task_id, shard)
            with self._lock:
                info = self._tasks[task_id]
                statuses = {r.status for r in info.results}
                if statuses == {ShardStatus.CANCELLED} or (
                        self._cancel_flags[task_id].is_set()
                        and ShardStatus.FAILED not in statuses):
                    info.status = TaskStatus.CANCELLED
                elif ShardStatus.FAILED in statuses:
                    info.status = TaskStatus.FAILED
                else:
                    info.status = TaskStatus.SUCCEEDED
                logger.info("task=%s finished status=%s "
                            "finished=%d/%d", task_id, info.status.value,
                            info.finished, info.total)
        finally:
            with self._lock:
                self._inflight -= 1

    def _run_shard(self, task_id: str, shard: ShardRequest) -> None:
        # 同一 target 不允许并发写入：拿不到锁立即拒绝
        with self._lock:
            lock = self._target_locks.setdefault(shard.target, threading.Lock())
        if not lock.acquire(blocking=False):
            self._record(task_id, ShardResult(
                shard_id=shard.shard_id, target=shard.target,
                status=ShardStatus.FAILED,
                detail=f"rejected: {RejectionReason.TARGET_BUSY.value} "
                       f"target={shard.target}"))
            return
        try:
            detail = self._store.commit_shard(task_id, shard)
            status = (ShardStatus.DEDUPED if detail.startswith("dedup")
                      else ShardStatus.COMMITTED)
            self._record(task_id, ShardResult(
                shard_id=shard.shard_id, target=shard.target,
                status=status, fingerprint=shard.fingerprint, detail=detail))
        except StagingError as e:
            self._record(task_id, ShardResult(
                shard_id=shard.shard_id, target=shard.target,
                status=ShardStatus.FAILED, detail=f"{type(e).__name__}: {e}"))
        finally:
            lock.release()

    def _record(self, task_id: str, result: ShardResult) -> None:
        with self._lock:
            info = self._tasks[task_id]
            info.results.append(result)
            info.finished += 1
            if result.status == ShardStatus.FAILED and not info.error:
                info.error = result.detail
        logger.info("task=%s shard=%s -> %s (%s)", task_id, result.shard_id,
                    result.status.value, result.detail)
