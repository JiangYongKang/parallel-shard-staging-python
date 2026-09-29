"""暂存与落盘调度核心。

职责：
- 批量任务的提交、并发执行、进度查询与取消；
- 同一目标位置的互斥（TargetLockRegistry）；
- 提交时三道准入闸：目标冲突 409 / 容量不足 507 / 并发超限 503；
- 按内容指纹去重（BlobStore 内容寻址）与按分片标识幂等（manifest 索引）；
- 重启恢复：扫描 manifest 重建已完成分片索引、引用计数与中断续跑；
- 失败/取消回滚：半成品 blob 立即解除引用，随后可被 LRU 淘汰。

引用（pin）语义：
- STAGING/STAGED/FLUSHING 中的分片持有 blob 引用，不可淘汰；
- COMPLETED 且已落盘（flushed=True）后，blob 仅是暂存缓存副本，释放引用，
  可被 LRU 淘汰；但分片状态仍记录在 manifest 中，重启后仍可幂等命中。
"""

from __future__ import annotations

import io
import logging
import os
import tempfile
import threading
import uuid
from collections.abc import Iterable
from pathlib import Path

from .blobstore import BlobStore
from .config import StagingConfig
from .digest import normalize_digest
from .errors import (
    CapacityExceededError,
    ConcurrencyLimitError,
    InvalidRequestError,
    TargetLockedError,
    TaskNotFoundError,
)
from .locks import TargetLockRegistry
from .manifest import ManifestStore
from .models import (
    SHARD_TERMINAL_STATES,
    ShardRecord,
    ShardSpec,
    ShardState,
    TaskRecord,
    TaskState,
)

logger = logging.getLogger("staging.manager")

_FLUSH_CHUNK = 64 * 1024


class StagingManager:
    """对外暴露的调度门面。"""

    def __init__(
        self,
        config: StagingConfig,
        blob_store: BlobStore | None = None,
        manifest_store: ManifestStore | None = None,
        lock_registry: TargetLockRegistry | None = None,
    ) -> None:
        self._config = config
        self._blobs = blob_store or BlobStore(config)
        self._manifests = manifest_store or ManifestStore(config)
        self._target_locks = lock_registry or TargetLockRegistry()
        self._slots = threading.BoundedSemaphore(config.max_concurrent_shards)
        self._state_lock = threading.RLock()
        self._tasks: dict[str, TaskRecord] = {}
        self._known_shards: dict[str, ShardRecord] = {}
        self._cancel_events: dict[str, threading.Event] = {}
        self._started = False

    # ---- 生命周期 -------------------------------------------------------

    def start(self) -> None:
        """初始化目录并从磁盘恢复。"""
        if self._started:
            return
        self._config.staging_dir.mkdir(parents=True, exist_ok=True)
        self._blobs.ensure_dirs()
        self._manifests.ensure_dirs()
        self._blobs.scan_blobs()
        self.recover()
        self._started = True
        logger.info(
            "staging manager started dir=%s capacity=%s concurrency=%s known_shards=%s",
            self._config.staging_dir,
            self._config.capacity_bytes,
            self._config.max_concurrent_shards,
            len(self._known_shards),
        )

    def shutdown(self) -> None:
        self._started = False

    def recover(self) -> None:
        """扫描 manifest 重建索引、引用计数；标记可续跑的中断分片。"""
        with self._state_lock:
            records = self._manifests.load_all()
            self._known_shards.clear()
            for record in records:
                self._known_shards[record.shard_id] = record
                if record.state is ShardState.COMPLETED and record.flushed:
                    # 数据已在最终位置，blob 仅为缓存副本，保持无引用可淘汰
                    continue
                if record.state is ShardState.COMPLETED and not record.flushed:
                    # 暂存即终态（flush_on_commit=False）：blob 必须继续受引用保护
                    if record.blob_digest and self._blobs.exists(record.blob_digest):
                        self._blobs.pin(record.blob_digest)
                    continue
                if record.state in (ShardState.STAGED, ShardState.FLUSHING) and record.blob_digest:
                    # 崩溃发生在落盘前/中：blob 仍需保留，重新 pin 住等待续跑
                    if self._blobs.exists(record.blob_digest):
                        self._blobs.pin(record.blob_digest)
                        record.state = ShardState.STAGED
                        record.flushed = False
                        self._manifests.save(record)
                        logger.info(
                            "shard recovered to STAGED shard_id=%s task_id=%s digest=%s",
                            record.shard_id,
                            record.task_id,
                            record.digest[:25],
                        )
                # STAGING/FAILED/CANCELED 等状态：无有效 blob 引用，重新提交时全量重跑
            logger.info("recovery complete shards=%s", len(records))

    # ---- 查询 -----------------------------------------------------------

    def get_task(self, task_id: str) -> TaskRecord:
        with self._state_lock:
            task = self._tasks.get(task_id)
            if task is None:
                raise TaskNotFoundError(
                    f"任务不存在: {task_id}", reason="task_not_found", task_id=task_id
                )
            return task

    def find_shard(self, shard_id: str) -> ShardRecord | None:
        """按分片标识查最近的持久化记录（重启后仍可查）。"""
        with self._state_lock:
            return self._known_shards.get(shard_id)

    def list_recovered_shards(self) -> list[ShardRecord]:
        with self._state_lock:
            return list(self._known_shards.values())

    # ---- 提交 -----------------------------------------------------------

    def submit_task(self, specs: Iterable[ShardSpec]) -> str:
        """提交批量分片任务，立即返回 task_id，分片并发执行。"""
        spec_list = list(specs)
        if not spec_list:
            raise InvalidRequestError("分片列表不能为空", reason="empty_batch")
        if len(spec_list) > self._config.max_shards_per_task:
            raise InvalidRequestError(
                f"单任务分片数 {len(spec_list)} 超过上限 {self._config.max_shards_per_task}",
                reason="too_many_shards",
                count=len(spec_list),
                limit=self._config.max_shards_per_task,
            )

        # 1) 基础校验与批内唯一性
        seen_ids: set[str] = set()
        seen_targets: set[str] = set()
        normalized: list[ShardSpec] = []
        for spec in spec_list:
            spec.validate()
            spec.digest = normalize_digest(spec.digest)
            if spec.shard_id in seen_ids:
                raise InvalidRequestError(
                    f"同批提交中分片标识重复: {spec.shard_id}",
                    reason="duplicate_shard_id_in_batch",
                    shard_id=spec.shard_id,
                )
            target_key = TargetLockRegistry.normalize(spec.target_path)
            if target_key in seen_targets:
                raise InvalidRequestError(
                    f"同批提交中目标位置重复: {spec.target_path}",
                    reason="duplicate_target_in_batch",
                    target_path=spec.target_path,
                )
            seen_ids.add(spec.shard_id)
            seen_targets.add(target_key)
            normalized.append(spec)

        # 2) 幂等/续跑判定，同时把要真正干活的分片挑出来
        work: list[tuple[ShardSpec, str, str]] = []  # (spec, mode, blob_digest)
        reused: list[tuple[ShardSpec, ShardRecord]] = []
        with self._state_lock:
            for spec in normalized:
                prior = self._known_shards.get(spec.shard_id)
                if prior is not None:
                    same = (
                        prior.digest == spec.digest
                        and prior.target_path == spec.target_path
                        and prior.size == spec.size
                    )
                    if not same:
                        raise InvalidRequestError(
                            f"分片 {spec.shard_id} 已存在但提交参数与历史记录不一致，拒绝覆盖",
                            reason="idempotent_conflict",
                            shard_id=spec.shard_id,
                            prior_digest=prior.digest,
                            submitted_digest=spec.digest,
                            prior_target=prior.target_path,
                            submitted_target=spec.target_path,
                        )
                    if prior.state is ShardState.COMPLETED:
                        reused.append((spec, prior))
                        continue
                    if prior.state is ShardState.STAGED and prior.blob_digest:
                        work.append((spec, "flush_only", prior.blob_digest))
                        continue
                work.append((spec, "full", ""))

        task_id = uuid.uuid4().hex
        cancel_event = threading.Event()
        with self._state_lock:
            task = TaskRecord(task_id=task_id, state=TaskState.RUNNING)
            self._tasks[task_id] = task
            self._cancel_events[task_id] = cancel_event
            now = task.created_at = task.updated_at = __import__("time").time()
            for spec, prior in reused:
                record = ShardRecord(
                    shard_id=spec.shard_id,
                    task_id=task_id,
                    target_path=spec.target_path,
                    size=spec.size,
                    digest=spec.digest,
                    state=ShardState.COMPLETED,
                    blob_digest=prior.blob_digest,
                    flushed=True,
                    created_at=now,
                    updated_at=now,
                )
                task.shards[spec.shard_id] = record
                logger.info(
                    "submit decision=idempotent_hit task_id=%s shard_id=%s digest=%s",
                    task_id, spec.shard_id, spec.digest[:25],
                )
            for spec, mode, _blob in work:
                task.shards[spec.shard_id] = ShardRecord(
                    shard_id=spec.shard_id,
                    task_id=task_id,
                    target_path=spec.target_path,
                    size=spec.size,
                    digest=spec.digest,
                    state=ShardState.PENDING,
                    created_at=now,
                    updated_at=now,
                )
                logger.info(
                    "submit decision=%s task_id=%s shard_id=%s target=%s bytes=%s",
                    mode, task_id, spec.shard_id, spec.target_path, spec.size,
                )

        if not work:
            task.state = TaskState.COMPLETED
            logger.info("task completed decision=all_idempotent task_id=%s", task_id)
            return task_id

        # 3) 目标锁：任一目标被占用，整批拒绝（409，原因可解释）
        acquired_locks: list[str] = []
        try:
            for spec, _mode, _blob in sorted(work, key=lambda item: TargetLockRegistry.normalize(item[0].target_path)):
                key = TargetLockRegistry.normalize(spec.target_path)
                if not self._target_locks.try_acquire(spec.target_path, holder=task_id):
                    holder = self._target_locks.holder_of(spec.target_path)
                    raise TargetLockedError(
                        f"目标位置 {spec.target_path} 正被任务 {holder} 写入",
                        reason="target_busy",
                        target_path=spec.target_path,
                        held_by=holder,
                        task_id=task_id,
                    )
                acquired_locks.append(key)

            # 4) 容量准入预估：已占用 + 新需求 - 可回收，仍超容量则整批拒绝（507）
            total_new = sum(spec.size for spec, mode, _blob in work if mode == "full")
            if total_new:
                used = self._blobs.used_bytes()
                # 淘汰策略被显式关闭时，任何 blob 都不能算可回收
                reclaimable = (
                    self._blobs.reclaimable_bytes() if self._config.eviction_enabled else 0
                )
                if used - reclaimable + total_new > self._config.capacity_bytes:
                    raise CapacityExceededError(
                        "预计暂存空间不足，且可淘汰数据不够，拒绝整批提交",
                        reason="admission_capacity",
                        requested=total_new,
                        used=used,
                        reclaimable=reclaimable,
                        capacity=self._config.capacity_bytes,
                        task_id=task_id,
                    )

            # 5) 并发准入：系统已饱和（工作槽全部被占）才拒绝，否则任务进入排队。
            #    这里只做一次“非阻塞探测”，不实际预借槽——槽由 worker 自行借还，
            #    否则会和 worker 的借用重复计数。批次大于并发度时分片在内部排队。
            if not self._slots.acquire(blocking=False):
                raise ConcurrencyLimitError(
                    f"并发分片数已达上限 {self._config.max_concurrent_shards}",
                    reason="concurrency_slots_busy",
                    limit=self._config.max_concurrent_shards,
                    task_id=task_id,
                )
            self._slots.release()
        except BaseException:
            for key in acquired_locks:
                self._target_locks.release(key)
            with self._state_lock:
                task.state = TaskState.FAILED
                task.error = "admission_rejected"
            raise

        # 全部准入通过：每个分片一个线程，执行时自行排队获取工作槽
        threads: list[threading.Thread] = []
        for spec, mode, blob_digest in work:
            thread = threading.Thread(
                target=self._run_shard,
                args=(task_id, spec, mode, blob_digest, cancel_event),
                name=f"shard-{task_id[:8]}-{spec.shard_id}",
                daemon=True,
            )
            threads.append(thread)

        def _release_all() -> None:
            for key in acquired_locks:
                self._target_locks.release(key)
        supervisor = threading.Thread(
            target=self._supervise, args=(task_id, threads, _release_all), daemon=True
        )
        for thread in threads:
            thread.start()
        supervisor.start()
        return task_id

    # ---- 取消 -----------------------------------------------------------

    def cancel_task(self, task_id: str) -> TaskRecord:
        with self._state_lock:
            task = self._tasks.get(task_id)
            if task is None:
                raise TaskNotFoundError(
                    f"任务不存在: {task_id}", reason="task_not_found", task_id=task_id
                )
            if task.state in (TaskState.COMPLETED, TaskState.FAILED):
                return task
            event = self._cancel_events.get(task_id)
            if event is not None:
                event.set()
            task.state = TaskState.CANCELING
        logger.info("cancel requested task_id=%s", task_id)
        return task

    # ---- 分片执行 -------------------------------------------------------

    def _supervise(self, task_id: str, threads: list[threading.Thread], cleanup) -> None:
        for thread in threads:
            thread.join()
        cleanup()
        with self._state_lock:
            task = self._tasks[task_id]
            states = [shard.state for shard in task.shards.values()]
            if all(state is ShardState.COMPLETED for state in states):
                task.state = TaskState.COMPLETED
            elif any(state is ShardState.CANCELED for state in states):
                task.state = TaskState.CANCELED
            elif any(state is ShardState.FAILED for state in states):
                task.state = TaskState.FAILED
            else:  # pragma: no cover - 兜底
                task.state = TaskState.FAILED
            task.touch()
            logger.info(
                "task finished task_id=%s state=%s progress=%s",
                task_id, task.state.value, task.progress(),
            )

    def _run_shard(
        self,
        task_id: str,
        spec: ShardSpec,
        mode: str,
        blob_digest: str,
        cancel_event: threading.Event,
    ) -> None:
        def mark(state: ShardState, error: str = "", blob_name: str = "", flushed: bool | None = None) -> None:
            with self._state_lock:
                record = self._tasks[task_id].shards[spec.shard_id]
                record.state = state
                if error:
                    record.error = error
                if blob_name:
                    record.blob_digest = blob_name
                if flushed is not None:
                    record.flushed = flushed
                record.touch()
                self._known_shards[spec.shard_id] = record
                snapshot = record
            self._manifests.save(snapshot)
            logger.info(
                "shard state task_id=%s shard_id=%s state=%s reason=%s",
                task_id, spec.shard_id, state.value, error or "progress",
            )

        if cancel_event.is_set():
            mark(ShardState.CANCELED, error="canceled_before_start")
            return

        self._slots.acquire()  # 排队获取工作槽（准入已保证系统有处理能力）
        if cancel_event.is_set():
            self._slots.release()
            mark(ShardState.CANCELED, error="canceled_while_queued")
            return
        try:
            if mode == "flush_only":
                blob_name = blob_digest
                logger.info(
                    "resume flush task_id=%s shard_id=%s blob=%s",
                    task_id, spec.shard_id, blob_name,
                )
                self._blobs.verify_blob(blob_name, spec.digest, spec.size)
                if cancel_event.is_set():
                    mark(ShardState.CANCELED, error="canceled_before_flush", blob_name=blob_name)
                    return
                mark(ShardState.FLUSHING, blob_name=blob_name)
                self._flush_blob(blob_name, spec, task_id)
                self._blobs.unpin(blob_name)
                mark(ShardState.COMPLETED, blob_name=blob_name, flushed=True)
                return

            # ---- 全量流程：暂存 -> 校验 -> 落盘 ----
            mark(ShardState.STAGING)
            source = spec.source
            if isinstance(source, (bytes, bytearray)):
                stream: object = io.BytesIO(bytes(source))
            elif source is None:
                raise InvalidRequestError(
                    f"分片 {spec.shard_id} 缺少数据源", reason="missing_source", shard_id=spec.shard_id
                )
            else:
                stream = source
            blob_name, size = self._blobs.stage_stream(
                stream,  # type: ignore[arg-type]
                expected_digest=spec.digest,
                expected_size=spec.size,
                pin=True,
            )
            logger.info(
                "staged task_id=%s shard_id=%s blob=%s bytes=%s",
                task_id, spec.shard_id, blob_name, size,
            )
            if cancel_event.is_set():
                self._blobs.unpin(blob_name)
                mark(ShardState.CANCELED, error="canceled_after_stage", blob_name=blob_name)
                return
            mark(ShardState.STAGED, blob_name=blob_name)
            if self._config.flush_on_commit:
                mark(ShardState.FLUSHING)
                self._flush_blob(blob_name, spec, task_id)
                self._blobs.unpin(blob_name)
                mark(ShardState.COMPLETED, blob_name=blob_name, flushed=True)
            else:
                # 不落盘：blob 必须继续被引用保护
                mark(ShardState.COMPLETED, blob_name=blob_name, flushed=False)
        except Exception as exc:  # noqa: BLE001 - 分片级错误统一落账
            code = getattr(exc, "error_code", type(exc).__name__)
            reason = getattr(exc, "reason", code)
            logger.warning(
                "shard failed task_id=%s shard_id=%s error_code=%s reason=%s message=%s",
                task_id, spec.shard_id, code, reason, exc,
            )
            with self._state_lock:
                rec = self._tasks[task_id].shards[spec.shard_id]
                if rec.blob_digest and self._blobs.ref_count(rec.blob_digest) > 0:
                    # 暂存成功但后续失败：解除引用，让 blob 可被淘汰（回滚干净）
                    try:
                        self._blobs.unpin(rec.blob_digest)
                    except RuntimeError:
                        pass
                rec.state = ShardState.FAILED
                rec.error = f"{code}[{reason}]: {exc}"
                rec.touch()
                self._known_shards[spec.shard_id] = rec
                failed_record = rec
            self._manifests.save(failed_record)
        finally:
            self._slots.release()

    def _flush_blob(self, blob_name: str, spec: ShardSpec, task_id: str) -> None:
        """把已校验的 blob 原子拷贝到最终目标位置。"""
        target = self._resolve_target(spec.target_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(prefix=".flush-", suffix=".tmp", dir=target.parent)
        tmp_path = Path(tmp_name)
        try:
            with self._blobs.open_blob(blob_name) as src, os.fdopen(fd, "wb") as dst:
                while True:
                    chunk = src.read(_FLUSH_CHUNK)
                    if not chunk:
                        break
                    dst.write(chunk)
                dst.flush()
                os.fsync(dst.fileno())
            os.replace(tmp_path, target)
        except BaseException:
            tmp_path.unlink(missing_ok=True)
            raise
        logger.info(
            "flushed task_id=%s shard_id=%s target=%s bytes=%s",
            task_id, spec.shard_id, target, spec.size,
        )

    def _resolve_target(self, target_path: str) -> Path:
        if self._config.output_dir is not None and not os.path.isabs(target_path):
            return (self._config.output_dir / target_path).resolve()
        return Path(target_path)
