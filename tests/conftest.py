"""pytest 公共夹具：本地临时目录、可控假数据源与日志。

不依赖任何外部服务；每个用例拿到独立的暂存根目录与落盘目录。
"""

from __future__ import annotations

import base64
import hashlib
import logging
import threading
import time
from collections.abc import Iterable
from pathlib import Path

import pytest

from staging.config import StagingConfig
from staging.digest import compute_digest
from staging.manager import StagingManager
from staging.models import ShardSpec, ShardState, TaskState


@pytest.fixture
def dirs(tmp_path: Path) -> tuple[Path, Path]:
    staging = tmp_path / "staging"
    output = tmp_path / "output"
    staging.mkdir()
    output.mkdir()
    return staging, output


def make_config(
    dirs: tuple[Path, Path],
    *,
    capacity_bytes: int = 10 * 1024,
    max_concurrent_shards: int = 4,
    flush_on_commit: bool = True,
    eviction_enabled: bool = True,
) -> StagingConfig:
    staging, output = dirs
    return StagingConfig(
        staging_dir=staging,
        output_dir=output,
        capacity_bytes=capacity_bytes,
        max_concurrent_shards=max_concurrent_shards,
        flush_on_commit=flush_on_commit,
        eviction_enabled=eviction_enabled,
    )


@pytest.fixture
def manager(dirs):
    config = make_config(dirs)
    mgr = StagingManager(config)
    mgr.start()
    yield mgr
    mgr.shutdown()


def sha256(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


def make_spec(shard_id: str, target: str, data: bytes) -> ShardSpec:
    return ShardSpec(
        shard_id=shard_id,
        target_path=target,
        size=len(data),
        digest=compute_digest([data]),
        source=data,
    )


class BlockingSource:
    """可控假数据源：先吐出 head 字节，然后阻塞在 gate 上。

    gate.set() 后继续吐完剩余数据；若 abort 被设置，则抛 RuntimeError 模拟中断。
    """

    def __init__(
        self,
        payload: bytes,
        gate: threading.Event,
        *,
        head: int = 8,
        abort: threading.Event | None = None,
        chunk_size: int = 7,
        fail_after_head: bool = False,
    ) -> None:
        self.payload = payload
        self.gate = gate
        self.head = head
        self.abort = abort
        self.chunk_size = chunk_size
        self.fail_after_head = fail_after_head
        self._head_sent = False
        self.bytes_emitted = 0

    def read(self, _n: int) -> bytes:
        if not self._head_sent:
            self._head_sent = True
            part, self.payload = self.payload[: self.head], self.payload[self.head :]
            self.bytes_emitted += len(part)
            return part
        self.gate.wait(5)
        if self.fail_after_head or (self.abort is not None and self.abort.is_set()):
            raise RuntimeError("fake source aborted by test")
        if not self.payload:
            return b""
        part, self.payload = self.payload[: self.chunk_size], self.payload[self.chunk_size :]
        self.bytes_emitted += len(part)
        return part


class FailingSource:
    """读到 fail_at 字节后必然抛错的数据源。"""

    def __init__(self, payload: bytes, fail_at: int = 4) -> None:
        self.payload = payload
        self.fail_at = fail_at
        self.emitted = 0

    def __iter__(self) -> Iterable[bytes]:
        while self.payload:
            part, self.payload = self.payload[:3], self.payload[3:]
            self.emitted += len(part)
            if self.emitted >= self.fail_at:
                raise RuntimeError("fake source exploded")
            yield part


def wait_for_states(
    mgr: StagingManager,
    task_id: str,
    states: set[TaskState],
    timeout: float = 5.0,
) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        task = mgr.get_task(task_id)
        if task.state in states:
            return task
        time.sleep(0.01)
    raise AssertionError(
        f"task {task_id} 未在 {timeout}s 内到达 {states}，当前 {mgr.get_task(task_id).state}"
    )


def wait_completed(mgr: StagingManager, task_id: str) -> dict:
    return wait_for_states(
        mgr,
        task_id,
        {TaskState.COMPLETED, TaskState.FAILED, TaskState.CANCELED},
    )


def b64(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def shard_states(task_view: dict) -> dict[str, str]:
    return {sid: shard["state"] for sid, shard in task_view["shards"].items()}


def assert_no_tmp_files(root: Path) -> None:
    leftovers = list(root.rglob("*.tmp"))
    assert leftovers == [], f"发现未清理的临时文件: {leftovers}"


# 让每个用例都能在日志中看到任务标识、分片标识与判定依据
@pytest.fixture(autouse=True)
def _scenario_logging(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO, logger="staging")
