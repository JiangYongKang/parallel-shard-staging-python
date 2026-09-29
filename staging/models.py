"""类型定义：分片、任务、状态与错误。"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Callable, Optional


class ShardStatus(str, enum.Enum):
    PENDING = "pending"
    WRITING = "writing"
    COMMITTED = "committed"
    DEDUPED = "deduped"      # 指纹命中已有数据，未占用新空间
    FAILED = "failed"
    CANCELLED = "cancelled"


class TaskStatus(str, enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class RejectionReason(str, enum.Enum):
    CONCURRENCY_LIMIT = "concurrency_limit"   # 并发压力过大
    CAPACITY_EXCEEDED = "capacity_exceeded"   # 暂存空间不足
    TARGET_BUSY = "target_busy"               # 目标位置正被其他任务写入


class StagingError(Exception):
    """暂存系统错误基类。"""


class CapacityExceededError(StagingError):
    """暂存区容量不足。"""


class ConcurrencyLimitError(StagingError):
    """并发任务数超过上限。"""


class TargetBusyError(StagingError):
    """同一目标位置已被其他任务占用。"""


class ChecksumMismatchError(StagingError):
    """内容指纹校验失败。"""


class MetadataCorruptedError(StagingError):
    """持久化元数据损坏。"""


class TaskNotFoundError(StagingError):
    """任务不存在。"""


class TaskCancelledError(StagingError):
    """任务已被取消。"""


# 数据源：分次产出字节块的假数据/真实数据提供者
DataSource = Callable[[], bytes]


@dataclass
class ShardRequest:
    shard_id: str
    target: str                    # 目标位置（落盘后的逻辑位置）
    fingerprint: str               # 内容指纹（sha256 十六进制）
    size: int                      # 声明的字节数
    read_chunk: DataSource         # 每次调用返回下一块数据，空 bytes 表示结束


@dataclass
class ShardResult:
    shard_id: str
    target: str
    status: ShardStatus
    fingerprint: str = ""
    detail: str = ""               # 判定依据（去重命中 / 校验失败原因等）


@dataclass
class TaskInfo:
    task_id: str
    status: TaskStatus = TaskStatus.PENDING
    total: int = 0
    finished: int = 0
    rejection: Optional[RejectionReason] = None
    error: str = ""
    results: list[ShardResult] = field(default_factory=list)
