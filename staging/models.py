"""核心数据模型与状态定义。"""

from __future__ import annotations

import enum
import time
from dataclasses import dataclass, field


class ShardState(str, enum.Enum):
    """分片生命周期状态。

    状态迁移见 README“状态机”一节：
        PENDING -> STAGING -> STAGED -> FLUSHING -> COMPLETED
                    |-> FAILED / CANCELED      |-> FAILED
    """

    PENDING = "pending"
    STAGING = "staging"
    STAGED = "staged"
    FLUSHING = "flushing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"


class TaskState(str, enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    CANCELING = "canceling"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"


# 终态：处于这些状态的分片状态不再变化
SHARD_TERMINAL_STATES = frozenset({ShardState.COMPLETED, ShardState.FAILED, ShardState.CANCELED})
TASK_TERMINAL_STATES = frozenset({TaskState.COMPLETED, TaskState.FAILED, TaskState.CANCELED})


@dataclass
class ShardSpec:
    """一次提交里的单个分片请求规格。"""

    shard_id: str
    target_path: str
    size: int
    digest: str  # 客户端声明的内容指纹，形如 "sha256:...."
    source: object = None  # 数据源：bytes 或可迭代字节块 / 类文件对象

    def validate(self) -> None:
        """基础合法性校验，不触碰 IO。"""
        if not self.shard_id or not isinstance(self.shard_id, str):
            raise ValueError("shard_id 不能为空")
        if not self.target_path or not isinstance(self.target_path, str):
            raise ValueError("target_path 不能为空")
        if not isinstance(self.size, int) or self.size < 0:
            raise ValueError("size 必须为非负整数")
        if not self.digest or not isinstance(self.digest, str):
            raise ValueError("digest 不能为空")


@dataclass
class ShardRecord:
    """单个分片的持久化元数据（manifest）。"""

    shard_id: str
    task_id: str
    target_path: str
    size: int
    digest: str  # 校验通过后服务端确认的内容指纹
    state: ShardState = ShardState.PENDING
    blob_digest: str = ""  # 内容寻址 blob 名（hex）
    flushed: bool = False
    error: str = ""
    created_at: float = 0.0
    updated_at: float = 0.0

    def touch(self) -> None:
        self.updated_at = time.time()

    def to_dict(self) -> dict[str, object]:
        return {
            "shard_id": self.shard_id,
            "task_id": self.task_id,
            "target_path": self.target_path,
            "size": self.size,
            "digest": self.digest,
            "state": self.state.value,
            "blob_digest": self.blob_digest,
            "flushed": self.flushed,
            "error": self.error,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> "ShardRecord":
        required = (
            "shard_id",
            "task_id",
            "target_path",
            "size",
            "digest",
            "state",
            "blob_digest",
            "flushed",
            "error",
            "created_at",
            "updated_at",
        )
        missing = [key for key in required if key not in data]
        if missing:
            raise ValueError(f"manifest 缺少必需字段: {missing}")
        try:
            state = ShardState(str(data["state"]))
        except ValueError as exc:
            raise ValueError(f"未知分片状态: {data['state']!r}") from exc
        try:
            size = int(data["size"])  # type: ignore[arg-type]
            created_at = float(data["created_at"])  # type: ignore[arg-type]
            updated_at = float(data["updated_at"])  # type: ignore[arg-type]
        except (TypeError, ValueError) as exc:
            raise ValueError(f"manifest 数值字段类型损坏: {exc}") from exc
        str_fields = ("shard_id", "task_id", "target_path", "digest", "blob_digest", "error")
        for key in str_fields:
            if not isinstance(data[key], str):
                raise ValueError(f"manifest 字段 {key} 不是字符串")
        if not isinstance(data["flushed"], bool):
            raise ValueError("manifest 字段 flushed 不是布尔值")
        return cls(
            shard_id=data["shard_id"],  # type: ignore[arg-type]
            task_id=data["task_id"],  # type: ignore[arg-type]
            target_path=data["target_path"],  # type: ignore[arg-type]
            size=size,
            digest=data["digest"],  # type: ignore[arg-type]
            state=state,
            blob_digest=data["blob_digest"],  # type: ignore[arg-type]
            flushed=bool(data["flushed"]),
            error=data["error"],  # type: ignore[arg-type]
            created_at=created_at,
            updated_at=updated_at,
        )


@dataclass
class TaskRecord:
    """一次批量提交任务的运行状态（仅保存在内存中）。"""

    task_id: str
    state: TaskState = TaskState.PENDING
    shards: dict[str, ShardRecord] = field(default_factory=dict)
    created_at: float = 0.0
    updated_at: float = 0.0
    error: str = ""

    def touch(self) -> None:
        self.updated_at = time.time()

    def progress(self) -> dict[str, int]:
        """按状态统计分片数量，用于进度查询。"""
        counts: dict[str, int] = {state.value: 0 for state in ShardState}
        for shard in self.shards.values():
            counts[shard.state.value] += 1
        counts["total"] = len(self.shards)
        return counts

    def to_dict(self) -> dict[str, object]:
        return {
            "task_id": self.task_id,
            "state": self.state.value,
            "shards": {sid: record.to_dict() for sid, record in self.shards.items()},
            "progress": self.progress(),
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "error": self.error,
        }
