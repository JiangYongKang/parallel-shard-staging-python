"""可解释的错误类型，与 HTTP 状态码一一对应。"""

from __future__ import annotations


class StagingError(Exception):
    """所有暂存错误的基类。"""

    status_code: int = 500
    error_code: str = "staging_error"

    def __init__(self, message: str, *, reason: str = "", **context: object) -> None:
        super().__init__(message)
        self.message = message
        self.reason = reason or self.error_code
        self.context = context

    def to_dict(self) -> dict[str, object]:
        return {
            "error": self.error_code,
            "reason": self.reason,
            "message": self.message,
            "context": self.context,
        }


class TargetLockedError(StagingError):
    """同一目标位置正被其他任务写入。"""

    status_code = 409
    error_code = "target_locked"


class ConcurrencyLimitError(StagingError):
    """并发压力超过上限，请求被拒绝。"""

    status_code = 503
    error_code = "concurrency_limit"


class CapacityExceededError(StagingError):
    """暂存空间不足且无法淘汰，请求被拒绝。"""

    status_code = 507
    error_code = "capacity_exceeded"


class ChecksumMismatchError(StagingError):
    """客户端声明指纹与实际内容指纹不一致。"""

    status_code = 422
    error_code = "checksum_mismatch"


class MetadataCorruptedError(StagingError):
    """磁盘上的分片元数据损坏。"""

    status_code = 409
    error_code = "metadata_corrupted"


class TaskNotFoundError(StagingError):
    status_code = 404
    error_code = "task_not_found"


class ShardNotFoundError(StagingError):
    status_code = 404
    error_code = "shard_not_found"


class InvalidRequestError(StagingError):
    status_code = 400
    error_code = "invalid_request"


class FlushError(StagingError):
    """从暂存区落盘到最终目标失败。"""

    status_code = 500
    error_code = "flush_failed"
