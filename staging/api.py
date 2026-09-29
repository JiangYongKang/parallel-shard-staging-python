"""HTTP 路由层：把 StagingManager 暴露为 FastAPI 接口。

接口一览：
- GET  /health                    健康检查 + 当前暂存用量
- POST /tasks                     提交多分片任务（数据以 base64 内联）
- GET  /tasks/{task_id}           查询任务进度（按状态计数 + 每个分片明细）
- POST /tasks/{task_id}/cancel    取消任务
- GET  /shards/{shard_id}         查询分片持久化状态（重启后仍可查）
"""

from __future__ import annotations

import base64
import binascii
import logging

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator

from .errors import InvalidRequestError, StagingError
from .manager import StagingManager
from .models import ShardSpec

logger = logging.getLogger("staging.api")


class ShardIn(BaseModel):
    shard_id: str = Field(..., min_length=1)
    target_path: str = Field(..., min_length=1)
    digest: str = Field(..., min_length=1, description="内容指纹，形如 sha256:hex")
    data_base64: str = Field("", description="分片内容的 base64 编码（内联提交）")

    @field_validator("shard_id", "target_path", "digest")
    @classmethod
    def _non_empty(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("字段不能为空字符串")
        return value


class TaskIn(BaseModel):
    shards: list[ShardIn] = Field(..., min_length=1)


def _decode_payload(item: ShardIn) -> tuple[bytes, int]:
    try:
        raw = base64.b64decode(item.data_base64, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise InvalidRequestError(
            f"分片 {item.shard_id} 的 data_base64 不是合法 base64",
            reason="bad_base64",
            shard_id=item.shard_id,
        ) from exc
    return raw, len(raw)


def create_app(manager: StagingManager) -> FastAPI:
    """构造挂载了暂存调度接口的 FastAPI 应用。"""
    app = FastAPI(title="parallel-shard-staging", version="0.1.0")
    app.state.manager = manager

    @app.exception_handler(StagingError)
    async def _handle_staging_error(_request: Request, exc: StagingError) -> JSONResponse:
        logger.warning(
            "http rejected error_code=%s reason=%s context=%s",
            exc.error_code, exc.reason, exc.context,
        )
        return JSONResponse(status_code=exc.status_code, content=exc.to_dict())

    @app.get("/health")
    async def health() -> dict[str, object]:
        return {
            "status": "ok",
            "staging_dir": str(manager._config.staging_dir),  # noqa: SLF001
            "capacity_bytes": manager._config.capacity_bytes,  # noqa: SLF001
            "used_bytes": manager._blobs.used_bytes(),  # noqa: SLF001
            "reclaimable_bytes": manager._blobs.reclaimable_bytes(),  # noqa: SLF001
        }

    @app.post("/tasks", status_code=202)
    async def submit(task: TaskIn) -> dict[str, object]:
        specs: list[ShardSpec] = []
        for item in task.shards:
            payload, size = _decode_payload(item)
            specs.append(
                ShardSpec(
                    shard_id=item.shard_id,
                    target_path=item.target_path,
                    size=size,
                    digest=item.digest,
                    source=payload,
                )
            )
        task_id = manager.submit_task(specs)
        logger.info(
            "http accepted task_id=%s shard_count=%s", task_id, len(specs),
        )
        return {"task_id": task_id, "accepted": len(specs)}

    @app.get("/tasks/{task_id}")
    async def get_task(task_id: str) -> dict[str, object]:
        return manager.get_task(task_id).to_dict()

    @app.post("/tasks/{task_id}/cancel")
    async def cancel_task(task_id: str) -> dict[str, object]:
        record = manager.cancel_task(task_id)
        return {"task_id": task_id, "state": record.state.value}

    @app.get("/shards/{shard_id}")
    async def get_shard(shard_id: str) -> dict[str, object]:
        record = manager.find_shard(shard_id)
        if record is None:
            from .errors import ShardNotFoundError

            raise ShardNotFoundError(
                f"分片不存在: {shard_id}", reason="shard_not_found", shard_id=shard_id
            )
        return record.to_dict()

    return app
