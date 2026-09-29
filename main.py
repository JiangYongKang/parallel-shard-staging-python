"""HTTP 服务：并行分片暂存与落盘调度。"""

from __future__ import annotations

import base64
import os

import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from staging import (
    CapacityExceededError,
    ChecksumMismatchError,
    ConcurrencyLimitError,
    MetadataCorruptedError,
    Scheduler,
    ShardRequest,
    StagingStore,
    TaskNotFoundError,
)

STAGING_DIR = os.environ.get("STAGING_DIR", "./staging-data")
MAX_BYTES = int(os.environ.get("STAGING_MAX_BYTES", str(256 << 20)))
MAX_WORKERS = int(os.environ.get("STAGING_MAX_WORKERS", "4"))
MAX_PENDING = int(os.environ.get("STAGING_MAX_PENDING", "8"))

store = StagingStore(STAGING_DIR, max_bytes=MAX_BYTES)
scheduler = Scheduler(store, max_workers=MAX_WORKERS, max_pending=MAX_PENDING)
app = FastAPI(title="parallel-shard-staging")


class ShardIn(BaseModel):
    shard_id: str
    target: str
    fingerprint: str          # sha256 十六进制
    size: int
    content_b64: str          # 分片内容（base64）


class SubmitIn(BaseModel):
    shards: list[ShardIn]


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/stats")
def stats():
    return {"used_bytes": store.used_bytes, "max_bytes": store.max_bytes}


@app.post("/tasks", status_code=201)
def submit_task(body: SubmitIn):
    def make(shard: ShardIn) -> ShardRequest:
        data = base64.b64decode(shard.content_b64)
        sent = {"done": False}

        def read_chunk() -> bytes:
            if sent["done"]:
                return b""
            sent["done"] = True
            return data

        return ShardRequest(shard.shard_id, shard.target,
                            shard.fingerprint, shard.size, read_chunk)

    try:
        task_id = scheduler.submit([make(s) for s in body.shards])
    except ConcurrencyLimitError as e:
        raise HTTPException(429, f"concurrency_limit: {e}")
    except CapacityExceededError as e:
        raise HTTPException(507, f"capacity_exceeded: {e}")
    return {"task_id": task_id}


@app.get("/tasks/{task_id}")
def get_task(task_id: str):
    try:
        info = scheduler.get(task_id)
    except TaskNotFoundError:
        raise HTTPException(404, "task not found")
    return {
        "task_id": info.task_id,
        "status": info.status.value,
        "finished": info.finished,
        "total": info.total,
        "error": info.error,
        "results": [
            {"shard_id": r.shard_id, "target": r.target,
             "status": r.status.value, "detail": r.detail}
            for r in info.results
        ],
    }


@app.post("/tasks/{task_id}/cancel")
def cancel_task(task_id: str):
    try:
        info = scheduler.cancel(task_id)
    except TaskNotFoundError:
        raise HTTPException(404, "task not found")
    return {"task_id": info.task_id, "status": info.status.value}


@app.on_event("shutdown")
def _shutdown():
    scheduler.shutdown()


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=0)
