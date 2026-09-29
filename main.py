"""服务入口：从环境变量构造配置、调度管理器与 HTTP 应用。

环境变量见 README“配置项”一节。
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import uvicorn
from fastapi import FastAPI

from staging.api import create_app
from staging.config import StagingConfig
from staging.manager import StagingManager


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def load_config() -> StagingConfig:
    staging_dir = Path(
        os.environ.get("STAGING_DIR", str(Path(tempfile.gettempdir()) / "shard-staging"))
    )
    output_raw = os.environ.get("OUTPUT_DIR")
    return StagingConfig(
        staging_dir=staging_dir,
        output_dir=Path(output_raw) if output_raw else None,
        capacity_bytes=int(os.environ.get("STAGING_CAPACITY_BYTES", str(64 * 1024 * 1024))),
        max_concurrent_shards=int(os.environ.get("STAGING_MAX_CONCURRENCY", "4")),
        max_shards_per_task=int(os.environ.get("STAGING_MAX_SHARDS_PER_TASK", "256")),
        digest_algorithm=os.environ.get("STAGING_DIGEST_ALGORITHM", "sha256"),
        eviction_enabled=_env_bool("STAGING_EVICTION_ENABLED", True),
        flush_on_commit=_env_bool("STAGING_FLUSH_ON_COMMIT", True),
        request_timeout_seconds=float(os.environ.get("STAGING_REQUEST_TIMEOUT_SECONDS", "30")),
    )


def build_app(config: StagingConfig | None = None) -> FastAPI:
    config = config or load_config()
    manager = StagingManager(config)
    manager.start()
    return create_app(manager)


app = build_app()


if __name__ == "__main__":
    uvicorn.run(app, host=os.environ.get("HOST", "127.0.0.1"), port=int(os.environ.get("PORT", "8000")))
