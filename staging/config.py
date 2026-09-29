"""暂存调度相关配置项。"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class StagingConfig:
    """暂存区配置。

    具体配置项含义见 README“配置项”一节。
    """

    staging_dir: Path
    output_dir: Path | None = None  # 落盘根目录；None 时 target_path 视为绝对路径
    capacity_bytes: int = 64 * 1024 * 1024
    max_concurrent_shards: int = 4
    max_shards_per_task: int = 256
    digest_algorithm: str = "sha256"
    eviction_enabled: bool = True
    flush_on_commit: bool = True
    request_timeout_seconds: float = 30.0
