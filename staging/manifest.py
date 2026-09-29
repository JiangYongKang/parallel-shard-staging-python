"""分片元数据（manifest）的原子持久化与重启恢复。

每个分片对应 ``shards/<shard_id>.json``，写入采用
``写临时文件 -> flush -> fsync -> os.replace`` 的原子替换方式，
保证任何时刻磁盘上要么是完整的旧版本，要么是完整的新版本，不会读到半成品。
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
from pathlib import Path

from .config import StagingConfig
from .errors import MetadataCorruptedError, ShardNotFoundError
from .models import ShardRecord

logger = logging.getLogger("staging.manifest")


class ManifestStore:
    """``shards/<shard_id>.json`` 的读写与扫描恢复。"""

    def __init__(self, config: StagingConfig) -> None:
        self._config = config
        self._shards_dir = config.staging_dir / "shards"

    def ensure_dirs(self) -> None:
        self._shards_dir.mkdir(parents=True, exist_ok=True)

    def manifest_path(self, shard_id: str) -> Path:
        return self._shards_dir / f"{shard_id}.json"

    def save(self, record: ShardRecord) -> None:
        """原子写入 manifest。"""
        record.touch()
        payload = json.dumps(record.to_dict(), ensure_ascii=False, sort_keys=True).encode("utf-8")
        final_path = self.manifest_path(record.shard_id)
        self.ensure_dirs()
        fd, tmp_name = tempfile.mkstemp(
            prefix=f".{record.shard_id}.", suffix=".tmp", dir=self._shards_dir
        )
        tmp_path = Path(tmp_name)
        try:
            with os.fdopen(fd, "wb") as fh:
                fh.write(payload)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp_path, final_path)
        except BaseException:
            tmp_path.unlink(missing_ok=True)
            raise
        logger.info(
            "manifest saved task_id=%s shard_id=%s state=%s",
            record.task_id,
            record.shard_id,
            record.state.value,
        )

    def load(self, shard_id: str) -> ShardRecord:
        path = self.manifest_path(shard_id)
        if not path.exists():
            raise ShardNotFoundError(
                f"分片元数据不存在: {shard_id}", reason="manifest_missing", shard_id=shard_id
            )
        return self._load_path(path)

    def _load_path(self, path: Path) -> ShardRecord:
        try:
            raw = path.read_bytes()
        except OSError as exc:
            raise MetadataCorruptedError(
                f"manifest 无法读取: {path.name}: {exc}",
                reason="manifest_unreadable",
                file=path.name,
            ) from exc
        try:
            data = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise MetadataCorruptedError(
                f"manifest 不是合法 JSON: {path.name}: {exc}",
                reason="manifest_invalid_json",
                file=path.name,
            ) from exc
        if not isinstance(data, dict):
            raise MetadataCorruptedError(
                f"manifest 顶层结构不是对象: {path.name}",
                reason="manifest_bad_shape",
                file=path.name,
            )
        try:
            return ShardRecord.from_dict(data)
        except ValueError as exc:
            raise MetadataCorruptedError(
                f"manifest 字段损坏: {path.name}: {exc}",
                reason="manifest_bad_fields",
                file=path.name,
            ) from exc

    def delete(self, shard_id: str) -> None:
        self.manifest_path(shard_id).unlink(missing_ok=True)

    def load_all(self) -> list[ShardRecord]:
        """扫描全部 manifest，用于重启恢复。

        任何一个文件损坏都立即抛 MetadataCorruptedError——绝不静默忽略或覆盖。
        """
        self.ensure_dirs()
        records: list[ShardRecord] = []
        for path in sorted(self._shards_dir.glob("*.json")):
            record = self._load_path(path)
            records.append(record)
            logger.info(
                "manifest recovered shard_id=%s task_id=%s state=%s digest=%s",
                record.shard_id,
                record.task_id,
                record.state.value,
                record.digest[: 18 + 7],
            )
        return records
