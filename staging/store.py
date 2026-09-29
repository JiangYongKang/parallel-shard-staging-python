"""暂存区：容量上限、指纹去重、淘汰、完整性校验与崩溃恢复。"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import threading
import time
from pathlib import Path

from .models import (
    CapacityExceededError,
    ChecksumMismatchError,
    MetadataCorruptedError,
    ShardRequest,
)

logger = logging.getLogger("staging.store")

_CHUNK = 64 * 1024


class StagingStore:
    """基于本地目录的暂存区。

    目录组织：
        <root>/blobs/<fingerprint>      已提交的完整数据（按指纹去重）
        <root>/tmp/<task>-<shard>.part  写入中的临时文件
        <root>/index.json               元数据（指纹 -> 大小/引用计数/时间戳）
    """

    def __init__(self, root: str | Path, max_bytes: int) -> None:
        self._root = Path(root)
        self._max_bytes = max_bytes
        self._lock = threading.Lock()
        self._index: dict[str, dict] = {}
        self._used_bytes = 0
        (self._root / "blobs").mkdir(parents=True, exist_ok=True)
        (self._root / "tmp").mkdir(parents=True, exist_ok=True)
        self._load_index()
        self._cleanup_orphans()

    # ---- 查询 ----

    @property
    def used_bytes(self) -> int:
        with self._lock:
            return self._used_bytes

    @property
    def max_bytes(self) -> int:
        return self._max_bytes

    @property
    def available_bytes(self) -> int:
        """当前可用空间：剩余空间 + 可淘汰（未被引用）的 blob 总大小。"""
        with self._lock:
            evictable = sum(m["size"] for m in self._index.values()
                            if m["refs"] == 0)
            return self._max_bytes - self._used_bytes + evictable

    def has(self, fingerprint: str) -> bool:
        """指纹是否已存在已提交数据。"""
        with self._lock:
            return fingerprint in self._index

    # ---- 写入与提交 ----

    def commit_shard(self, task_id: str, shard: ShardRequest) -> str:
        """写入分片：先写 .part 并边写边算指纹，校验通过后原子改名提交。

        返回判定说明（提交 / 去重命中）。失败时清理半成品。
        """
        # 去重：指纹已存在则不占新空间
        with self._lock:
            if shard.fingerprint in self._index:
                detail = (f"dedup hit: fingerprint={shard.fingerprint} "
                          f"already committed, no extra space")
                logger.info("task=%s shard=%s %s", task_id, shard.shard_id, detail)
                return detail

        # 容量预占（写之前先确认放得下）
        with self._lock:
            self._evict_if_needed(shard.size)
            if self._used_bytes + shard.size > self._max_bytes:
                raise CapacityExceededError(
                    f"need {shard.size}B, used {self._used_bytes}B, "
                    f"limit {self._max_bytes}B, no evictable blobs")

        part = self._root / "tmp" / f"{task_id}-{shard.shard_id}.part"
        hasher = hashlib.sha256()
        written = 0
        try:
            with open(part, "wb") as f:
                while True:
                    chunk = shard.read_chunk()
                    if not chunk:
                        break
                    hasher.update(chunk)
                    f.write(chunk)
                    written += len(chunk)
                f.flush()
                os.fsync(f.fileno())
        except Exception:
            part.unlink(missing_ok=True)
            logger.warning("task=%s shard=%s write error, rolled back %s",
                           task_id, shard.shard_id, part)
            raise

        actual = hasher.hexdigest()
        if written != shard.size or actual != shard.fingerprint:
            part.unlink(missing_ok=True)
            raise ChecksumMismatchError(
                f"shard={shard.shard_id} declared fp={shard.fingerprint} "
                f"size={shard.size}, actual fp={actual} size={written}; "
                f"temp file removed, old data untouched")

        blob = self._root / "blobs" / shard.fingerprint
        with self._lock:
            # 并发下可能另一线程刚提交了同一指纹
            if shard.fingerprint in self._index:
                part.unlink(missing_ok=True)
                detail = f"dedup hit after write: fingerprint={shard.fingerprint}"
                logger.info("task=%s shard=%s %s", task_id, shard.shard_id, detail)
                return detail
            os.replace(part, blob)  # 原子提交
            self._index[shard.fingerprint] = {
                "size": written,
                "refs": 0,
                "last_used": time.time(),
            }
            self._used_bytes += written
            self._save_index()
        detail = f"committed: fingerprint={shard.fingerprint} size={written}"
        logger.info("task=%s shard=%s %s", task_id, shard.shard_id, detail)
        return detail

    def add_ref(self, fingerprint: str) -> None:
        """目标位置引用该数据，引用期间不可被淘汰。"""
        with self._lock:
            entry = self._index[fingerprint]
            entry["refs"] += 1
            entry["last_used"] = time.time()
            self._save_index()

    def release_ref(self, fingerprint: str) -> None:
        """释放引用。"""
        with self._lock:
            entry = self._index[fingerprint]
            entry["refs"] = max(0, entry["refs"] - 1)
            self._save_index()

    # ---- 内部 ----

    def _evict_if_needed(self, incoming: int) -> None:
        """LRU 淘汰未被引用的 blob，直到放得下 incoming 字节。"""
        while self._used_bytes + incoming > self._max_bytes:
            candidates = [
                (meta["last_used"], fp, meta["size"])
                for fp, meta in self._index.items()
                if meta["refs"] == 0
            ]
            if not candidates:
                return  # 没有可淘汰的，让调用方报容量错误
            _, fp, size = min(candidates)
            (self._root / "blobs" / fp).unlink(missing_ok=True)
            del self._index[fp]
            self._used_bytes -= size
            logger.info("evicted blob fp=%s size=%d (LRU, refs=0)", fp, size)
        self._save_index()

    def _save_index(self) -> None:
        tmp = self._root / "index.json.tmp"
        tmp.write_text(json.dumps(self._index, indent=1))
        os.replace(tmp, self._root / "index.json")

    def _load_index(self) -> None:
        path = self._root / "index.json"
        if not path.exists():
            return
        try:
            data = json.loads(path.read_text())
            if not isinstance(data, dict):
                raise ValueError("index root is not a dict")
            for fp, meta in data.items():
                if not isinstance(meta.get("size"), int) or "refs" not in meta:
                    raise ValueError(f"bad entry for {fp}")
                blob = self._root / "blobs" / fp
                if not blob.exists() or blob.stat().st_size != meta["size"]:
                    raise ValueError(f"blob missing or size mismatch for {fp}")
        except (ValueError, KeyError, json.JSONDecodeError) as e:
            raise MetadataCorruptedError(
                f"index.json corrupted: {e}; refusing to overwrite old data") from e
        self._index = data
        self._used_bytes = sum(m["size"] for m in data.values())
        logger.info("recovered %d blobs, used=%d bytes", len(data), self._used_bytes)

    def _cleanup_orphans(self) -> None:
        """清理崩溃残留的 .part 半成品，绝不让其被读到。"""
        for part in (self._root / "tmp").glob("*.part"):
            part.unlink()
            logger.info("removed orphan temp file %s", part.name)
