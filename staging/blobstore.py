"""内容寻址的暂存 blob 存储。

目录布局：
    blobs/<digest-hex>          已校验通过的不可变内容块
    blobs/.staging-XXXX.tmp     写入中的临时块（失败即删除，绝不允许被读到）

关键保证：
1. 先预留容量（含在途字节），预留失败先做 LRU 淘汰，仍不足才明确拒绝；
2. 引用计数 > 0 的 blob 永远不会被淘汰；
3. 流式写入时同步计算指纹与长度，校验不过立即删除临时文件并回滚预留；
4. 内容按指纹命名，相同内容物理上只存一份，天然去重。
"""

from __future__ import annotations

import logging
import os
import tempfile
import threading
from collections import OrderedDict
from collections.abc import Iterable
from pathlib import Path
from typing import IO

from .config import StagingConfig
from .digest import compute_digest, digest_algorithm, digest_to_blob_name, normalize_digest
from .errors import CapacityExceededError, ChecksumMismatchError

logger = logging.getLogger("staging.blobstore")

_READ_CHUNK = 64 * 1024


def _iter_chunks(stream: Iterable[bytes] | IO[bytes]) -> Iterable[bytes]:
    """统一字节迭代协议：类文件对象按块读，可迭代对象逐块产出。"""
    read = getattr(stream, "read", None)
    if callable(read):
        while True:
            chunk = read(_READ_CHUNK)
            if not chunk:
                break
            yield chunk
    else:
        yield from stream  # type: ignore[misc]


class BlobStore:
    """暂存区 ``blobs/`` 目录的管理、引用计数与 LRU 淘汰。"""

    def __init__(self, config: StagingConfig) -> None:
        self._config = config
        self._blobs_dir = config.staging_dir / "blobs"
        self._lock = threading.RLock()
        self._sizes: dict[str, int] = {}          # blob 名 -> 字节数
        self._refs: dict[str, int] = {}           # blob 名 -> 引用计数
        self._lru: OrderedDict[str, bool] = OrderedDict()
        self._inflight: dict[str, int] = {}       # 临时文件路径 -> 预留字节数

    # ---- 目录与恢复 -----------------------------------------------------

    def ensure_dirs(self) -> None:
        self._blobs_dir.mkdir(parents=True, exist_ok=True)

    def scan_blobs(self) -> None:
        """启动时扫描磁盘重建容量账本与 LRU 顺序。"""
        with self._lock:
            self.ensure_dirs()
            # 上次崩溃残留的临时块直接清理（不可能被引用）
            for leftover in self._blobs_dir.glob(".staging-*.tmp"):
                try:
                    leftover.unlink()
                except FileNotFoundError:
                    pass
            self._sizes.clear()
            self._lru.clear()
            entries = [(p, p.stat().st_mtime) for p in self._blobs_dir.glob("*") if p.is_file()]
            entries.sort(key=lambda item: item[1])
            for path, _mtime in entries:
                self._sizes[path.name] = path.stat().st_size
                self._lru[path.name] = True

    # ---- 容量与淘汰 -----------------------------------------------------

    def used_bytes(self) -> int:
        """已落盘 blob + 在途预留的总字节数。"""
        with self._lock:
            return sum(self._sizes.values()) + sum(self._inflight.values())

    def reclaimable_bytes(self) -> int:
        """当前可被淘汰（引用计数为 0）的 blob 总字节数。"""
        with self._lock:
            return sum(
                size for name, size in self._sizes.items() if self._refs.get(name, 0) == 0
            )

    def _evict_locked(self, need_free: int) -> int:
        """按 LRU 淘汰引用计数为 0 的 blob，直到腾出 >= need_free 字节。

        返回实际腾出的字节数。被引用的 blob 一律跳过。
        """
        freed = 0
        while freed < need_free:
            candidate: str | None = None
            for name in self._lru:
                if self._refs.get(name, 0) == 0:
                    candidate = name
                    break
            if candidate is None:
                break
            path = self.blob_path(candidate)
            size = self._sizes.pop(candidate, 0)
            self._lru.pop(candidate, None)
            try:
                path.unlink()
            except FileNotFoundError:
                size = 0
            freed += size
            logger.info("blob evicted blob=%s bytes=%s reason=lru_unreferenced", candidate, size)
        return freed

    def _reserve_locked(self, size: int) -> None:
        if size > self._config.capacity_bytes:
            raise CapacityExceededError(
                f"分片 {size} 字节超过暂存区总容量 {self._config.capacity_bytes}",
                reason="shard_larger_than_capacity",
                shard_size=size,
                capacity=self._config.capacity_bytes,
            )
        used = sum(self._sizes.values()) + sum(self._inflight.values())
        if used + size <= self._config.capacity_bytes:
            return
        # 容量吃紧：先尝试淘汰无人引用的旧 blob
        need = used + size - self._config.capacity_bytes
        freed = self._evict_locked(need) if self._config.eviction_enabled else 0
        used_after = used - freed
        if used_after + size > self._config.capacity_bytes:
            raise CapacityExceededError(
                "暂存空间不足，且没有可淘汰的空闲数据",
                reason="no_reclaimable_space",
                requested=size,
                used=used_after,
                capacity=self._config.capacity_bytes,
                reclaimable_busy=True,
            )

    # ---- 写入暂存 -------------------------------------------------------

    def stage_stream(
        self,
        stream: Iterable[bytes] | IO[bytes],
        expected_digest: str,
        expected_size: int,
        pin: bool = True,
    ) -> tuple[str, int]:
        """流式写入暂存并做完整性校验。

        成功返回 ``(blob_name, size)``；任何校验失败或中途异常都会删除临时
        文件并释放预留，保证半成品不会留在暂存区。
        """
        expected_digest = normalize_digest(expected_digest)
        algorithm = digest_algorithm(expected_digest)

        with self._lock:
            self.ensure_dirs()
            self._reserve_locked(expected_size)
            fd, tmp_name = tempfile.mkstemp(
                prefix=".staging-", suffix=".tmp", dir=self._blobs_dir
            )
            self._inflight[tmp_name] = expected_size

        actual_size = 0
        try:
            with os.fdopen(fd, "wb") as fh:
                def hashing_iter() -> Iterable[bytes]:
                    nonlocal actual_size
                    for chunk in _iter_chunks(stream):
                        actual_size += len(chunk)
                        # 提前发现超长，避免恶意大文件把磁盘写爆
                        if actual_size > expected_size:
                            raise ChecksumMismatchError(
                                f"实际大小 {actual_size} 超过声明大小 {expected_size}",
                                reason="size_exceeds_declared",
                                expected_size=expected_size,
                                actual_size=actual_size,
                            )
                        fh.write(chunk)
                        yield chunk
                actual_digest = compute_digest(hashing_iter(), algorithm=algorithm)
                fh.flush()
                os.fsync(fh.fileno())
        except BaseException:
            with self._lock:
                self._inflight.pop(tmp_name, None)
            Path(tmp_name).unlink(missing_ok=True)
            logger.warning(
                "stage rolled back expected_digest=%s reason=stream_or_checksum_failure actual_bytes=%s",
                expected_digest,
                actual_size,
            )
            raise

        if actual_size != expected_size:
            Path(tmp_name).unlink(missing_ok=True)
            with self._lock:
                self._inflight.pop(tmp_name, None)
            raise ChecksumMismatchError(
                f"大小不一致：声明 {expected_size}，实际 {actual_size}",
                reason="size_mismatch",
                expected_size=expected_size,
                actual_size=actual_size,
                expected_digest=expected_digest,
            )
        if actual_digest != expected_digest:
            Path(tmp_name).unlink(missing_ok=True)
            with self._lock:
                self._inflight.pop(tmp_name, None)
            raise ChecksumMismatchError(
                "内容指纹不一致，拒绝写入",
                reason="digest_mismatch",
                expected_digest=expected_digest,
                actual_digest=actual_digest,
            )

        blob_name = digest_to_blob_name(actual_digest)
        final_path = self.blob_path(blob_name)
        with self._lock:
            self._inflight.pop(tmp_name, None)
            existed = final_path.exists()
            os.replace(tmp_name, final_path)
            if not existed:
                self._sizes[blob_name] = actual_size
            self._lru[blob_name] = True
            self._lru.move_to_end(blob_name)
            if pin:
                self._refs[blob_name] = self._refs.get(blob_name, 0) + 1
        logger.info(
            "stage committed blob=%s bytes=%s dedup_hit=%s pinned=%s",
            blob_name,
            actual_size,
            existed,
            pin,
        )
        return blob_name, actual_size

    # ---- 引用计数 -------------------------------------------------------

    def pin(self, blob_name: str) -> None:
        with self._lock:
            if blob_name not in self._sizes:
                raise FileNotFoundError(f"blob 不存在，无法 pin: {blob_name}")
            self._refs[blob_name] = self._refs.get(blob_name, 0) + 1
            self._lru.move_to_end(blob_name)

    def unpin(self, blob_name: str) -> None:
        with self._lock:
            count = self._refs.get(blob_name, 0)
            if count <= 0:
                raise RuntimeError(f"blob 未被引用，不能 unpin: {blob_name}")
            if count == 1:
                del self._refs[blob_name]
            else:
                self._refs[blob_name] = count - 1

    def ref_count(self, blob_name: str) -> int:
        with self._lock:
            return self._refs.get(blob_name, 0)

    # ---- 读取与校验 -----------------------------------------------------

    def exists(self, blob_name: str) -> bool:
        return self.blob_path(blob_name).is_file()

    def blob_path(self, blob_name: str) -> Path:
        return self._blobs_dir / blob_name

    def open_blob(self, blob_name: str) -> IO[bytes]:
        return self.blob_path(blob_name).open("rb")

    def verify_blob(self, blob_name: str, expected_digest: str, expected_size: int) -> None:
        """对已落盘 blob 重新做一次完整校验，损坏则显式报错。"""
        expected_digest = normalize_digest(expected_digest)
        path = self.blob_path(blob_name)
        with path.open("rb") as fh:
            actual_digest = compute_digest(
                iter(lambda: fh.read(_READ_CHUNK), b""),
                algorithm=digest_algorithm(expected_digest),
            )
        actual_size = path.stat().st_size
        if actual_size != expected_size or actual_digest != expected_digest:
            raise ChecksumMismatchError(
                "暂存 blob 完整性校验失败，拒绝使用",
                reason="blob_corrupted",
                blob=blob_name,
                expected_digest=expected_digest,
                actual_digest=actual_digest,
                expected_size=expected_size,
                actual_size=actual_size,
            )

    def evict_expired(self, target_bytes: int | None = None) -> list[str]:
        """淘汰无人引用的 LRU blob，使总占用不超过 target_bytes。

        target_bytes 默认取容量上限。返回被删除的 blob 名列表。
        """
        target = self._config.capacity_bytes if target_bytes is None else target_bytes
        removed: list[str] = []
        with self._lock:
            need = sum(self._sizes.values()) - target
            while need > 0:
                candidate: str | None = None
                for name in self._lru:
                    if self._refs.get(name, 0) == 0:
                        candidate = name
                        break
                if candidate is None:
                    break
                path = self.blob_path(candidate)
                size = self._sizes.pop(candidate, 0)
                self._lru.pop(candidate, None)
                try:
                    path.unlink()
                except FileNotFoundError:
                    continue
                removed.append(candidate)
                need -= size
                logger.info("blob evicted blob=%s bytes=%s reason=capacity_target", candidate, size)
        return removed
