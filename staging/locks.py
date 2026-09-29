"""进程内并发控制：目标位置互斥锁注册表。

设计说明（单进程服务）：
- 每个规范化目标路径对应一把可重入计数的互斥锁；
- 获取时按“全局大锁 -> 路径锁”顺序加锁，避免死锁；
- 注册表本身只负责锁的生命周期与借用计数，不做排队等待（拿不到立刻失败，
  由上层映射成 TargetLockedError / 409，语义清晰）。
"""

from __future__ import annotations

import os
import threading


class TargetLockRegistry:
    """按目标路径发放互斥锁，保证同一位置不会被两个任务并发写入。"""

    def __init__(self) -> None:
        self._registry_lock = threading.Lock()
        self._locks: dict[str, threading.Lock] = {}
        # 每个目标路径当前持有者，仅用于生成可解释的错误信息
        self._holders: dict[str, str] = {}

    @staticmethod
    def normalize(target_path: str) -> str:
        """把目标路径规范化成稳定的键。"""
        return os.path.normpath(target_path)

    def try_acquire(self, target_path: str, holder: str = "") -> bool:
        """非阻塞尝试获取目标锁。成功返回 True；被占用返回 False。"""
        key = self.normalize(target_path)
        with self._registry_lock:
            lock = self._locks.get(key)
            if lock is None:
                lock = threading.Lock()
                self._locks[key] = lock
            acquired = lock.acquire(blocking=False)
            if acquired:
                self._holders[key] = holder or "unknown"
            return acquired

    def release(self, target_path: str) -> None:
        """释放目标锁。未持有时释放会抛 RuntimeError（属于编程错误）。"""
        key = self.normalize(target_path)
        with self._registry_lock:
            lock = self._locks.get(key)
            if lock is None:
                raise RuntimeError(f"目标路径未注册过锁: {target_path}")
            self._holders.pop(key, None)
        lock.release()

    def holder_of(self, target_path: str) -> str:
        """查询当前持有目标锁的任务标识（用于日志与错误信息）。"""
        key = self.normalize(target_path)
        with self._registry_lock:
            return self._holders.get(key, "")

    def locked_paths(self) -> list[str]:
        """当前被锁住的全部目标路径（观测/测试用）。"""
        with self._registry_lock:
            return sorted(self._holders)
