import hashlib
import time

import pytest

from staging import Scheduler, ShardRequest, StagingStore, TaskStatus


def make_shard(shard_id, target, data: bytes, chunk=8, delay=0.0,
               fingerprint=None):
    """构造可控假数据源：分块产出、可注入延迟、可伪造指纹。"""
    fp = fingerprint or hashlib.sha256(data).hexdigest()
    chunks = [data[i:i + chunk] for i in range(0, len(data), chunk)] + [b""]
    it = iter(chunks)

    def read():
        if delay:
            time.sleep(delay)
        return next(it)

    return ShardRequest(shard_id, target, fp, len(data), read)


def wait_done(sched: Scheduler, task_id: str, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        info = sched.get(task_id)
        if info.status not in (TaskStatus.PENDING, TaskStatus.RUNNING):
            return info
        time.sleep(0.01)
    raise TimeoutError(task_id)


@pytest.fixture
def store(tmp_path):
    return StagingStore(tmp_path / "staging", max_bytes=1 << 20)


@pytest.fixture
def sched(store):
    s = Scheduler(store, max_workers=2, max_pending=1)
    yield s
    s.shutdown()
