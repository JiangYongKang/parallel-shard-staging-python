"""拒绝原因区分：容量不足 vs 并发超限（调度层 + HTTP 层）。"""

import base64
import hashlib
import importlib
import logging

import pytest

from staging import CapacityExceededError, Scheduler, StagingStore
from tests.conftest import make_shard

log = logging.getLogger("test.rejection")


def test_capacity_exceeded_at_submit(tmp_path):
    """暂存空间不足时提交即被拒绝，原因是 capacity_exceeded。"""
    store = StagingStore(tmp_path / "staging", max_bytes=64)
    sched = Scheduler(store, max_workers=1)
    try:
        with pytest.raises(CapacityExceededError) as exc:
            sched.submit([make_shard("big", "/big", b"x" * 128)])
        log.info("判定依据: need 128B > limit 64B -> %s", exc.value)
    finally:
        sched.shutdown()


def test_dedup_shards_dont_count_against_capacity(tmp_path):
    """已去重的分片不计入容量预检，重复提交不会被容量拒绝。"""
    store = StagingStore(tmp_path / "staging", max_bytes=64)
    sched = Scheduler(store, max_workers=1)
    try:
        data = b"y" * 60
        from tests.conftest import wait_done
        t = sched.submit([make_shard("s", "/a", data)])
        wait_done(sched, t)
        # 同样的数据再提交：虽然剩余空间不足 60B，但去重命中应放行
        t2 = sched.submit([make_shard("s2", "/b", data)])
        info = wait_done(sched, t2)
        log.info("task=%s 判定=%s", t2, info.results[0].detail)
        assert info.results[0].status.value == "deduped"
    finally:
        sched.shutdown()


def test_http_rejection_reasons(tmp_path, monkeypatch):
    """HTTP 层：429=并发超限，507=容量不足，二者可区分。"""
    monkeypatch.setenv("STAGING_DIR", str(tmp_path / "staging"))
    monkeypatch.setenv("STAGING_MAX_BYTES", "64")
    monkeypatch.setenv("STAGING_MAX_WORKERS", "1")
    monkeypatch.setenv("STAGING_MAX_PENDING", "0")
    import main
    importlib.reload(main)
    from fastapi.testclient import TestClient
    client = TestClient(main.app)

    def body(sid, data):
        return {"shards": [{"shard_id": sid, "target": f"/{sid}",
                            "fingerprint": hashlib.sha256(data).hexdigest(),
                            "size": len(data),
                            "content_b64": base64.b64encode(data).decode()}]}

    r = client.post("/tasks", json=body("too-big", b"x" * 128))
    log.info("容量不足 -> %s %s", r.status_code, r.json())
    assert r.status_code == 507
    assert "capacity_exceeded" in r.json()["detail"]

    # 让第一个任务卡在写入中，第二个任务必然因并发超限被拒
    import threading
    gate = threading.Event()
    orig_commit = main.store.commit_shard

    def blocking_commit(*a, **kw):
        gate.wait(5)
        return orig_commit(*a, **kw)

    monkeypatch.setattr(main.store, "commit_shard", blocking_commit)
    r1 = client.post("/tasks", json=body("small", b"x" * 8))
    assert r1.status_code == 201
    r2 = client.post("/tasks", json=body("small2", b"y" * 8))
    log.info("并发超限 -> %s %s", r2.status_code, r2.json())
    assert r2.status_code == 429
    assert "concurrency_limit" in r2.json()["detail"]
    gate.set()
