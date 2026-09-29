"""去重幂等、重启恢复、容量淘汰、校验失败回滚、元数据损坏。"""

import hashlib
import json
import logging

import pytest

from staging import (
    CapacityExceededError,
    ChecksumMismatchError,
    MetadataCorruptedError,
    ShardStatus,
    StagingStore,
    TaskStatus,
)
from staging.scheduler import Scheduler
from tests.conftest import make_shard, wait_done

log = logging.getLogger("test.store")


def test_dedup_repeat_submit(store, sched):
    """重复提交相同分片：第二次去重命中，不再占用空间。"""
    data = b"duplicate me" * 10
    t1 = sched.submit([make_shard("s1", "/a", data)])
    info1 = wait_done(sched, t1)
    used_after_first = store.used_bytes

    t2 = sched.submit([make_shard("s1-again", "/b", data)])
    info2 = wait_done(sched, t2)
    log.info("task=%s shard=%s 判定=%s; task=%s shard=%s 判定=%s",
             t1, "s1", info1.results[0].detail,
             t2, "s1-again", info2.results[0].detail)
    assert info1.results[0].status == ShardStatus.COMMITTED
    assert info2.results[0].status == ShardStatus.DEDUPED
    assert store.used_bytes == used_after_first


def test_restart_recovery(tmp_path):
    """进程重启后已完成分片状态可恢复，重复提交直接去重。"""
    root = tmp_path / "staging"
    data = b"persisted shard"
    fp = hashlib.sha256(data).hexdigest()

    store1 = StagingStore(root, max_bytes=1 << 20)
    sched1 = Scheduler(store1, max_workers=1)
    t = sched1.submit([make_shard("s1", "/a", data)])
    info = wait_done(sched1, t)
    sched1.shutdown()
    assert info.results[0].status == ShardStatus.COMMITTED
    log.info("重启前: task=%s shard=s1 fp=%s committed", t, fp)

    # 模拟重启：同一目录重建
    store2 = StagingStore(root, max_bytes=1 << 20)
    sched2 = Scheduler(store2, max_workers=1)
    assert store2.has(fp), "重启后指纹状态应恢复"
    t2 = sched2.submit([make_shard("s1-reboot", "/a", data)])
    info2 = wait_done(sched2, t2)
    sched2.shutdown()
    log.info("重启后: task=%s shard=s1-reboot 判定=%s",
             t2, info2.results[0].detail)
    assert info2.results[0].status == ShardStatus.DEDUPED


def test_capacity_eviction(tmp_path):
    """容量上限触发 LRU 淘汰；被引用的数据不能淘汰；放不下时报容量错误。"""
    store = StagingStore(tmp_path / "staging", max_bytes=100)
    sched = Scheduler(store, max_workers=1)

    def put(sid, n):
        t = sched.submit([make_shard(sid, f"/{sid}", bytes([n]) * 40)])
        return wait_done(sched, t)

    put("old", 1)   # 40B
    put("keep", 2)  # 40B
    fp_keep = hashlib.sha256(bytes([2]) * 40).hexdigest()
    store.add_ref(fp_keep)  # keep 被引用，不可淘汰
    put("new", 3)   # 需要 40B：淘汰未被引用的 old
    sched.shutdown()

    fp_old = hashlib.sha256(bytes([1]) * 40).hexdigest()
    fp_new = hashlib.sha256(bytes([3]) * 40).hexdigest()
    log.info("判定依据: old(fp=%s)被淘汰, keep被引用保留, new提交成功",
             fp_old[:12])
    assert not store.has(fp_old)
    assert store.has(fp_keep)
    assert store.has(fp_new)
    assert store.used_bytes <= 100

    # 全部被引用且空间不足 -> 明确拒绝
    store.add_ref(fp_new)
    with pytest.raises(CapacityExceededError):
        store.commit_shard("manual", make_shard("too-big", "/big", b"z" * 50))
    log.info("判定依据: 所有 blob 被引用且容量不足 -> capacity_exceeded")


def test_checksum_failure_rollback(store, sched, tmp_path):
    """指纹对不上：明确报错，半成品被清理，旧数据不被覆盖。"""
    good = b"good data"
    fp_good = hashlib.sha256(good).hexdigest()
    t1 = sched.submit([make_shard("orig", "/a", good)])
    wait_done(sched, t1)

    # 声明一个不存在的指纹，但内容对不上
    fp_fake = hashlib.sha256(b"not the real content").hexdigest()
    t2 = sched.submit([make_shard("evil", "/a", b"corrupted!",
                                  fingerprint=fp_fake)])
    info2 = wait_done(sched, t2)
    r = info2.results[0]
    log.info("task=%s shard=evil 判定=%s", t2, r.detail)
    assert r.status == ShardStatus.FAILED
    assert "ChecksumMismatchError" in r.detail
    assert info2.status == TaskStatus.FAILED

    # 半成品已回滚，tmp 目录干净
    leftovers = list((tmp_path / "staging" / "tmp").glob("*.part"))
    assert leftovers == [], f"半成品未回滚: {leftovers}"
    # 旧数据未被覆盖
    blob = tmp_path / "staging" / "blobs" / fp_good
    assert blob.read_bytes() == good


def test_metadata_corrupted(tmp_path):
    """元数据损坏：明确报错，不静默覆盖。"""
    root = tmp_path / "staging"
    store = StagingStore(root, max_bytes=1024)
    store.commit_shard("t", make_shard("s", "/a", b"data"))
    del store

    (root / "index.json").write_text("{not valid json")
    with pytest.raises(MetadataCorruptedError):
        StagingStore(root, max_bytes=1024)
    log.info("判定依据: index.json 损坏 -> MetadataCorruptedError，拒绝启动")

    # 索引与磁盘不一致也算损坏
    (root / "index.json").write_text(json.dumps(
        {hashlib.sha256(b"data").hexdigest():
         {"size": 999, "refs": 0, "last_used": 0}}))
    with pytest.raises(MetadataCorruptedError):
        StagingStore(root, max_bytes=1024)
    log.info("判定依据: 索引大小与磁盘 blob 不一致 -> MetadataCorruptedError")


def test_orphan_temp_cleanup_on_restart(tmp_path):
    """崩溃残留的 .part 半成品在重启时被清理，不会被读到。"""
    root = tmp_path / "staging"
    StagingStore(root, max_bytes=1024)
    orphan = root / "tmp" / "taskX-shardY.part"
    orphan.write_bytes(b"half-written")
    store = StagingStore(root, max_bytes=1024)  # 重启
    assert not orphan.exists()
    assert store.used_bytes == 0
    log.info("判定依据: 重启清理孤儿 .part，used_bytes=%d", store.used_bytes)
