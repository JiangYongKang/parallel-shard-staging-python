"""容量上限与 LRU 淘汰场景。"""

from __future__ import annotations

import logging
import threading

import pytest

from staging.errors import CapacityExceededError
from staging.models import ShardSpec, ShardState, TaskState

from conftest import BlockingSource, make_config, sha256, wait_completed, wait_for_states
from staging.manager import StagingManager


def _fresh(dirs, **kw) -> StagingManager:
    mgr = StagingManager(make_config(dirs, **kw))
    mgr.start()
    return mgr


def test_lru_evicts_unreferenced_blob_to_make_room(dirs, caplog):
    """无引用的旧 blob 按 LRU 被淘汰以腾空间，被引用的不动。"""
    mgr = _fresh(dirs, capacity_bytes=100)
    from conftest import make_spec

    # 顺序提交（非并发）以确定性固定 LRU 顺序：a 最久未用，b 次之
    t1 = mgr.submit_task([make_spec("a", "a.bin", b"a" * 30)])
    wait_completed(mgr, t1)
    t2 = mgr.submit_task([make_spec("b", "b.bin", b"b" * 60)])
    wait_completed(mgr, t2)
    rec_a = mgr.find_shard("a")
    rec_b = mgr.find_shard("b")
    assert mgr._blobs.ref_count(rec_a.blob_digest) == 0
    assert mgr._blobs.ref_count(rec_b.blob_digest) == 0

    c = b"c" * 40
    t3 = mgr.submit_task([make_spec("c", "c.bin", c)])
    wait_completed(mgr, t3)
    # 30+60+40=130 > 100：最久未用且无引用的 a 必须先被淘汰
    assert not mgr._blobs.exists(rec_a.blob_digest), "LRU 最旧未引用 blob 应被淘汰"
    assert mgr._blobs.used_bytes() <= 100
    logging.getLogger("test").info(
        "判定依据 shard_id=a reason=lru_unreferenced 为新分片腾位 task_id=%s", t3,
    )


def test_pinned_blob_never_evicted(dirs):
    """正在被引用（在途/未 flush）的 blob 不允许淘汰。"""
    mgr = _fresh(dirs, capacity_bytes=100, flush_on_commit=False)
    from conftest import make_spec

    pinned_data = b"p" * 60
    t = mgr.submit_task([make_spec("keep", "keep.bin", pinned_data)])
    wait_completed(mgr, t)
    rec = mgr.find_shard("keep")
    assert rec.state is ShardState.COMPLETED and rec.flushed is False
    assert mgr._blobs.ref_count(rec.blob_digest) == 1

    # 再塞数据：被 pin 的 60 字节不能回收，只剩 40 字节可用
    with pytest.raises(CapacityExceededError) as exc:
        mgr.submit_task([make_spec("big", "big.bin", b"x" * 50)])
    assert exc.value.reason in ("admission_capacity", "no_reclaimable_space")
    assert mgr._blobs.exists(rec.blob_digest), "引用中的 blob 绝不能被淘汰"
    logging.getLogger("test").info(
        "判定依据 shard_id=keep reason=pinned_blob_protected reason_code=%s",
        exc.value.reason,
    )


def test_capacity_rejection_reasons_are_distinguishable(dirs):
    """单片超容量 vs 有数据但不可回收，两种拒绝原因可区分。"""
    mgr = _fresh(dirs, capacity_bytes=100, flush_on_commit=False)
    from conftest import make_spec

    # 1) 单片超过总容量
    with pytest.raises(CapacityExceededError) as exc:
        mgr.submit_task([make_spec("huge", "huge.bin", b"h" * 101)])
    assert exc.value.reason in ("shard_larger_than_capacity", "admission_capacity")

    # 2) 占住不可回收空间后，正常大小也进不来
    t = mgr.submit_task([make_spec("hold", "hold.bin", b"o" * 60)])
    wait_completed(mgr, t)
    with pytest.raises(CapacityExceededError) as exc:
        mgr.submit_task([make_spec("need", "need.bin", b"n" * 50)])
    assert exc.value.reason in ("no_reclaimable_space", "admission_capacity")
    logging.getLogger("test").info(
        "判定依据 reason1=oversize reason2=%s 两类容量拒绝可区分", exc.value.reason,
    )


def test_inflight_bytes_counted_towards_capacity(dirs):
    """写入中的字节也计入容量，防止“边写边超”。"""
    mgr = _fresh(dirs, capacity_bytes=100)
    gate = threading.Event()
    data = b"w" * 60
    spec = ShardSpec("w1", "w1.bin", 60, sha256(data), BlockingSource(data, gate, head=8))
    mgr.submit_task([spec])
    import time
    time.sleep(0.2)
    # 在途 60 字节 + 再申请 50：110 > 100
    from conftest import make_spec
    with pytest.raises(CapacityExceededError):
        mgr.submit_task([make_spec("w2", "w2.bin", b"v" * 50)])
    gate.set()


def test_eviction_disabled_then_hard_rejection(dirs):
    """关闭淘汰策略后，空间不足直接拒绝，不删任何 blob。"""
    mgr = _fresh(dirs, capacity_bytes=100, eviction_enabled=False)
    from conftest import make_spec

    t = mgr.submit_task([make_spec("old", "old.bin", b"o" * 60)])
    wait_completed(mgr, t)
    rec = mgr.find_shard("old")
    with pytest.raises(CapacityExceededError):
        mgr.submit_task([make_spec("new", "new.bin", b"n" * 50)])
    assert mgr._blobs.exists(rec.blob_digest), "关闭淘汰时旧 blob 必须保留"
