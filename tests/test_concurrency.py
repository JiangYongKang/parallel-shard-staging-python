"""并发场景：目标互斥、并发上限拒绝、进度查询与取消。"""

from __future__ import annotations

import logging
import threading

import pytest

from staging.errors import ConcurrencyLimitError, InvalidRequestError, TargetLockedError
from staging.models import ShardSpec, ShardState, TaskState

from conftest import BlockingSource, make_config, make_spec, sha256, wait_completed, wait_for_states


def test_same_target_conflict_returns_409_reason(manager, dirs, caplog):
    """两个任务写同一目标位置：第二个必须被明确拒绝，原因 target_busy。"""
    gate = threading.Event()
    data = b"sharded-data!!"
    source = BlockingSource(data, gate)
    spec = ShardSpec("s-hold", "shared/out.bin", len(data), sha256(data), source)
    task_a = manager.submit_task([spec])

    # 等 A 真正进入 STAGING，占住目标锁与工作槽
    deadline = threading.Event().wait(0.3)
    import time
    time.sleep(0.2)

    with pytest.raises(TargetLockedError) as exc:
        manager.submit_task([make_spec("s-b", "shared/out.bin", b"other content!!!")])
    assert exc.value.status_code == 409
    assert exc.value.reason == "target_busy"
    assert exc.value.context["held_by"] == task_a
    logging.getLogger("test").info(
        "判定依据 task_id=%s shard_id=%s reason=%s held_by=%s",
        "second-request", "s-b", exc.value.reason, exc.value.context["held_by"],
    )

    gate.set()
    view = wait_completed(manager, task_a)
    assert view.state is TaskState.COMPLETED
    assert (dirs[1] / "shared" / "out.bin").read_bytes() == data


def test_concurrency_limit_returns_503(manager, dirs, caplog):
    """工作槽全部被占时，新请求返回 503，reason=concurrency_slots_busy。"""
    cfg = make_config(dirs, max_concurrent_shards=2)
    manager._config = cfg  # 让夹具内 manager 使用更紧的并发度
    manager._slots = threading.BoundedSemaphore(2)

    gate = threading.Event()
    d1, d2, d3 = b"a" * 16, b"b" * 16, b"c" * 16
    specs = [
        ShardSpec("c1", "c1.bin", len(d1), sha256(d1), BlockingSource(d1, gate)),
        ShardSpec("c2", "c2.bin", len(d2), sha256(d2), BlockingSource(d2, gate)),
    ]
    task_id = manager.submit_task(specs)
    import time
    time.sleep(0.25)

    with pytest.raises(ConcurrencyLimitError) as exc:
        manager.submit_task([make_spec("c3", "c3.bin", d3)])
    assert exc.value.status_code == 503
    assert exc.value.reason == "concurrency_slots_busy"
    logging.getLogger("test").info(
        "判定依据 task_id=%s shard_id=%s reason=%s limit=%s",
        "overflow", "c3", exc.value.reason, 2,
    )

    gate.set()
    view = wait_completed(manager, task_id)
    assert view.state is TaskState.COMPLETED


def test_duplicate_target_in_single_batch_rejected(manager):
    """同一批次内两个分片指向同一目标：400，原因 duplicate_target_in_batch。"""
    with pytest.raises(InvalidRequestError) as exc:
        manager.submit_task(
            [make_spec("x", "dup.bin", b"1"), make_spec("y", "dup.bin", b"22")]
        )
    assert exc.value.reason == "duplicate_target_in_batch"


def test_duplicate_shard_id_in_single_batch_rejected(manager):
    with pytest.raises(InvalidRequestError) as exc:
        manager.submit_task(
            [make_spec("s", "a.bin", b"1"), make_spec("s", "b.bin", b"22")]
        )
    assert exc.value.reason == "duplicate_shard_id_in_batch"


def test_progress_query_counts_states(manager, dirs):
    """进度接口按状态统计分片数量。"""
    gate = threading.Event()
    blocking = BlockingSource(b"z" * 20, gate)
    specs = [
        ShardSpec("slow", "slow.bin", 20, sha256(b"z" * 20), blocking),
        make_spec("fast1", "fast1.bin", b"quick"),
        make_spec("fast2", "fast2.bin", b"rapid!"),
    ]
    task_id = manager.submit_task(specs)
    import time
    time.sleep(0.25)
    progress = manager.get_task(task_id).progress()
    assert progress["total"] == 3
    # 两个快分片应已完成
    assert progress["completed"] == 2
    assert progress[ShardState.STAGING.value] == 1

    gate.set()
    view = wait_completed(manager, task_id)
    assert view.progress()["completed"] == 3
    assert view.state is TaskState.COMPLETED


def test_cancel_mid_stage_marks_canceled(manager, dirs, caplog):
    """取消进行中的任务：分片转为 canceled，落盘侧无半成品。"""
    gate = threading.Event()
    data = b"q" * 40
    spec = ShardSpec("k1", "k1.bin", len(data), sha256(data), BlockingSource(data, gate))
    task_id = manager.submit_task([spec])
    import time
    time.sleep(0.25)
    manager.cancel_task(task_id)
    gate.set()
    view = wait_for_states(manager, task_id, {TaskState.CANCELED})
    assert view.state is TaskState.CANCELED
    states = {s: r.state for s, r in view.shards.items()}
    assert states["k1"] is ShardState.CANCELED
    assert not (dirs[1] / "k1.bin").exists()

    leftovers = list(dirs[0].rglob("*.tmp"))
    assert leftovers == []
    logging.getLogger("test").info(
        "判定依据 task_id=%s shard_id=k1 state=canceled tmp_leftovers=0", task_id,
    )
