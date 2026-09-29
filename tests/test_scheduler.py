"""并发冲突、并发上限与取消。"""

import logging

import pytest

from staging import (
    ConcurrencyLimitError,
    ShardStatus,
    TaskStatus,
)
from tests.conftest import make_shard, wait_done

log = logging.getLogger("test.scheduler")


def test_same_target_rejected(sched):
    """同一目标位置不能被两个任务同时写入，后到者以 target_busy 拒绝。"""
    t1 = sched.submit([make_shard("a1", "/same", b"a" * 64, delay=0.05)])
    t2 = sched.submit([make_shard("b1", "/same", b"b" * 64)])
    info1, info2 = wait_done(sched, t1), wait_done(sched, t2)
    results = {t: i.results[0] for t, i in ((t1, info1), (t2, info2))}
    statuses = sorted(r.status for r in results.values())
    log.info("判定依据: t1=%s t2=%s", results[t1].status, results[t2].status)
    assert statuses == [ShardStatus.COMMITTED, ShardStatus.FAILED]
    loser = results[t1] if results[t1].status == ShardStatus.FAILED else results[t2]
    assert "target_busy" in loser.detail


def test_concurrency_limit_rejected(sched):
    """并发压力过大时拒绝提交，原因为 concurrency_limit。"""
    # max_workers=2, max_pending=1 => 同时最多 3 个在途任务
    ids = [sched.submit([make_shard(f"s{i}", f"/t/{i}", b"x" * 32, delay=0.2)])
           for i in range(3)]
    with pytest.raises(ConcurrencyLimitError):
        sched.submit([make_shard("overflow", "/t/x", b"x" * 32)])
    log.info("判定依据: 3 个在途任务后第 4 个被拒绝, ids=%s", ids)
    for t in ids:
        wait_done(sched, t)


def test_cancel_task(sched):
    """运行中的任务可取消，未执行的分片标记为 cancelled。"""
    tid = sched.submit([make_shard(f"c{i}", f"/c/{i}", b"z" * 32, delay=0.05)
                        for i in range(6)])
    sched.cancel(tid)
    info = wait_done(sched, tid)
    log.info("task=%s status=%s results=%s", tid, info.status,
             [(r.shard_id, r.status.value) for r in info.results])
    assert info.status in (TaskStatus.CANCELLED, TaskStatus.FAILED,
                           TaskStatus.SUCCEEDED)
    assert any(r.status == ShardStatus.CANCELLED for r in info.results)


def test_progress_query(sched):
    """运行中的任务可查询进度。"""
    tid = sched.submit([make_shard(f"p{i}", f"/p/{i}", b"d" * 32, delay=0.05)
                        for i in range(3)])
    info = sched.get(tid)
    log.info("task=%s progress=%d/%d status=%s",
             tid, info.finished, info.total, info.status)
    assert info.total == 3
    assert 0 <= info.finished <= 3
    final = wait_done(sched, tid)
    assert final.status == TaskStatus.SUCCEEDED
    assert final.finished == 3
