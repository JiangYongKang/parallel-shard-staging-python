"""去重与幂等场景。"""

from __future__ import annotations

import logging

import pytest

from staging.errors import InvalidRequestError
from staging.models import ShardSpec, ShardState, TaskState

from conftest import compute_digest, make_spec, sha256, wait_completed


def test_identical_content_shares_one_blob(manager, dirs, caplog):
    """不同 shard_id 但内容相同：物理上只占一份空间。"""
    data = b"same content payload" * 3
    task_id = manager.submit_task(
        [
            make_spec("d1", "d1.bin", data),
            make_spec("d2", "d2.bin", data),
            make_spec("d3", "d3.bin", data),
        ]
    )
    view = wait_completed(manager, task_id)
    assert view.state is TaskState.COMPLETED
    assert manager._blobs.used_bytes() == len(data)
    logging.getLogger("test").info(
        "判定依据 task_id=%s shard_ids=d1,d2,d3 相同指纹共享一个 blob used_bytes=%s",
        task_id, len(data),
    )


def test_repeated_submit_is_idempotent(manager, dirs, caplog):
    """同一 shard_id + 相同指纹/目标/大小重复提交：直接命中，不重新写入。"""
    data = b"idempotent-data"
    first = manager.submit_task([make_spec("idem", "idem.bin", data)])
    wait_completed(manager, first)
    used_after_first = manager._blobs.used_bytes()

    second = manager.submit_task([make_spec("idem", "idem.bin", data)])
    view = wait_completed(manager, second)
    assert view.state is TaskState.COMPLETED
    assert manager._blobs.used_bytes() == used_after_first
    assert (dirs[1] / "idem.bin").read_bytes() == data
    logging.getLogger("test").info(
        "判定依据 task_id=%s shard_id=idem decision=idempotent_hit", second,
    )


def test_conflicting_redigest_is_rejected_not_overwritten(manager, dirs):
    """相同 shard_id 但指纹不同：显式拒绝，旧数据原封不动。"""
    manager.submit_task([make_spec("fixed", "fixed.bin", b"original-bytes")])
    import time
    time.sleep(0.15)
    before = (dirs[1] / "fixed.bin").read_bytes()

    with pytest.raises(InvalidRequestError) as exc:
        manager.submit_task([make_spec("fixed", "fixed.bin", b"tampered-bytes")])
    assert exc.value.reason == "idempotent_conflict"
    assert (dirs[1] / "fixed.bin").read_bytes() == before
    logging.getLogger("test").info(
        "判定依据 shard_id=fixed reason=idempotent_conflict 旧数据保持不变",
    )


def test_conflicting_target_for_same_shard_rejected(manager):
    """相同 shard_id 但目标位置变化：拒绝，不允许换位置覆盖。"""
    manager.submit_task([make_spec("mv", "a.bin", b"payload here")])
    import time
    time.sleep(0.15)
    with pytest.raises(InvalidRequestError) as exc:
        manager.submit_task([make_spec("mv", "b.bin", b"payload here")])
    assert exc.value.reason == "idempotent_conflict"


def test_different_content_stored_separately(manager):
    """不同内容各占各的 blob。"""
    a, b = b"alpha-alpha", b"beta-beta!!"
    task_id = manager.submit_task([make_spec("a", "a.bin", a), make_spec("b", "b.bin", b)])
    wait_completed(manager, task_id)
    assert manager._blobs.used_bytes() == len(a) + len(b)


def test_digest_algorithm_prefix_accepted(manager, dirs):
    """裸 hex 与带前缀指纹等价。"""
    data = b"prefix-check"
    prefixed = sha256(data)
    bare = prefixed.split(":", 1)[1]
    spec = ShardSpec(
        "pre", "pre.bin", len(data), bare, data,
    )
    task_id = manager.submit_task([spec])
    view = wait_completed(manager, task_id)
    assert view.shards["pre"].state is ShardState.COMPLETED
    assert view.shards["pre"].digest == prefixed
