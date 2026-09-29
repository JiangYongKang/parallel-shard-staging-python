"""完整性校验与失败回滚场景。"""

from __future__ import annotations

import logging
from pathlib import Path

import pytest

from staging.errors import ChecksumMismatchError
from staging.models import ShardSpec, ShardState, TaskState

from conftest import (
    FailingSource,
    assert_no_tmp_files,
    make_spec,
    sha256,
    wait_completed,
)


def test_digest_mismatch_fails_and_rolls_back(manager, dirs, caplog):
    """声明指纹与实际内容不符：分片 FAILED，目标无文件，暂存无残留。"""
    claimed = sha256(b"expected content")
    spec = ShardSpec("bad", "bad.bin", 16, claimed, b"actual content!!")
    task_id = manager.submit_task([spec])
    view = wait_completed(manager, task_id)
    assert view.state is TaskState.FAILED
    shard = view.shards["bad"]
    assert shard.state is ShardState.FAILED
    assert "checksum_mismatch" in shard.error
    assert not (dirs[1] / "bad.bin").exists()
    assert_no_tmp_files(dirs[0])
    logging.getLogger("test").info(
        "判定依据 task_id=%s shard_id=bad reason=digest_mismatch 已回滚", task_id,
    )


def test_size_mismatch_too_long_fails_and_rolls_back(manager, dirs):
    """实际字节多于声明大小：失败并回滚。"""
    data = b"x" * 50
    spec = ShardSpec("long", "long.bin", 20, sha256(data), data)
    task_id = manager.submit_task([spec])
    view = wait_completed(manager, task_id)
    assert view.state is TaskState.FAILED
    assert not (dirs[1] / "long.bin").exists()
    assert_no_tmp_files(dirs[0])
    logging.getLogger("test").info(
        "判定依据 shard_id=long reason=size_exceeds_declared 已回滚",
    )


def test_size_mismatch_too_short_fails(manager, dirs):
    data = b"short"
    spec = ShardSpec("short", "short.bin", 99, sha256(data), data)
    task_id = manager.submit_task([spec])
    view = wait_completed(manager, task_id)
    assert view.state is TaskState.FAILED
    assert "size_mismatch" in view.shards["short"].error
    assert_no_tmp_files(dirs[0])


def test_source_exception_rolls_back_partial(manager, dirs):
    """假数据源读到一半抛错：临时文件被删除，不留半成品。"""
    data = b"abcdefghijklmnop"
    spec = ShardSpec(
        "boom", "boom.bin", len(data), sha256(data), FailingSource(data, fail_at=4)
    )
    task_id = manager.submit_task([spec])
    view = wait_completed(manager, task_id)
    assert view.state is TaskState.FAILED
    assert not (dirs[1] / "boom.bin").exists()
    assert_no_tmp_files(dirs[0])
    logging.getLogger("test").info(
        "判定依据 shard_id=boom reason=source_error_partial_rollback",
    )


def test_failed_shard_not_readable_by_later_request(manager, dirs):
    """失败的分片不能被后续请求当作有效数据读到（blob 无引用且可被淘汰）。"""
    bad_spec = ShardSpec("ghost", "ghost.bin", 5, sha256(b"zzzzz"), b"aaaaa")
    first = manager.submit_task([bad_spec])
    wait_completed(manager, first)
    assert manager.find_shard("ghost").state is ShardState.FAILED

    # 用正确数据重提：必须全量重跑成功，而不是读到失败残留
    good = b"zzzzz"
    second = manager.submit_task([make_spec("ghost2", "ghost2.bin", good)])
    view = wait_completed(manager, second)
    assert view.state is TaskState.COMPLETED
    assert (dirs[1] / "ghost2.bin").read_bytes() == good
    assert_no_tmp_files(dirs[0])


def test_staged_blob_tampering_detected(manager, dirs):
    """已暂存 blob 在 flush 前被篡改：恢复校验必须发现并报错，拒绝落盘。"""
    import json
    import time

    data = b"tamper-target!!"
    task_id = manager.submit_task([make_spec("tp", "tp.bin", data)])
    wait_completed(manager, task_id)
    manager.shutdown()

    rec = json.loads((dirs[0] / "shards" / "tp.json").read_text())
    blob = dirs[0] / "blobs" / rec["blob_digest"]
    blob.write_bytes(b"0" * len(data))  # 直接篡改 blob 内容
    # 伪造成 STAGED 未 flush，让重启恢复路径去做 verify
    rec["state"] = "staged"
    rec["flushed"] = False
    (dirs[0] / "shards" / "tp.json").write_text(json.dumps(rec))
    (dirs[1] / "tp.bin").unlink()

    from staging.manager import StagingManager
    from conftest import make_config

    mgr2 = StagingManager(make_config(dirs))
    mgr2.start()
    task_id2 = mgr2.submit_task([make_spec("tp", "tp.bin", data)])
    view = wait_completed(mgr2, task_id2)
    assert view.state is TaskState.FAILED
    assert not (dirs[1] / "tp.bin").exists(), "损坏 blob 绝不允许落盘"
    logging.getLogger("test").info(
        "判定依据 shard_id=tp reason=blob_corrupted 拒绝落盘",
    )
