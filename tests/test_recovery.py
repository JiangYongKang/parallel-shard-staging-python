"""重启恢复与元数据损坏场景。"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest

from staging.errors import MetadataCorruptedError
from staging.manager import StagingManager
from staging.models import ShardState, TaskState

from conftest import make_config, make_spec, wait_completed


def _restart(dirs, **kwargs) -> StagingManager:
    mgr = StagingManager(make_config(dirs, **kwargs))
    mgr.start()
    return mgr


def test_completed_shards_recoverable_after_restart(manager, dirs, caplog):
    """已完成分片在“新进程”里仍可查询、仍可幂等命中。"""
    data = b"persisted-payload-123"
    task_id = manager.submit_task([make_spec("p1", "p1.bin", data)])
    wait_completed(manager, task_id)
    manager.shutdown()

    mgr2 = _restart(dirs)
    record = mgr2.find_shard("p1")
    assert record is not None
    assert record.state is ShardState.COMPLETED
    assert record.flushed is True

    again = mgr2.submit_task([make_spec("p1", "p1.bin", data)])
    view = wait_completed(mgr2, again)
    assert view.state is TaskState.COMPLETED
    logging.getLogger("test").info(
        "判定依据 restart task_id=%s shard_id=p1 decision=idempotent_hit_after_recovery",
        again,
    )


def test_crash_staging_tmp_files_cleaned_on_startup(manager, dirs):
    """崩溃残留的 .staging-*.tmp 在启动扫描时被清掉，绝不参与读取。"""
    manager.shutdown()
    leftover = dirs[0] / "blobs" / ".staging-crash.tmp"
    leftover.parent.mkdir(parents=True, exist_ok=True)
    leftover.write_bytes(b"half-written")
    mgr2 = _restart(dirs)
    assert not leftover.exists()
    assert mgr2._blobs.used_bytes() == 0
    logging.getLogger("test").info("判定依据 reason=leftover_tmp_purged")


def test_staged_interrupted_shard_resumes_to_flush(manager, dirs):
    """落盘前崩溃的 STAGED 分片：重启后 blob 被重新 pin，再提交只跑落盘。"""
    import time

    data = b"needs-flush!!"
    manager.submit_task([make_spec("nf", "nf.bin", data)])
    # 等 manifest 落盘后，人为把记录改写成“已暂存未 flush”，模拟落盘前崩溃
    time.sleep(0.2)
    manager.shutdown()
    manifest = json.loads((dirs[0] / "shards" / "nf.json").read_text())
    assert manifest["state"] == "completed"
    manifest["state"] = "staged"
    manifest["flushed"] = False
    (dirs[0] / "shards" / "nf.json").write_text(json.dumps(manifest))
    # 最终文件删掉，模拟 flush 没发生
    (dirs[1] / "nf.bin").unlink()

    mgr2 = _restart(dirs)
    rec = mgr2.find_shard("nf")
    assert rec.state is ShardState.STAGED
    assert mgr2._blobs.ref_count(rec.blob_digest) == 1

    task_id = mgr2.submit_task([make_spec("nf", "nf.bin", data)])
    view = wait_completed(mgr2, task_id)
    assert view.shards["nf"].state is ShardState.COMPLETED
    assert (dirs[1] / "nf.bin").read_bytes() == data
    logging.getLogger("test").info(
        "判定依据 restart shard_id=nf decision=resume_flush task_id=%s", task_id,
    )


def test_corrupted_manifest_raises_instead_of_silent_ignore(manager, dirs):
    """manifest 损坏：恢复必须显式报错，而不是静默忽略/覆盖。"""
    manager.submit_task([make_spec("ok", "ok.bin", b"fine")])
    import time
    time.sleep(0.2)
    manager.shutdown()
    (dirs[0] / "shards" / "broken.json").write_text("{broken-json", encoding="utf-8")

    mgr2 = StagingManager(make_config(dirs))
    with pytest.raises(MetadataCorruptedError) as exc:
        mgr2.start()
    assert exc.value.reason == "manifest_invalid_json"
    assert exc.value.context["file"] == "broken.json"
    logging.getLogger("test").info(
        "判定依据 shard_id=broken reason=manifest_invalid_json 拒绝静默恢复",
    )


def test_blob_unreferenced_after_flush_but_state_kept(manager, dirs):
    """已落盘完成的分片：blob 无引用可淘汰，但 manifest 状态仍在。"""
    data = b"cached-copy"
    task_id = manager.submit_task([make_spec("cache", "cache.bin", data)])
    wait_completed(manager, task_id)
    rec = manager.find_shard("cache")
    assert rec.state is ShardState.COMPLETED
    assert manager._blobs.ref_count(rec.blob_digest) == 0

    manager.shutdown()
    mgr2 = _restart(dirs)
    rec2 = mgr2.find_shard("cache")
    assert rec2.state is ShardState.COMPLETED
    assert mgr2._blobs.ref_count(rec2.blob_digest) == 0
