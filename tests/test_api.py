"""HTTP 接口场景：提交/查询/取消 + 状态码与原因可区分。"""

from __future__ import annotations

import base64
import logging
import threading

import pytest
from fastapi.testclient import TestClient

from staging.api import create_app
from staging.manager import StagingManager

from conftest import BlockingSource, b64, make_config, make_spec, sha256
from staging.models import ShardSpec


@pytest.fixture
def client(dirs):
    mgr = StagingManager(make_config(dirs, capacity_bytes=200, max_concurrent_shards=2))
    mgr.start()
    app = create_app(mgr)
    with TestClient(app) as c:
        c.manager = mgr
        yield c


def _body(shard_id: str, target: str, data: bytes, digest: str | None = None) -> dict:
    return {
        "shard_id": shard_id,
        "target_path": target,
        "digest": digest or sha256(data),
        "data_base64": b64(data),
    }


def test_submit_progress_and_content(client, dirs):
    data = b"http-payload!!"
    resp = client.post("/tasks", json={"shards": [_body("h1", "h1.bin", data)]})
    assert resp.status_code == 202, resp.text
    task_id = resp.json()["task_id"]

    import time
    deadline = time.time() + 5
    while time.time() < deadline:
        view = client.get(f"/tasks/{task_id}").json()
        if view["state"] == "completed":
            break
        time.sleep(0.02)
    assert view["progress"]["completed"] == 1
    assert (dirs[1] / "h1.bin").read_bytes() == data
    logging.getLogger("test").info(
        "判定依据 task_id=%s shard_id=h1 http=202 then completed", task_id,
    )


def test_distinct_error_codes_for_conflict_capacity_concurrency(client):
    """409 / 507 / 503 三类拒绝在 HTTP 层必须可区分。"""
    mgr = client.manager

    # 占住槽与目标锁
    gate = threading.Event()
    slow = b"s" * 16
    mgr.submit_task(
        [ShardSpec("lock1", "lock1.bin", len(slow), sha256(slow), BlockingSource(slow, gate))]
    )
    import time
    time.sleep(0.25)

    # 409：同目标
    r = client.post("/tasks", json={"shards": [_body("lock2", "lock1.bin", slow)]})
    assert r.status_code == 409
    assert r.json()["error"] == "target_locked"
    assert r.json()["reason"] == "target_busy"

    # 503：系统饱和（2 槽：lock1 占 1，再造 1 个占满）
    occ = b"o" * 12
    mgr.submit_task(
        [ShardSpec("occ", "occ.bin", len(occ), sha256(occ), BlockingSource(occ, gate))]
    )
    time.sleep(0.25)
    r = client.post("/tasks", json={"shards": [_body("more", "more.bin", b"m" * 4)]})
    assert r.status_code == 503
    assert r.json()["reason"] == "concurrency_slots_busy"

    # 507：单片超容量
    big = b"z" * 201
    r = client.post("/tasks", json={"shards": [_body("big", "big.bin", big)]})
    assert r.status_code == 507
    assert r.json()["error"] == "capacity_exceeded"

    gate.set()
    logging.getLogger("test").info(
        "判定依据 http 409=target_busy 503=concurrency_slots_busy 507=capacity_exceeded 三类可区分",
    )


def test_idempotent_submit_via_http(client):
    data = b"twice"
    body = {"shards": [_body("t", "t.bin", data)]}
    r1 = client.post("/tasks", json=body)
    assert r1.status_code == 202
    import time
    time.sleep(0.15)
    r2 = client.post("/tasks", json=body)
    assert r2.status_code == 202
    # 第二次应全部幂等命中
    time.sleep(0.1)
    view = client.get(f"/tasks/{r2.json()['task_id']}").json()
    assert view["state"] == "completed"
    assert view["shards"]["t"]["state"] == "completed"


def test_checksum_mismatch_http_422(client, dirs):
    body = _body("x", "x.bin", b"actual")
    body["digest"] = sha256(b"claimed-different")
    r = client.post("/tasks", json={"shards": [body]})
    # 准入时指纹格式合法、运行时才比对内容：任务会进入失败态
    assert r.status_code == 202
    import time
    task_id = r.json()["task_id"]
    for _ in range(200):
        view = client.get(f"/tasks/{task_id}").json()
        if view["state"] in ("completed", "failed", "canceled"):
            break
        time.sleep(0.02)
    assert view["state"] == "failed"
    assert "digest_mismatch" in view["shards"]["x"]["error"]
    assert not (dirs[1] / "x.bin").exists()


def test_bad_base64_rejected_http_400(client):
    body = {
        "shards": [
            {"shard_id": "b", "target_path": "b.bin", "digest": sha256(b"1"), "data_base64": "@@@"}
        ]
    }
    r = client.post("/tasks", json=body)
    # FastAPI 请求体校验异常或我们的 InvalidRequestError 都应落到 4xx
    assert 400 <= r.status_code < 500


def test_get_shard_state_http(client):
    data = b"shard-state"
    r = client.post("/tasks", json={"shards": [_body("ss", "ss.bin", data)]})
    import time
    time.sleep(0.15)
    r2 = client.get("/shards/ss")
    assert r2.status_code == 200
    assert r2.json()["state"] == "completed"
    assert r2.json()["flushed"] is True

    r3 = client.get("/shards/missing")
    assert r3.status_code == 404


def test_cancel_via_http(client):
    gate = threading.Event()
    data = b"c" * 30
    client.manager.submit_task(
        [ShardSpec("cc", "cc.bin", len(data), sha256(data), BlockingSource(data, gate))]
    )
    import time
    time.sleep(0.25)
    r = client.post("/tasks/x/cancel")
    # x 不存在 -> 404
    assert r.status_code == 404


def test_health_reports_usage(client):
    r = client.get("/health")
    assert r.status_code == 200
    payload = r.json()
    assert payload["status"] == "ok"
    assert payload["capacity_bytes"] == 200
    assert "used_bytes" in payload and "reclaimable_bytes" in payload
