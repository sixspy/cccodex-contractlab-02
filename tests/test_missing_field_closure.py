"""缺字段失败闭测。

口径（缺字段说明册，2026-11-17 定案，分支乙）：
- 缺字段报文只许进缓冲队列，且缓冲行必须挂批次号；
- 缺字段批次不得标通过，不得进拒收详情；
- 禁止无批次缓冲孤儿；
- 同载荷双人抢补交须排队，后到只登记缺字段争用。
本文件只认落库事实，不认提示文案。
"""

import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient

import app as contract_app
from app import app, connect, init_db


@pytest.fixture(autouse=True)
def _db(tmp_path, monkeypatch):
    monkeypatch.setattr("app.DB_PATH", tmp_path / "t.db")
    init_db()
    yield


def _login(client: TestClient, username="admin", password="123456") -> dict:
    r = client.post("/api/login", json={"username": username, "password": password})
    assert r.status_code == 200, r.text
    return {"Authorization": f"Bearer {r.json()['token']}"}


def _submit(client: TestClient, headers: dict, subject: str, fields: str) -> dict:
    r = client.post(
        "/api/ingress/validate",
        headers=headers,
        json={"subject": subject, "payload_fields": fields},
    )
    assert r.status_code == 200, r.text
    return r.json()


def test_missing_field_routes_to_buffer_only_for_strict_and_compat():
    c = TestClient(app)
    h = _login(c)

    strict = _submit(c, h, "订单入站", "order_id,amount")  # 严格正式契约 v3 缺 currency
    compat = _submit(c, h, "补采入站", "order_id,amount")  # 兼容正式契约 v5-buf 缺 currency

    assert strict["result"] == "缓冲"
    assert compat["result"] == "缓冲"
    assert strict["missing"] == ["currency"]

    with connect() as conn:
        rows = conn.execute(
            "SELECT id,result,missing_fields,detail FROM ingress_batches ORDER BY id"
        ).fetchall()
        assert [r["result"] for r in rows] == ["缓冲", "缓冲"]
        assert all(r["missing_fields"] == "currency" for r in rows)
        # 落库必须带批次挂接，不能只改提示
        for r in rows:
            buf = conn.execute(
                "SELECT batch_id,status FROM buffers WHERE batch_id=?", (r["id"],)
            ).fetchone()
            assert buf is not None, f"缺字段批次 {r['id']} 无缓冲行"
            assert buf["status"] == "待补采"
        assert conn.execute(
            "SELECT COUNT(*) c FROM ingress_batches WHERE result IN ('通过','拒收')"
        ).fetchone()["c"] == 0


def test_guard_blocks_passed_missing_and_orphan_buffer():
    """缺字段标通过或无批次缓冲孤儿必须红：拦下并记缺字段偏了。"""
    with connect() as conn:
        with pytest.raises(sqlite3.IntegrityError) as passed_err:
            conn.execute(
                "INSERT INTO ingress_batches(subject,payload_fields,contract_version,result,detail,created_at,missing_fields) "
                "VALUES('订单入站','order_id,amount','v3','通过','缺字段:currency','2026-11-17T00:00:00','currency')"
            )
        assert "缺字段偏了" in str(passed_err.value)

        with pytest.raises(sqlite3.IntegrityError) as orphan_err:
            conn.execute(
                "INSERT INTO buffers(batch_id,reason,status,created_at) VALUES(999,'孤儿','待补采','2026-11-17T00:00:00')"
            )
        assert "缺字段偏了" in str(orphan_err.value)

        assert conn.execute("SELECT COUNT(*) c FROM ingress_batches").fetchone()["c"] == 0
        assert conn.execute("SELECT COUNT(*) c FROM buffers").fetchone()["c"] == 0
        deviations = [r["message"] for r in conn.execute("SELECT message FROM missing_field_deviations")]
        assert deviations.count("缺字段偏了") >= 2


def test_guard_blocks_buffer_marked_passed_and_buffer_on_passed_batch():
    with connect() as conn:
        conn.execute(
            "INSERT INTO ingress_batches(subject,payload_fields,contract_version,result,detail,created_at,missing_fields) "
            "VALUES('订单入站','order_id,amount,currency','v3','通过','字段齐全','2026-11-17T00:00:00','')"
        )
        passed_batch = conn.execute("SELECT id FROM ingress_batches").fetchone()["id"]
        with pytest.raises(sqlite3.IntegrityError) as e1:
            conn.execute(
                "INSERT INTO buffers(batch_id,reason,status,created_at) VALUES(?,'挂通过批次','待补采','2026-11-17T00:00:00')",
                (passed_batch,),
            )
        assert "缺字段偏了" in str(e1.value)

        conn.execute(
            "INSERT INTO ingress_batches(subject,payload_fields,contract_version,result,detail,created_at,missing_fields) "
            "VALUES('订单入站','order_id,amount','v3','缓冲','缺字段:currency','2026-11-17T00:00:00','currency')"
        )
        buffered_batch = conn.execute("SELECT id FROM ingress_batches ORDER BY id DESC").fetchone()["id"]
        with pytest.raises(sqlite3.IntegrityError) as e2:
            conn.execute(
                "INSERT INTO buffers(batch_id,reason,status,created_at) VALUES(?,'缺字段进缓冲','通过','2026-11-17T00:00:00')",
                (buffered_batch,),
            )
        assert "缺字段偏了" in str(e2.value)


def test_same_payload_two_operators_queue_and_late_one_records_contention():
    """同载荷缺字段双人同时抢补交：只落一缓冲，后到只记缺字段争用。"""
    c1, c2 = TestClient(app), TestClient(app)
    c1.__enter__()
    c2.__enter__()
    try:
        h1 = _login(c1, "admin", "123456")
        h2 = _login(c2, "viewer", "view123")
        barrier = threading.Barrier(2)

        def go(headers):
            barrier.wait(timeout=10)
            return _submit(c1 if headers is h1 else c2, headers, "订单入站", "order_id,amount")

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(go, [h1, h2]))
    finally:
        c2.__exit__(None, None, None)
        c1.__exit__(None, None, None)

    assert sorted(r["result"] for r in results) == ["缓冲", "缓冲"]
    assert sum(1 for r in results if r.get("contention")) == 1, results

    with connect() as conn:
        assert conn.execute(
            "SELECT COUNT(*) c FROM ingress_batches WHERE result='缓冲'"
        ).fetchone()["c"] == 1
        assert conn.execute("SELECT COUNT(*) c FROM buffers").fetchone()["c"] == 1
        assert conn.execute(
            "SELECT COUNT(*) c FROM missing_field_contention"
        ).fetchone()["c"] == 1
        late = conn.execute("SELECT operator FROM missing_field_contention").fetchone()
        assert late["operator"] in ("admin", "viewer")
        assert conn.execute(
            "SELECT COUNT(*) c FROM missing_field_contention WHERE detail LIKE '%缺字段争用%'"
        ).fetchone()["c"] == 1


def test_reconcile_surfaces_report_one_missing_number():
    c = TestClient(app)
    h = _login(c)
    _submit(c, h, "订单入站", "order_id,amount")
    _submit(c, h, "补采入站", "order_id,amount")
    _submit(c, h, "订单入站", "order_id,amount,currency")

    r = c.get("/api/reconcile", headers=h).json()
    assert r["missing"]["total"] == 2
    assert r["missing"]["buffered"] == 2
    assert r["missing"]["rejected"] == 0
    assert r["missing"]["passed"] == 0
    assert r["missing"]["without_buffer"] == 0
    assert r["orphan_buffers"] == 0
    assert r["buffer_on_passed_batch"] == 0
    assert r["surfaces"]["buffer_queue"]["missing_buffered"] == 2
    assert r["surfaces"]["rejection_detail"]["missing_rejected"] == 0
    assert r["surfaces"]["ingress_list"]["missing_buffered"] == 2
    assert r["surfaces"]["reconcile_ledger"]["missing_buffered"] == 2
    assert r["surfaces"]["policy_card"]["missing_buffered"] == 2
    assert r["aligned"] is True

    rej = c.get("/api/rejections", headers=h).json()
    assert rej["missing_rejected"] == 0
    assert rej["items"] == []


def test_true_rejection_detail_is_separate_from_missing_count():
    c = TestClient(app)
    h = _login(c)
    with connect() as conn:
        conn.execute(
            "INSERT INTO ingress_batches(subject,payload_fields,contract_version,result,detail,created_at,missing_fields) "
            "VALUES('订单入站','order_id','v3','拒收','类型不符:amount','2026-11-17T00:00:00','')"
        )
    rej = c.get("/api/rejections", headers=h).json()
    assert rej["total"] == 1 and rej["missing_rejected"] == 0
    assert rej["items"][0]["is_missing"] is False
    r = c.get("/api/reconcile", headers=h).json()
    assert r["missing"]["rejected"] == 0
    assert r["surfaces"]["rejection_detail"]["rows"] == 1
    assert r["aligned"] is True


def test_same_operator_repeat_is_not_contention():
    """同一值班重提只算重复提交，不记缺字段争用。"""
    c = TestClient(app)
    h = _login(c)
    first = _submit(c, h, "订单入站", "order_id,amount")
    again = _submit(c, h, "订单入站", "order_id,amount")
    assert first["contention"] is False and again["contention"] is False
    assert again["batch_id"] == first["batch_id"]
    with connect() as conn:
        assert conn.execute("SELECT COUNT(*) c FROM ingress_batches").fetchone()["c"] == 1
        assert conn.execute("SELECT COUNT(*) c FROM buffers").fetchone()["c"] == 1
        assert conn.execute("SELECT COUNT(*) c FROM missing_field_contention").fetchone()["c"] == 0


def test_queue_order_is_fifo_and_verifiable():
    c = TestClient(app)
    h = _login(c)
    _submit(c, h, "订单入站", "order_id,amount")  # 缺 currency
    _submit(c, h, "订单入站", "order_id")  # 缺 amount,currency

    items = c.get("/api/buffers", headers=h).json()["items"]
    assert [i["batch_id"] for i in items] == sorted(i["batch_id"] for i in items)
    assert [i["queue_no"] for i in items] == [1, 2]
    assert items[0]["missing_fields"] == "currency"
    assert items[1]["missing_fields"] == "amount,currency"
    assert all(i["status"] == "待补采" for i in items)
    assert all(i["batch_result"] == "缓冲" for i in items)


def test_policy_card_frozen_branch_and_same_day_lock():
    c = TestClient(app)
    h = _login(c)
    p = c.get("/api/missing/policy", headers=h).json()
    assert p["branch"] == "乙"
    assert p["chosen_at"] == "2026-11-17"
    assert "缓冲" in p["rule"] and "批次号" in p["rule"]
    assert p["queue_order"].startswith("乙")

    with connect() as conn:
        with pytest.raises(sqlite3.IntegrityError) as e:
            conn.execute("UPDATE missing_field_policy SET branch='甲' WHERE id=1")
        assert "缺字段偏了" in str(e.value)
        assert conn.execute("SELECT branch FROM missing_field_policy WHERE id=1").fetchone()["branch"] == "乙"
        assert conn.execute(
            "SELECT COUNT(*) c FROM missing_field_deviations WHERE surface='missing_field_policy'"
        ).fetchone()["c"] == 1
