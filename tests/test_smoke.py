import pytest
from fastapi.testclient import TestClient
from app import app, init_db

@pytest.fixture(autouse=True)
def _db(tmp_path, monkeypatch):
    monkeypatch.setattr("app.DB_PATH", tmp_path/"t.db"); init_db(); yield

def test_validate_paths():
    c = TestClient(app)
    assert c.get("/api/health").json()["product"] == "入站契约"
    tok = c.post("/api/login", json={"username":"admin","password":"123456"}).json()["token"]
    h = {"Authorization": f"Bearer {tok}"}
    # 缺字段一律进缓冲队列（乙路径），严格契约也不得拒收
    r = c.post("/api/ingress/validate", headers=h, json={"subject":"订单入站","payload_fields":"order_id,amount"})
    assert r.status_code == 200 and r.json()["result"] == "缓冲"
    assert r.json()["batch_id"] > 0
    ok = c.post("/api/ingress/validate", headers=h, json={"subject":"订单入站","payload_fields":"order_id,amount,currency"})
    assert ok.json()["result"] == "通过"
    q = c.get("/api/reconcile", headers=h).json()
    assert q["passed"] >= 1
    assert q["missing"]["buffered"] == 1 and q["missing"]["rejected"] == 0
    assert q["orphan_buffers"] == 0 and q["aligned"] is True
