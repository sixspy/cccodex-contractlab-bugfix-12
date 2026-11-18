
import threading
import time

import pytest
from fastapi.testclient import TestClient

import app as app_module
from app import app, init_db


@pytest.fixture(autouse=True)
def _db(tmp_path, monkeypatch):
    monkeypatch.setattr("app.DB_PATH", tmp_path / "t.db")
    init_db()
    yield
    app_module.ROLLBACK_GATE_HOLD_S = 0.0
    app_module.INGRESS_GATE_HOLD_S = 0.0


@pytest.fixture()
def client():
    return TestClient(app)


def login(client, u="admin", p="123456"):
    tok = client.post("/api/login", json={"username": u, "password": p}).json()["token"]
    return {"Authorization": f"Bearer {tok}"}


def active_formals(subject="订单入站"):
    with app_module.connect() as c:
        return [dict(r) for r in c.execute(
            "SELECT version,status FROM contracts WHERE subject=? AND track='正式' AND status='启用'",
            (subject,)).fetchall()]


def table_rows(name):
    with app_module.connect() as c:
        return [dict(r) for r in c.execute(f"SELECT * FROM {name}").fetchall()]


# ---------- 甲路径：回滚事务内停用当前正式并启用目标版 ----------

def test_rollback_single_active_pointer_transactional(client):
    h = login(client)
    r = client.post("/api/contracts/rollback", headers=h,
                    json={"subject": "订单入站", "target_version": "v1"})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["path"] == "甲"
    assert body["from_version"] == "v3" and body["to_version"] == "v1"
    # 禁止回滚后成双正式：正式指针只能有一个
    assert [x["version"] for x in active_formals()] == ["v1"]
    assert body["active_formal"] == {"订单入站": "v1"}
    # 回滚链路留痕
    ev = table_rows("rollback_events")
    assert len(ev) == 1 and ev[0]["from_version"] == "v3" and ev[0]["to_version"] == "v1"


def test_rollback_keeps_target_required_fields(client):
    h = login(client)
    r = client.post("/api/contracts/rollback", headers=h,
                    json={"subject": "订单入站", "target_version": "v1"})
    assert r.status_code == 200, r.text
    # 禁止回滚丢掉目标必填
    assert r.json()["required_fields"] == "order_id,amount"
    with app_module.connect() as c:
        row = c.execute("SELECT required_fields FROM contracts WHERE version='v1'").fetchone()
    assert row["required_fields"] == "order_id,amount"


def test_rollback_activates_subject_without_current(client):
    h = login(client)
    r = client.post("/api/contracts/rollback", headers=h,
                    json={"subject": "库存入站", "target_version": "v2"})
    assert r.status_code == 200, r.text
    assert r.json()["active_formal"] == {"库存入站": "v2"}
    assert [x["version"] for x in active_formals("库存入站")] == ["v2"]


# ---------- 偏离回滚双活册：拒绝并记双活偏了 ----------

def test_rollback_rejects_target_with_empty_required(client):
    h = login(client)
    with app_module.connect() as c:
        c.execute("INSERT INTO contracts(version,subject,mode,track,status,required_fields) "
                  "VALUES ('v0','订单入站','严格','正式','停用','')")
    r = client.post("/api/contracts/rollback", headers=h,
                    json={"subject": "订单入站", "target_version": "v0"})
    assert r.status_code == 409 and "双活偏了" in r.json()["detail"]
    assert [x["version"] for x in active_formals()] == ["v3"]  # 指针未动
    dev = table_rows("dual_active_deviations")
    assert len(dev) == 1 and dev[0]["kind"] == "双活偏了"


def test_register_tampered_rejects_and_records_deviation(client):
    h = login(client)
    with app_module.connect() as c:
        c.execute("UPDATE rollback_register SET path='乙' WHERE id=1")  # 篡改回滚双活册
    r = client.post("/api/contracts/rollback", headers=h,
                    json={"subject": "订单入站", "target_version": "v1"})
    assert r.status_code == 409 and "双活偏了" in r.json()["detail"]
    dev = table_rows("dual_active_deviations")
    assert len(dev) == 1 and dev[0]["kind"] == "双活偏了"
    assert [x["version"] for x in active_formals()] == ["v3"]


def test_rollback_rejects_shadow_unknown_current_and_cross_subject(client):
    h = login(client)
    r = client.post("/api/contracts/rollback", headers=h,
                    json={"subject": "订单入站", "target_version": "v4-shadow"})
    assert r.status_code == 400  # 影子版不可作回滚目标
    r = client.post("/api/contracts/rollback", headers=h,
                    json={"subject": "订单入站", "target_version": "v9"})
    assert r.status_code == 404
    r = client.post("/api/contracts/rollback", headers=h,
                    json={"subject": "订单入站", "target_version": "v3"})
    assert r.status_code == 409  # 目标已是当前正式
    r = client.post("/api/contracts/rollback", headers=h,
                    json={"subject": "订单入站", "target_version": "v2"})
    assert r.status_code == 400  # 目标版本不属于该主题
    assert table_rows("dual_active_deviations") == []  # 普通校验拒绝不记偏离
    assert [x["version"] for x in active_formals()] == ["v3"]


def test_validate_refuses_dual_active_and_records_deviation(client):
    h = login(client)
    with app_module.connect() as c:  # 脏数据造成双正式
        c.execute("UPDATE contracts SET status='启用' WHERE version='v1'")
    r = client.post("/api/ingress/validate", headers=h,
                    json={"subject": "订单入站", "payload_fields": "order_id,amount,currency"})
    assert r.status_code == 409 and "双活偏了" in r.json()["detail"]
    assert len(table_rows("dual_active_deviations")) == 1


# ---------- 入站与读模型只认一正式 ----------

def test_ingress_uses_single_pointer_after_rollback(client):
    h = login(client)
    client.post("/api/contracts/rollback", headers=h,
                json={"subject": "订单入站", "target_version": "v1"})
    r = client.post("/api/ingress/validate", headers=h,
                    json={"subject": "订单入站", "payload_fields": "order_id,amount"})
    assert r.json()["result"] == "通过" and r.json()["version"] == "v1"  # v1 不需要 currency


def test_read_models_recognize_single_pointer(client):
    h = login(client)
    client.post("/api/contracts/rollback", headers=h,
                json={"subject": "订单入站", "target_version": "v1"})
    b = client.post("/api/ingress/validate", headers=h,
                    json={"subject": "订单入站", "payload_fields": "order_id,amount"}).json()
    cat = client.get("/api/contracts", headers=h).json()
    assert cat["active_formal"] == {"订单入站": "v1"} and cat["dual_active"] is False
    lst = client.get("/api/ingress", headers=h).json()
    assert lst["active_formal"] == {"订单入站": "v1"}
    det = client.get(f"/api/ingress/{b['batch_id']}", headers=h).json()
    assert det["contract_version"] == "v1" and det["is_current_pointer"] is True
    rec = client.get("/api/reconcile", headers=h).json()
    assert rec["active_formal"] == {"订单入站": "v1"} and rec["dual_active"] is False
    pol = client.get("/api/policy", headers=h).json()
    assert pol["register"]["path"] == "甲" and pol["register_ok"] is True
    assert pol["active_formal"] == {"订单入站": "v1"}
    assert len(pol["rollback_events"]) == 1


def test_detail_marks_stale_pointer(client):
    h = login(client)
    b = client.post("/api/ingress/validate", headers=h,
                    json={"subject": "订单入站", "payload_fields": "order_id,amount,currency"}).json()
    client.post("/api/contracts/rollback", headers=h,
                json={"subject": "订单入站", "target_version": "v1"})
    det = client.get(f"/api/ingress/{b['batch_id']}", headers=h).json()
    assert det["contract_version"] == "v3" and det["is_current_pointer"] is False


# ---------- 回滚与入站抢写须排队，后到者只记一条回滚入站争用 ----------

def _wait_gate_locked():
    for _ in range(300):
        if app_module.GATE.locked():
            return True
        time.sleep(0.005)
    return False


def test_concurrent_rollback_and_ingress_queue_with_one_contention(client):
    h = login(client)
    app_module.ROLLBACK_GATE_HOLD_S = 0.5
    errors = []

    def do_rollback():
        try:
            app_module.rollback(app_module.RollbackIn(subject="订单入站", target_version="v1"),
                                authorization=h["Authorization"])
        except Exception as e:  # noqa
            errors.append(e)

    t = threading.Thread(target=do_rollback)
    t.start()
    assert _wait_gate_locked()
    r = client.post("/api/ingress/validate", headers=h,
                    json={"subject": "订单入站", "payload_fields": "order_id,amount,currency"})
    t.join(timeout=10)
    assert not errors and r.status_code == 200, r.text
    assert r.json()["version"] == "v1"  # 排队后用回滚后的唯一指针
    ct = table_rows("contentions")
    assert len(ct) == 1 and ct[0]["kind"] == "回滚入站争用"
    assert ct[0]["waiter"] == "ingress" and ct[0]["holder"] == "rollback"
    assert [x["version"] for x in active_formals()] == ["v1"]


def test_concurrent_ingress_then_rollback_records_one_contention(client):
    h = login(client)
    app_module.INGRESS_GATE_HOLD_S = 0.5
    errors = []

    def do_ingress():
        try:
            app_module.validate(app_module.IngressIn(subject="订单入站",
                                                     payload_fields="order_id,amount,currency"),
                                authorization=h["Authorization"])
        except Exception as e:  # noqa
            errors.append(e)

    t = threading.Thread(target=do_ingress)
    t.start()
    assert _wait_gate_locked()
    r = client.post("/api/contracts/rollback", headers=h,
                    json={"subject": "订单入站", "target_version": "v1"})
    t.join(timeout=10)
    assert not errors and r.status_code == 200, r.text
    ct = table_rows("contentions")
    assert len(ct) == 1 and ct[0]["kind"] == "回滚入站争用"
    assert ct[0]["waiter"] == "rollback" and ct[0]["holder"] == "ingress"
    assert [x["version"] for x in active_formals()] == ["v1"]
