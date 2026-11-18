
import contextlib, hashlib, secrets, sqlite3, threading, time
from pathlib import Path
from fastapi import Header, HTTPException
from pydantic import BaseModel

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
DB_PATH = DATA / "app.db"
FRONTEND = ROOT / "frontend"
SESSIONS: dict[str, str] = {}

def connect():
    DATA.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def sha(p: str) -> str:
    return hashlib.sha256(p.encode("utf-8")).hexdigest()

def ensure_users(conn):
    conn.execute("""CREATE TABLE IF NOT EXISTS users(
        username TEXT PRIMARY KEY, password_sha TEXT NOT NULL, role TEXT NOT NULL)""")
    if conn.execute("SELECT COUNT(*) c FROM users").fetchone()["c"] == 0:
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("admin", sha("123456"), "管理员"))
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("viewer", sha("view123"), "只读"))

class LoginIn(BaseModel):
    username: str
    password: str

def require_user(authorization: str | None) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "未登录")
    u = SESSIONS.get(authorization.removeprefix("Bearer ").strip())
    if not u:
        raise HTTPException(401, "登录已失效")
    return u

from datetime import datetime
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

PRODUCT = "入站契约"
SKELETON = "Schema 契约版本约束入站校验，破坏性变更进缓冲或拒收，并与落库结果对账"

# ---- 回滚双活册（选定路径甲，2026-11-18 选定，当天不许再换；详见 docs/回滚双活册.md）----
REGISTER_PATH = "甲"           # 甲路径：回滚事务内停用当前正式并启用目标版（不混用乙路径历史快照）
REGISTER_DECIDED_ON = "2026-11-18"
REGISTER_NOTE = "回滚事务内停用当前正式并启用目标版；正式指针每主题唯一；选定当天不许再换"
POLICY_RULES = [
    "回滚后正式指针只能有一个（每主题）",
    "甲路径：回滚事务内停用当前正式并启用目标版，选定当天不许再换",
    "禁止回滚丢掉目标必填",
    "回滚与入站两名值班同时抢写须排队，后到者只记一条回滚入站争用",
    "偏离回滚双活册则拒绝并记双活偏了",
]

# 回滚与入站两名值班的写闸门：同时抢写须排队，后到者记一条争用
GATE = threading.Lock()
GATE_HOLDER: str | None = None
ROLLBACK_GATE_HOLD_S = 0.0  # 测试钩子：回滚持闸时长
INGRESS_GATE_HOLD_S = 0.0   # 测试钩子：入站持闸时长


@contextlib.contextmanager
def write_gate(who: str):
    """排队进写区；若发现对方（回滚/入站）正持闸，则作为后到者记一条回滚入站争用。"""
    global GATE_HOLDER
    waited = None
    if GATE.locked():
        holder = GATE_HOLDER
        if holder and holder != who and {holder, who} == {"rollback", "ingress"}:
            waited = (who, holder)
    with GATE:
        GATE_HOLDER = who
        try:
            yield waited
        finally:
            GATE_HOLDER = None


class Deviation(Exception):
    """偏离回滚双活册：拒绝并记双活偏了。"""


def record_deviation(detail: str):
    now = datetime.now().isoformat(timespec="seconds")
    with connect() as c:
        c.execute("INSERT INTO dual_active_deviations(kind,detail,created_at) VALUES (?,?,?)",
                  ("双活偏了", detail, now))


def active_pointer_map(c):
    """正式指针：每主题恰一个启用正式才认；多于一个即双活。"""
    rows = c.execute("SELECT subject,version FROM contracts WHERE track='正式' AND status='启用'").fetchall()
    by_subject: dict[str, list[str]] = {}
    for r in rows:
        by_subject.setdefault(r["subject"], []).append(r["version"])
    single = {s: v[0] for s, v in by_subject.items() if len(v) == 1}
    duals = sorted(s for s, v in by_subject.items() if len(v) > 1)
    return single, duals

app = FastAPI(title=PRODUCT)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

def init_db():
    with connect() as conn:
        ensure_users(conn)
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS contracts(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          version TEXT UNIQUE NOT NULL,
          subject TEXT NOT NULL,
          mode TEXT NOT NULL,
          track TEXT NOT NULL,
          status TEXT NOT NULL,
          required_fields TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS ingress_batches(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          subject TEXT NOT NULL,
          payload_fields TEXT NOT NULL,
          contract_version TEXT,
          result TEXT NOT NULL,
          detail TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS buffers(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          batch_id INTEGER NOT NULL,
          reason TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS rollback_register(
          id INTEGER PRIMARY KEY CHECK(id=1),
          path TEXT NOT NULL,
          decided_on TEXT NOT NULL,
          note TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS rollback_events(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          subject TEXT NOT NULL,
          from_version TEXT,
          to_version TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS contentions(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          kind TEXT NOT NULL,
          waiter TEXT NOT NULL,
          holder TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS dual_active_deviations(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          kind TEXT NOT NULL,
          detail TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        """)
        if conn.execute("SELECT COUNT(*) c FROM rollback_register").fetchone()["c"] == 0:
            conn.execute("INSERT INTO rollback_register(id,path,decided_on,note) VALUES (1,?,?,?)",
                         (REGISTER_PATH, REGISTER_DECIDED_ON, REGISTER_NOTE))
        if conn.execute("SELECT COUNT(*) c FROM contracts").fetchone()["c"] == 0:
            conn.execute("INSERT INTO contracts(version,subject,mode,track,status,required_fields) VALUES (?,?,?,?,?,?)",
                         ("v1","订单入站","严格","正式","停用","order_id,amount"))
            conn.execute("INSERT INTO contracts(version,subject,mode,track,status,required_fields) VALUES (?,?,?,?,?,?)",
                         ("v3","订单入站","严格","正式","启用","order_id,amount,currency"))
            conn.execute("INSERT INTO contracts(version,subject,mode,track,status,required_fields) VALUES (?,?,?,?,?,?)",
                         ("v4-shadow","订单入站","兼容","影子","试跑","order_id,amount,currency,channel"))
            conn.execute("INSERT INTO contracts(version,subject,mode,track,status,required_fields) VALUES (?,?,?,?,?,?)",
                         ("v2","库存入站","严格","正式","停用","sku,qty"))

@app.on_event("startup")
def _s():
    init_db()

@app.get("/api/health")
def health():
    with connect() as c:
        n = c.execute("SELECT COUNT(*) c FROM contracts").fetchone()["c"]
    return {"ok": True, "product": PRODUCT, "skeleton": SKELETON, "contracts": n, "db": str(DB_PATH)}

@app.post("/api/login")
def login(body: LoginIn):
    with connect() as c:
        row = c.execute("SELECT username,role FROM users WHERE username=? AND password_sha=?", (body.username, sha(body.password))).fetchone()
    if not row: raise HTTPException(401, "账号或密码错误")
    tok = secrets.token_hex(16); SESSIONS[tok]=row["username"]
    return {"token": tok, "username": row["username"], "role": row["role"]}

@app.get("/api/contracts")
def contracts(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        items = [dict(r) for r in c.execute("SELECT * FROM contracts ORDER BY id").fetchall()]
        single, duals = active_pointer_map(c)
    return {"items": items, "active_formal": single, "dual_active": bool(duals), "dual_active_subjects": duals}

class RollbackIn(BaseModel):
    subject: str
    target_version: str

@app.post("/api/contracts/rollback")
def rollback(body: RollbackIn, authorization: str | None = Header(default=None)):
    require_user(authorization)
    try:
        with write_gate("rollback") as waited:
            if ROLLBACK_GATE_HOLD_S:
                time.sleep(ROLLBACK_GATE_HOLD_S)
            with connect() as c:
                reg = c.execute("SELECT * FROM rollback_register").fetchall()
                if len(reg) != 1 or reg[0]["path"] != REGISTER_PATH:
                    raise Deviation("回滚双活册缺失或被改（应为单条甲路径）")
                target = c.execute("SELECT * FROM contracts WHERE version=?", (body.target_version,)).fetchone()
                if not target:
                    raise HTTPException(404, "回滚目标版本不存在")
                if target["subject"] != body.subject:
                    raise HTTPException(400, "目标版本不属于该主题")
                if target["track"] != "正式":
                    raise HTTPException(400, "影子版不可作回滚目标")
                if not target["required_fields"].strip():
                    raise Deviation(f"禁止回滚丢掉目标必填：{target['version']} 必填为空")
                current = c.execute(
                    "SELECT * FROM contracts WHERE subject=? AND track='正式' AND status='启用'",
                    (body.subject,)).fetchone()
                if current and current["version"] == target["version"]:
                    raise HTTPException(409, "目标已是当前正式指针")
                # 甲路径：同一事务内停用当前正式并启用目标版，原子切换正式指针
                if current:
                    c.execute("UPDATE contracts SET status='停用' WHERE id=?", (current["id"],))
                c.execute("UPDATE contracts SET status='启用' WHERE id=?", (target["id"],))
                single, duals = active_pointer_map(c)
                if body.subject in duals or single.get(body.subject) != target["version"]:
                    raise Deviation("回滚后正式指针不唯一")
                now = datetime.now().isoformat(timespec="seconds")
                c.execute("INSERT INTO rollback_events(subject,from_version,to_version,created_at) VALUES (?,?,?,?)",
                          (body.subject, current["version"] if current else None, target["version"], now))
                if waited:
                    c.execute("INSERT INTO contentions(kind,waiter,holder,created_at) VALUES (?,?,?,?)",
                              ("回滚入站争用", waited[0], waited[1], now))
                return {"ok": True, "path": REGISTER_PATH, "subject": body.subject,
                        "from_version": current["version"] if current else None,
                        "to_version": target["version"],
                        "required_fields": target["required_fields"],
                        "active_formal": {body.subject: target["version"]},
                        "contention_recorded": bool(waited)}
    except Deviation as d:
        record_deviation(str(d))
        raise HTTPException(409, f"偏离回滚双活册：{d}；已记双活偏了")

class IngressIn(BaseModel):
    subject: str
    payload_fields: str  # comma fields present

@app.post("/api/ingress/validate")
def validate(body: IngressIn, authorization: str | None = Header(default=None)):
    require_user(authorization)
    fields = {x.strip() for x in body.payload_fields.split(",") if x.strip()}
    try:
        with write_gate("ingress") as waited:
            if INGRESS_GATE_HOLD_S:
                time.sleep(INGRESS_GATE_HOLD_S)
            with connect() as c:
                single, duals = active_pointer_map(c)
                if body.subject in duals:
                    raise Deviation(f"主题 {body.subject} 出现双正式，入站拒裁")
                if body.subject not in single:
                    raise HTTPException(404, "无启用正式契约")
                ctr = c.execute("SELECT * FROM contracts WHERE subject=? AND track='正式' AND status='启用'",
                                (body.subject,)).fetchone()
                need = {x.strip() for x in ctr["required_fields"].split(",") if x.strip()}
                missing = sorted(need - fields)
                now = datetime.now().isoformat(timespec="seconds")
                if waited:
                    c.execute("INSERT INTO contentions(kind,waiter,holder,created_at) VALUES (?,?,?,?)",
                              ("回滚入站争用", waited[0], waited[1], now))
                if missing:
                    if ctr["mode"] == "兼容":
                        cur = c.execute("INSERT INTO ingress_batches(subject,payload_fields,contract_version,result,detail,created_at) VALUES (?,?,?,?,?,?)",
                                        (body.subject, body.payload_fields, ctr["version"], "缓冲", "缺字段:"+ ",".join(missing), now))
                        c.execute("INSERT INTO buffers(batch_id,reason) VALUES (?,?)", (cur.lastrowid, "缺字段进双写缓冲"))
                        return {"result":"缓冲", "version": ctr["version"], "missing": missing, "batch_id": cur.lastrowid}
                    cur = c.execute("INSERT INTO ingress_batches(subject,payload_fields,contract_version,result,detail,created_at) VALUES (?,?,?,?,?,?)",
                                    (body.subject, body.payload_fields, ctr["version"], "拒收", "缺字段:"+ ",".join(missing), now))
                    return {"result":"拒收", "version": ctr["version"], "missing": missing, "batch_id": cur.lastrowid}
                cur = c.execute("INSERT INTO ingress_batches(subject,payload_fields,contract_version,result,detail,created_at) VALUES (?,?,?,?,?,?)",
                                (body.subject, body.payload_fields, ctr["version"], "通过", "字段齐全", now))
                return {"result":"通过", "version": ctr["version"], "batch_id": cur.lastrowid}
    except Deviation as d:
        record_deviation(str(d))
        raise HTTPException(409, f"偏离回滚双活册：{d}；已记双活偏了")

@app.get("/api/ingress")
def ingress(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        items = [dict(r) for r in c.execute("SELECT * FROM ingress_batches ORDER BY id DESC").fetchall()]
        single, duals = active_pointer_map(c)
    return {"items": items, "active_formal": single, "dual_active": bool(duals)}

@app.get("/api/ingress/{batch_id}")
def ingress_detail(batch_id: int, authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        row = c.execute("SELECT * FROM ingress_batches WHERE id=?", (batch_id,)).fetchone()
        if not row:
            raise HTTPException(404, "批次不存在")
        single, duals = active_pointer_map(c)
    item = dict(row)
    pointer = single.get(item["subject"])
    return {"item": item, "contract_version": item["contract_version"],
            "active_formal": single, "dual_active": bool(duals),
            "current_pointer": pointer,
            "is_current_pointer": bool(pointer) and item["contract_version"] == pointer}

@app.get("/api/buffers")
def buffers(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        rows = c.execute("""SELECT b.*, i.subject, i.payload_fields FROM buffers b JOIN ingress_batches i ON i.id=b.batch_id ORDER BY b.id DESC""").fetchall()
        return {"items":[dict(r) for r in rows]}

@app.get("/api/reconcile")
def reconcile(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        total = c.execute("SELECT COUNT(*) c FROM ingress_batches").fetchone()["c"]
        passed = c.execute("SELECT COUNT(*) c FROM ingress_batches WHERE result='通过'").fetchone()["c"]
        buffered = c.execute("SELECT COUNT(*) c FROM ingress_batches WHERE result='缓冲'").fetchone()["c"]
        rejected = c.execute("SELECT COUNT(*) c FROM ingress_batches WHERE result='拒收'").fetchone()["c"]
        buf_rows = c.execute("SELECT COUNT(*) c FROM buffers").fetchone()["c"]
        single, duals = active_pointer_map(c)
    return {"ingress_total": total, "passed": passed, "buffered": buffered, "rejected": rejected,
            "buffer_rows": buf_rows, "aligned": buffered == buf_rows,
            "active_formal": single, "dual_active": bool(duals), "dual_active_subjects": duals}

@app.get("/api/policy")
def policy(authorization: str | None = Header(default=None)):
    """策略附页：回滚双活册落库内容 + 回滚链路 + 争用 + 双活偏了，只认一正式。"""
    require_user(authorization)
    with connect() as c:
        reg = [dict(r) for r in c.execute("SELECT * FROM rollback_register").fetchall()]
        single, duals = active_pointer_map(c)
        events = [dict(r) for r in c.execute("SELECT * FROM rollback_events ORDER BY id DESC LIMIT 10").fetchall()]
        contentions = [dict(r) for r in c.execute("SELECT * FROM contentions ORDER BY id DESC LIMIT 10").fetchall()]
        deviations = [dict(r) for r in c.execute("SELECT * FROM dual_active_deviations ORDER BY id DESC LIMIT 10").fetchall()]
    register_ok = len(reg) == 1 and reg[0]["path"] == REGISTER_PATH
    return {"register": reg[0] if len(reg) == 1 else None, "register_ok": register_ok,
            "rules": POLICY_RULES,
            "active_formal": single, "dual_active": bool(duals), "dual_active_subjects": duals,
            "rollback_events": events, "contentions": contentions, "deviations": deviations}

@app.get("/")
def index():
    return FileResponse(FRONTEND / "index.html")
app.mount("/static", StaticFiles(directory=str(FRONTEND)), name="static")
