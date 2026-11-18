import hashlib, secrets, sqlite3
from datetime import date, datetime
from pathlib import Path

from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
DB_PATH = DATA / "app.db"
FRONTEND = ROOT / "frontend"
SESSIONS: dict[str, str] = {}

PRODUCT = "入站契约"
SKELETON = "Schema 契约版本约束入站校验，破坏性变更进缓冲或拒收，并与落库结果对账"

# 缺字段说明册（2026-11-17 定案）：全仓只走乙路径。
# 甲路径（缺字段与拒收批次同一事务且不写缓冲）不采用，禁止与本路径混用。
MISSING_BRANCH = "乙"
MISSING_RULE = "缺字段只进缓冲队列：先落缓冲批次并挂批次号，缓冲行待补采，不得标通过、不得进拒收详情"
MISSING_QUEUE_ORDER = "乙：批次先落 ingress_batches(result=缓冲) 取批次号，再以该批次号写 buffers 缓冲行；按 batch_id 先到先补"


def connect():
    DATA.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


def sha(p: str) -> str:
    return hashlib.sha256(p.encode("utf-8")).hexdigest()


def today() -> str:
    return date.today().isoformat()


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def parse_fields(raw: str) -> set[str]:
    return {x.strip() for x in raw.split(",") if x.strip()}


def payload_hash(subject: str, fields: set[str]) -> str:
    return sha(subject + "|" + ",".join(sorted(fields)))


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


app = FastAPI(title=PRODUCT)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])


def _ensure_columns(conn, table: str, columns: dict[str, str]):
    existing = {r["name"] for r in conn.execute(f"PRAGMA table_info({table})")}
    for name, ddl in columns.items():
        if name not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {ddl}")


def _migrate_legacy_missing(conn):
    """把历史缺字段行就地并入乙路径；库文件路径不变。"""
    now = now_iso()
    for r in conn.execute("SELECT * FROM ingress_batches").fetchall():
        missing = r["missing_fields"] or ""
        if not missing and r["detail"].startswith("缺字段:"):
            missing = ",".join(sorted(parse_fields(r["detail"].removeprefix("缺字段:"))))
        ph = r["payload_hash"] or payload_hash(r["subject"], parse_fields(r["payload_fields"]))
        result = "缓冲" if missing and r["result"] != "缓冲" else r["result"]
        conn.execute(
            "UPDATE ingress_batches SET missing_fields=?, payload_hash=?, result=? WHERE id=?",
            (missing, ph, result, r["id"]),
        )
        if missing:
            conn.execute(
                "INSERT INTO buffers(batch_id,reason,status,created_at) "
                "SELECT ?,?,?,? WHERE NOT EXISTS (SELECT 1 FROM buffers WHERE batch_id=?)",
                (r["id"], "缺字段进缓冲队列", "待补采", r["created_at"] or now, r["id"]),
            )
    conn.execute("UPDATE buffers SET status='待补采' WHERE status IS NULL OR status=''")
    conn.execute(
        "UPDATE buffers SET created_at=(SELECT created_at FROM ingress_batches WHERE id=buffers.batch_id) "
        "WHERE created_at IS NULL OR created_at=''"
    )


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
          created_at TEXT NOT NULL,
          missing_fields TEXT NOT NULL DEFAULT '',
          payload_hash TEXT NOT NULL DEFAULT '',
          operator TEXT NOT NULL DEFAULT ''
        );
        CREATE TABLE IF NOT EXISTS buffers(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          batch_id INTEGER NOT NULL,
          reason TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT '待补采',
          created_at TEXT NOT NULL DEFAULT ''
        );
        CREATE TABLE IF NOT EXISTS missing_field_policy(
          id INTEGER PRIMARY KEY CHECK (id=1),
          branch TEXT NOT NULL,
          chosen_at TEXT NOT NULL,
          rule TEXT NOT NULL,
          queue_order TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS missing_field_deviations(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          surface TEXT NOT NULL,
          message TEXT NOT NULL,
          detail TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS missing_field_contention(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          subject TEXT NOT NULL,
          payload_hash TEXT NOT NULL,
          batch_id INTEGER NOT NULL,
          operator TEXT NOT NULL,
          detail TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        """)
        _ensure_columns(conn, "ingress_batches", {
            "missing_fields": "TEXT NOT NULL DEFAULT ''",
            "payload_hash": "TEXT NOT NULL DEFAULT ''",
            "operator": "TEXT NOT NULL DEFAULT ''",
        })
        _ensure_columns(conn, "buffers", {
            "status": "TEXT NOT NULL DEFAULT '待补采'",
            "created_at": "TEXT NOT NULL DEFAULT ''",
        })
        conn.executescript("""
        CREATE INDEX IF NOT EXISTS ix_ingress_payload ON ingress_batches(subject,payload_hash);
        CREATE INDEX IF NOT EXISTS ix_missing_contention ON missing_field_contention(subject,payload_hash);
        """)
        _migrate_legacy_missing(conn)
        if conn.execute("SELECT COUNT(*) c FROM missing_field_policy").fetchone()["c"] == 0:
            conn.execute(
                "INSERT INTO missing_field_policy(branch,chosen_at,rule,queue_order) VALUES(?,?,?,?)",
                (MISSING_BRANCH, today(), MISSING_RULE, MISSING_QUEUE_ORDER),
            )
        else:
            conn.execute(
                "UPDATE missing_field_policy SET rule=?, queue_order=? WHERE id=1",
                (MISSING_RULE, MISSING_QUEUE_ORDER),
            )
        # 守卫最后建：迁移与种子行不被误拦。任何偏离说明册的写入都记“缺字段偏了”并拦下。
        conn.executescript("""
        CREATE TRIGGER IF NOT EXISTS trg_missing_never_passed_ins
        BEFORE INSERT ON ingress_batches
        WHEN NEW.missing_fields <> '' AND NEW.result IN ('通过','拒收')
        BEGIN
          INSERT INTO missing_field_deviations(surface,message,detail,created_at)
            VALUES('ingress_batches','缺字段偏了',
                   '缺字段批次写入了 '||NEW.result||'：'||NEW.subject||' / '||NEW.missing_fields,
                   datetime('now','localtime'));
          SELECT RAISE(FAIL,'缺字段偏了');
        END;

        CREATE TRIGGER IF NOT EXISTS trg_missing_never_passed_upd
        BEFORE UPDATE ON ingress_batches
        WHEN NEW.missing_fields <> '' AND NEW.result IN ('通过','拒收')
        BEGIN
          INSERT INTO missing_field_deviations(surface,message,detail,created_at)
            VALUES('ingress_batches','缺字段偏了',
                   '缺字段批次被改成 '||NEW.result||'：'||NEW.subject||' / '||NEW.missing_fields,
                   datetime('now','localtime'));
          SELECT RAISE(FAIL,'缺字段偏了');
        END;

        CREATE TRIGGER IF NOT EXISTS trg_buffer_integrity_ins
        BEFORE INSERT ON buffers
        WHEN NEW.status = '通过'
          OR NOT EXISTS (SELECT 1 FROM ingress_batches WHERE id = NEW.batch_id)
          OR EXISTS (SELECT 1 FROM ingress_batches WHERE id = NEW.batch_id AND result = '通过')
        BEGIN
          INSERT INTO missing_field_deviations(surface,message,detail,created_at)
            VALUES('buffers','缺字段偏了',
                   '缓冲行越界：batch='||COALESCE(NEW.batch_id,0)||' status='||NEW.status,
                   datetime('now','localtime'));
          SELECT RAISE(FAIL,'缺字段偏了');
        END;

        CREATE TRIGGER IF NOT EXISTS trg_buffer_integrity_upd
        BEFORE UPDATE ON buffers
        WHEN NEW.status = '通过'
          OR NOT EXISTS (SELECT 1 FROM ingress_batches WHERE id = NEW.batch_id)
          OR EXISTS (SELECT 1 FROM ingress_batches WHERE id = NEW.batch_id AND result = '通过')
        BEGIN
          INSERT INTO missing_field_deviations(surface,message,detail,created_at)
            VALUES('buffers','缺字段偏了',
                   '缓冲行被改成越界态：batch='||COALESCE(NEW.batch_id,0)||' status='||NEW.status,
                   datetime('now','localtime'));
          SELECT RAISE(FAIL,'缺字段偏了');
        END;

        CREATE TRIGGER IF NOT EXISTS trg_buffer_delete_guard
        BEFORE DELETE ON buffers
        WHEN EXISTS (SELECT 1 FROM ingress_batches WHERE id = OLD.batch_id AND missing_fields <> '')
        BEGIN
          INSERT INTO missing_field_deviations(surface,message,detail,created_at)
            VALUES('buffers','缺字段偏了',
                   '删缓冲行会让缺字段批次失去批次挂接：batch='||OLD.batch_id,
                   datetime('now','localtime'));
          SELECT RAISE(FAIL,'缺字段偏了');
        END;

        CREATE TRIGGER IF NOT EXISTS trg_policy_lock
        BEFORE UPDATE OF branch ON missing_field_policy
        WHEN NEW.branch <> OLD.branch AND date('now','localtime') = OLD.chosen_at
        BEGIN
          INSERT INTO missing_field_deviations(surface,message,detail,created_at)
            VALUES('missing_field_policy','缺字段偏了',
                   '缺字段说明册定案当天禁止改口：'||OLD.branch||' -> '||NEW.branch,
                   datetime('now','localtime'));
          SELECT RAISE(FAIL,'缺字段偏了');
        END;
        """)
        if conn.execute("SELECT COUNT(*) c FROM contracts").fetchone()["c"] == 0:
            conn.execute("INSERT INTO contracts(version,subject,mode,track,status,required_fields) VALUES (?,?,?,?,?,?)",
                         ("v3", "订单入站", "严格", "正式", "启用", "order_id,amount,currency"))
            conn.execute("INSERT INTO contracts(version,subject,mode,track,status,required_fields) VALUES (?,?,?,?,?,?)",
                         ("v4-shadow", "订单入站", "兼容", "影子", "试跑", "order_id,amount,currency,channel"))
            conn.execute("INSERT INTO contracts(version,subject,mode,track,status,required_fields) VALUES (?,?,?,?,?,?)",
                         ("v2", "库存入站", "严格", "正式", "停用", "sku,qty"))
            conn.execute("INSERT INTO contracts(version,subject,mode,track,status,required_fields) VALUES (?,?,?,?,?,?)",
                         ("v5-buf", "补采入站", "兼容", "正式", "启用", "order_id,amount,currency"))


@app.on_event("startup")
def _s():
    init_db()


def _surface_counts(conn) -> dict:
    missing_total = conn.execute(
        "SELECT COUNT(*) c FROM ingress_batches WHERE missing_fields <> ''").fetchone()["c"]
    buffered = conn.execute(
        "SELECT COUNT(*) c FROM ingress_batches WHERE missing_fields <> '' AND result='缓冲'").fetchone()["c"]
    rejected = conn.execute(
        "SELECT COUNT(*) c FROM ingress_batches WHERE missing_fields <> '' AND result='拒收'").fetchone()["c"]
    passed = conn.execute(
        "SELECT COUNT(*) c FROM ingress_batches WHERE missing_fields <> '' AND result='通过'").fetchone()["c"]
    return {
        "missing_total": missing_total,
        "missing_buffered": buffered,
        "missing_rejected": rejected,
        "missing_passed": passed,
    }


def _closure_counts(conn) -> dict:
    without_buffer = conn.execute(
        """SELECT COUNT(*) c FROM ingress_batches i
           WHERE i.missing_fields <> ''
             AND NOT EXISTS (SELECT 1 FROM buffers b WHERE b.batch_id=i.id)""").fetchone()["c"]
    orphan_buffers = conn.execute(
        """SELECT COUNT(*) c FROM buffers b
           WHERE NOT EXISTS (SELECT 1 FROM ingress_batches i WHERE i.id=b.batch_id)""").fetchone()["c"]
    buffer_on_passed = conn.execute(
        """SELECT COUNT(*) c FROM buffers b JOIN ingress_batches i ON i.id=b.batch_id
           WHERE i.result='通过'""").fetchone()["c"]
    buffer_passed_status = conn.execute(
        "SELECT COUNT(*) c FROM buffers WHERE status='通过'").fetchone()["c"]
    unlabeled = conn.execute(
        "SELECT COUNT(*) c FROM buffers WHERE batch_id IS NULL OR batch_id <= 0").fetchone()["c"]
    contention = conn.execute(
        "SELECT COUNT(*) c FROM missing_field_contention").fetchone()["c"]
    deviations = conn.execute(
        "SELECT COUNT(*) c FROM missing_field_deviations").fetchone()["c"]
    return {
        "without_buffer": without_buffer,
        "orphan_buffers": orphan_buffers,
        "buffer_on_passed_batch": buffer_on_passed,
        "buffer_passed_status": buffer_passed_status,
        "unlabeled_buffers": unlabeled,
        "contention": contention,
        "deviations": deviations,
    }


def _surface_payload(conn) -> tuple[dict, dict]:
    ledger = _surface_counts(conn)
    closure = _closure_counts(conn)
    rows = conn.execute("SELECT COUNT(*) c FROM buffers").fetchone()["c"]
    rejection_rows = conn.execute(
        "SELECT COUNT(*) c FROM ingress_batches WHERE result='拒收' AND missing_fields=''").fetchone()["c"]
    surfaces = {
        "buffer_queue": {**ledger, "rows": rows},
        "rejection_detail": {**ledger, "rows": rejection_rows},
        "ingress_list": {**ledger},
        "reconcile_ledger": {**ledger},
        "policy_card": {**ledger},
    }
    closure["aligned"] = bool(
        ledger["missing_total"] == ledger["missing_buffered"] + ledger["missing_rejected"] + ledger["missing_passed"]
        and ledger["missing_passed"] == 0
        and ledger["missing_rejected"] == 0
        and closure["without_buffer"] == 0
        and closure["orphan_buffers"] == 0
        and closure["buffer_on_passed_batch"] == 0
        and closure["buffer_passed_status"] == 0
        and closure["unlabeled_buffers"] == 0
        and all(
            s["missing_total"] == ledger["missing_total"]
            and s["missing_buffered"] == ledger["missing_buffered"]
            and s["missing_rejected"] == ledger["missing_rejected"]
            and s["missing_passed"] == ledger["missing_passed"]
            for s in surfaces.values()
        )
    )
    return ledger, {"surfaces": surfaces, **closure}


@app.get("/api/health")
def health():
    with connect() as c:
        n = c.execute("SELECT COUNT(*) c FROM contracts").fetchone()["c"]
    return {"ok": True, "product": PRODUCT, "skeleton": SKELETON,
            "contracts": n, "db": str(DB_PATH), "missing_branch": MISSING_BRANCH}


@app.post("/api/login")
def login(body: LoginIn):
    with connect() as c:
        row = c.execute("SELECT username,role FROM users WHERE username=? AND password_sha=?",
                        (body.username, sha(body.password))).fetchone()
    if not row:
        raise HTTPException(401, "账号或密码错误")
    tok = secrets.token_hex(16)
    SESSIONS[tok] = row["username"]
    return {"token": tok, "username": row["username"], "role": row["role"]}


@app.get("/api/contracts")
def contracts(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        return {"items": [dict(r) for r in c.execute("SELECT * FROM contracts ORDER BY id").fetchall()]}


class IngressIn(BaseModel):
    subject: str
    payload_fields: str


def _write_missing(ctr, body: IngressIn, fields: set[str], missing: list[str], operator: str):
    """乙路径唯一写法：缺字段批次与缓冲行同一事务，先挂批次号；缺字段绝不出现在通过/拒收。"""
    ph = payload_hash(body.subject, fields)
    now = now_iso()
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        existing = conn.execute(
            """SELECT i.id, i.contract_version, i.operator FROM ingress_batches i
               JOIN buffers b ON b.batch_id=i.id
               WHERE i.subject=? AND i.payload_hash=? AND i.result='缓冲'
                 AND i.missing_fields <> '' AND b.status='待补采'
               ORDER BY i.id LIMIT 1""",
            (body.subject, ph),
        ).fetchone()
        if existing:
            # 同载荷已在补采队列：不重复落库。同一值班重提只算重复提交；另一值班后到才记缺字段争用。
            if existing["operator"] == operator:
                conn.commit()
                return {"result": "缓冲", "version": existing["contract_version"], "missing": missing,
                        "batch_id": existing["id"], "contention": False,
                        "detail": "缺字段重复提交：本班已挂缓冲队列，未重复落库"}
            conn.execute(
                "INSERT INTO missing_field_contention(subject,payload_hash,batch_id,operator,detail,created_at) "
                "VALUES(?,?,?,?,?,?)",
                (body.subject, ph, existing["id"], operator, "缺字段争用", now),
            )
            conn.commit()
            return {"result": "缓冲", "version": existing["contract_version"], "missing": missing,
                    "batch_id": existing["id"], "contention": True,
                    "detail": "缺字段争用：已在补采队列，后到只登记争用，未重复落库"}
        cur = conn.execute(
            "INSERT INTO ingress_batches(subject,payload_fields,contract_version,result,detail,created_at,"
            "missing_fields,payload_hash,operator) VALUES(?,?,?,?,?,?,?,?,?)",
            (body.subject, body.payload_fields, ctr["version"], "缓冲", "缺字段:" + ",".join(missing),
             now, ",".join(missing), ph, operator),
        )
        batch_id = cur.lastrowid
        conn.execute(
            "INSERT INTO buffers(batch_id,reason,status,created_at) VALUES(?,?,?,?)",
            (batch_id, "缺字段进缓冲队列", "待补采", now),
        )
        conn.commit()
        return {"result": "缓冲", "version": ctr["version"], "missing": missing,
                "batch_id": batch_id, "contention": False, "detail": "缺字段已挂缓冲队列，待补采"}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


@app.post("/api/ingress/validate")
def validate(body: IngressIn, authorization: str | None = Header(default=None)):
    operator = require_user(authorization)
    fields = parse_fields(body.payload_fields)
    with connect() as c:
        ctr = c.execute(
            "SELECT * FROM contracts WHERE subject=? AND track='正式' AND status='启用' ORDER BY id DESC LIMIT 1",
            (body.subject,),
        ).fetchone()
    if not ctr:
        raise HTTPException(404, "无启用正式契约")
    need = parse_fields(ctr["required_fields"])
    missing = sorted(need - fields)
    if missing:
        return _write_missing(ctr, body, fields, missing, operator)

    ph = payload_hash(body.subject, fields)
    now = now_iso()
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        cur = conn.execute(
            "INSERT INTO ingress_batches(subject,payload_fields,contract_version,result,detail,created_at,"
            "missing_fields,payload_hash,operator) VALUES(?,?,?,?,?,?,?,?,?)",
            (body.subject, body.payload_fields, ctr["version"], "通过", "字段齐全", now, "", ph, operator),
        )
        conn.commit()
        return {"result": "通过", "version": ctr["version"], "batch_id": cur.lastrowid}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


@app.get("/api/ingress")
def ingress(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        return {"items": [dict(r) for r in c.execute("SELECT * FROM ingress_batches ORDER BY id DESC").fetchall()]}


@app.get("/api/buffers")
def buffers(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        rows = c.execute(
            """SELECT b.*, i.subject, i.payload_fields, i.result AS batch_result, i.missing_fields
               FROM buffers b JOIN ingress_batches i ON i.id=b.batch_id
               ORDER BY b.batch_id ASC, b.id ASC"""
        ).fetchall()
        items = []
        for idx, r in enumerate(rows, start=1):
            item = dict(r)
            item["queue_no"] = idx
            items.append(item)
        return {"items": items}


@app.get("/api/rejections")
def rejections(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        rows = c.execute(
            "SELECT * FROM ingress_batches WHERE result='拒收' AND missing_fields='' ORDER BY id DESC"
        ).fetchall()
        missing_rejected = c.execute(
            "SELECT COUNT(*) c FROM ingress_batches WHERE result='拒收' AND missing_fields <> ''"
        ).fetchone()["c"]
        items = [{**dict(r), "is_missing": False} for r in rows]
        return {"items": items, "total": len(items), "missing_rejected": missing_rejected}


@app.get("/api/missing/policy")
def missing_policy(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        row = c.execute("SELECT * FROM missing_field_policy WHERE id=1").fetchone()
        ledger, closure = _surface_payload(c)
    if not row:
        raise HTTPException(404, "缺字段说明册未就绪")
    return {**dict(row), "locked": row["chosen_at"] == today(), "ledger": ledger, "closure": closure}


@app.get("/api/reconcile")
def reconcile(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        total = c.execute("SELECT COUNT(*) c FROM ingress_batches").fetchone()["c"]
        passed = c.execute("SELECT COUNT(*) c FROM ingress_batches WHERE result='通过'").fetchone()["c"]
        buffered = c.execute("SELECT COUNT(*) c FROM ingress_batches WHERE result='缓冲'").fetchone()["c"]
        rejected = c.execute("SELECT COUNT(*) c FROM ingress_batches WHERE result='拒收'").fetchone()["c"]
        buffer_rows = c.execute("SELECT COUNT(*) c FROM buffers").fetchone()["c"]
        ledger, closure = _surface_payload(c)
    return {
        "ingress_total": total,
        "passed": passed,
        "buffered": buffered,
        "rejected": rejected,
        "buffer_rows": buffer_rows,
        "missing": {
            "total": ledger["missing_total"],
            "buffered": ledger["missing_buffered"],
            "rejected": ledger["missing_rejected"],
            "passed": ledger["missing_passed"],
            "without_buffer": closure["without_buffer"],
        },
        "orphan_buffers": closure["orphan_buffers"],
        "buffer_on_passed_batch": closure["buffer_on_passed_batch"],
        "buffer_passed_status": closure["buffer_passed_status"],
        "unlabeled_buffers": closure["unlabeled_buffers"],
        "contention": closure["contention"],
        "deviations": closure["deviations"],
        "surfaces": closure["surfaces"],
        "branch": MISSING_BRANCH,
        "aligned": closure["aligned"],
    }


@app.get("/")
def index():
    return FileResponse(FRONTEND / "index.html")


app.mount("/static", StaticFiles(directory=str(FRONTEND)), name="static")
