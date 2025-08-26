# pipelines/util.py
import os, hashlib, socket, sys
from urllib.parse import urlparse
from dotenv import load_dotenv
import psycopg2

load_dotenv()

# 连接串：优先 PG_DSN，然后 POSTGRES_URL（兼容你现有变量名）
POSTGRES_URL = (os.getenv("PG_DSN")
                or os.getenv("POSTGRES_URL",
                             "postgresql://compiler:compiler@localhost:5432/compiler_intel")).strip()

MEILI_HOST = os.getenv("MEILI_HOST", "http://localhost:7700").rstrip("/")

# ---- 友好异常（由入口包装器捕获，避免 Traceback） ----
class ServiceUnavailable(RuntimeError): ...
class InfraError(RuntimeError): ...  # 仅保留占位，不在这里主动抛

def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def _tcp_ready(host: str, port: int, timeout: float = 0.8) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except Exception:
        return False

# ---- 数据库连接（仅做友好报错，不做“前置检查”） ----
def pg_conn():
    try:
        return psycopg2.connect(POSTGRES_URL)
    except psycopg2.OperationalError as e:
        # 生成“人类可读”的定位提示；由 run_cli 捕获打印为一段文本
        try:
            u = urlparse(POSTGRES_URL)
            host, port = u.hostname or "localhost", int(u.port or 5432)
        except Exception:
            host, port = "localhost", 5432

        msg_lines = []
        if not _tcp_ready(host, port):
            msg_lines.append(
                f"❌ 无法连接 PostgreSQL {host}:{port}\n"
                f"   - docker compose:  docker compose up -d postgres\n"
                f"   - 本机（macOS）：  brew services start postgresql@16（或你的版本）"
            )
        else:
            msg_lines.append(
                "⚠️ PostgreSQL 端口可达，但认证/数据库名可能错误：\n"
                "   - 请检查用户/密码/数据库名是否与 PG_DSN/POSTGRES_URL 一致"
            )

        # 不携带原异常堆栈，交给 run_cli 统一友好打印
        raise ServiceUnavailable(
            "数据库未就绪或连接失败。\n"
            + "\n".join(msg_lines) +
            f"\n   - 当前连接串：{POSTGRES_URL}"
        ) from None

# ---- 建表（保持你原逻辑） ----
def ensure_tables():
    conn = pg_conn()
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS articles_raw (
        id TEXT PRIMARY KEY,
        source TEXT,
        url TEXT,
        title TEXT,
        ts TIMESTAMP,
        raw_html TEXT,
        fetched_at TIMESTAMP DEFAULT NOW()
    );""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS articles_clean (
        id TEXT PRIMARY KEY,
        source TEXT,
        url TEXT,
        title TEXT,
        ts TIMESTAMP,
        text TEXT,
        metadata JSONB,
        created_at TIMESTAMP DEFAULT NOW()
    );""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS clusters (
        cluster_id TEXT,
        id TEXT PRIMARY KEY,
        title TEXT,
        summary JSONB,
        created_at TIMESTAMP DEFAULT NOW()
    );""")
    conn.commit()
    cur.close(); conn.close()

# ---- 入口包装器：把异常变成“单段友好提示”，不打印 Traceback ----
def run_cli(main_fn):
    # 抑制 Python 默认长堆栈（兜底）
    sys.tracebacklimit = 0
    try:
        main_fn()
    except (ServiceUnavailable, InfraError) as e:
        print(str(e))
        sys.exit(2)

# ---- 可选：供 index_search 判断 Meili 是否就绪 ----
def meili_ready() -> bool:
    try:
        from http.client import HTTPConnection
        u = urlparse(MEILI_HOST + "/health")
        conn = HTTPConnection(u.hostname, u.port or (80 if u.scheme == "http" else 443), timeout=1.0)
        conn.request("GET", u.path or "/")
        r = conn.getresponse()
        return 200 <= r.status < 500
    except Exception:
        return False