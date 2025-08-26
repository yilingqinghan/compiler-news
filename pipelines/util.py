# pipelines/util.py
# ======================================================================
#  通用工具（数据库/索引/CLI 入口）
#  - 统一日志：info / warn / error / success（基于 pipelines.logging_utils）
#  - 友好异常：连接失败时给出明确修复建议（不打印长 Traceback）
#  - 无副作用：模块 import 不产生日志，只有函数调用时才输出
# ======================================================================

from __future__ import annotations
import os, hashlib, socket, sys
from urllib.parse import urlparse
from dotenv import load_dotenv
import psycopg2

# 统一日志（在无 rich 环境下会自动退化为纯文本）
from pipelines.logging_utils import (
    info, warn, error, success, debug, status
)

load_dotenv()

# --------------------------- 配置 ---------------------------
# 连接串：优先 PG_DSN，然后 POSTGRES_URL（兼容旧变量名）
POSTGRES_URL = (os.getenv("PG_DSN")
                or os.getenv("POSTGRES_URL",
                             "postgresql://compiler:compiler@localhost:5432/compiler_intel")).strip()

MEILI_HOST = os.getenv("MEILI_HOST", "http://localhost:7700").rstrip("/")

# --------------------------- 友好异常 ---------------------------
class ServiceUnavailable(RuntimeError): ...
class InfraError(RuntimeError): ...  # 占位：基础设施异常由上层决定如何触发

# --------------------------- 杂项 ---------------------------
def sha1(s: str) -> str:
    """计算字符串的 SHA1（用于生成稳定主键）"""
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def _tcp_ready(host: str, port: int, timeout: float = 0.8) -> bool:
    """简单 TCP 探测（用于区分“端口未开”与“认证失败”）"""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except Exception:
        return False

# --------------------------- 数据库连接 ---------------------------
def pg_conn():
    """
    打开 PostgreSQL 连接。
    - 若端口不可达：提示如何启动 PG（docker compose / brew services）。
    - 若端口可达但失败：提示检查用户/密码/库名。
    - 抛出 ServiceUnavailable，由 run_cli 统一友好打印。
    """
    try:
        debug(f"[util] connecting PostgreSQL: {POSTGRES_URL}")
        return psycopg2.connect(POSTGRES_URL)
    except psycopg2.OperationalError:
        # 生成人类可读的定位提示；由 run_cli 捕获并打印
        try:
            u = urlparse(POSTGRES_URL)
            host, port = u.hostname or "localhost", int(u.port or 5432)
        except Exception:
            host, port = "localhost", 5432

        if not _tcp_ready(host, port):
            hint = (
                f"❌ 无法连接 PostgreSQL {host}:{port}\n"
                f"   - docker compose:  docker compose up -d postgres\n"
                f"   - 本机（macOS）：  brew services start postgresql@16（或你的版本）"
            )
        else:
            hint = (
                "⚠️ PostgreSQL 端口可达，但认证/数据库名可能错误：\n"
                "   - 请检查用户/密码/数据库名是否与 PG_DSN/POSTGRES_URL 一致"
            )

        # 统一抛出友好异常（不携带原始堆栈）
        raise ServiceUnavailable(
            "数据库未就绪或连接失败。\n"
            + hint +
            f"\n   - 当前连接串：{POSTGRES_URL}"
        ) from None

# --------------------------- 建表 ---------------------------
def ensure_tables():
    """
    创建核心表（若不存在）：
      - articles_raw  : 原始抓取（HTML 或正文片段）
      - articles_clean: 清洗后文本、元数据
      - clusters      : 粗聚类/去重后的聚合记录
    """
    with status("[util] 创建/检查数据表 …", spinner="dots"):
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
    success("[util] 数据表可用")

# --------------------------- CLI 入口包装 ---------------------------
def run_cli(main_fn):
    """
    包装 CLI 入口：
      - 将 ServiceUnavailable / InfraError 打印为一段友好提示
      - 抑制长 Traceback（仍保持非零退出码以便 CI 感知失败）
    """
    # 抑制 Python 默认长堆栈（兜底）
    sys.tracebacklimit = 0
    try:
        main_fn()
    except (ServiceUnavailable, InfraError) as e:
        # 使用统一风格输出
        error(str(e))
        sys.exit(2)

# --------------------------- Meili 探测 ---------------------------
def meili_ready() -> bool:
    """
    轻量探测 Meilisearch 健康：
      - 仅做可达性判断；不打印错误以避免噪音（上层按需日志）
    """
    try:
        from http.client import HTTPConnection
        u = urlparse(MEILI_HOST + "/health")
        conn = HTTPConnection(u.hostname, u.port or (80 if u.scheme == "http" else 443), timeout=1.0)
        conn.request("GET", u.path or "/")
        r = conn.getresponse()
        return 200 <= r.status < 500
    except Exception:
        return False