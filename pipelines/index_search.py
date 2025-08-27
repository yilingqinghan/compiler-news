# pipelines/index_search.py
# ======================================================================
#  索引构建 & 搜索页渲染
#  - 从 clusters + articles_clean 构建文档
#  - 推送到 Meilisearch（健康检查/创建索引/写入属性）
#  - 自动生成 web/dist/search.html
#  - 日志统一：彩色、阶段耗时、spinner、进度条
# ======================================================================

import os, json, time, requests, meilisearch
from jinja2 import Environment, FileSystemLoader, select_autoescape
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone

from pipelines.util import pg_conn, ensure_tables, run_cli
from pipelines.logging_utils import (
    info, debug, warn, error, success,
    kv_line, kv_table, status, step
)

load_dotenv()

HOST   = os.getenv("MEILI_HOST", "http://localhost:7700").rstrip("/")
MASTER = os.getenv("MEILI_MASTER_KEY", "master_key_change_me")
INDEX  = "intel_clusters"
PUBLIC_EXP_DAYS = int(os.getenv("MEILI_PUBLIC_KEY_EXPIRES_DAYS", "3650"))


# --------------------------- Jinja 环境 ---------------------------
def env_jinja():
    return Environment(
        loader=FileSystemLoader("web/templates"),
        autoescape=select_autoescape(["html"])
    )


# --------------------------- Meili 健康检查 ---------------------------
def wait_for_meili(host, master, tries=18, base_delay=0.4):
    """循环探测 /health，直到 ready 或重试次数耗尽"""
    last_err = None
    url = host.rstrip("/") + "/health"
    headers = {"Authorization": f"Bearer {master}", "X-Meili-API-Key": master}

    for i in range(tries):
        try:
            r = requests.get(url, headers=headers, timeout=2.5)
            if r.ok and (r.json().get("status") in ("available","healthy","ready")):
                success("[index] Meilisearch 就绪")
                return
            last_err = RuntimeError(f"health={r.text}")
        except Exception as e:
            last_err = e
        time.sleep(base_delay * (1.5 ** i))

    raise RuntimeError(
        f"Meilisearch 未就绪或无法访问（HOST={host}）。最近一次错误：{last_err}"
    )


# --------------------------- 辅助函数 ---------------------------
def get_latest_ts(conn, cid):
    """取 cluster 最新文章的时间戳"""
    cur = conn.cursor()
    cur.execute("""
      SELECT MAX(a.ts) FROM clusters c
      JOIN articles_clean a ON a.id=c.id
      WHERE c.cluster_id=%s
    """, (cid,))
    ts = cur.fetchone()[0]
    cur.close()
    return ts.isoformat() if ts else None


def build_docs(conn):
    """从 clusters 表构建待写入的文档数组"""
    cur = conn.cursor()
    cur.execute("SELECT cluster_id, title, summary FROM clusters ORDER BY created_at DESC;")
    rows = cur.fetchall()
    cur.close()

    docs = []
    for cid, title, summary in rows:
        js = summary if isinstance(summary, dict) else json.loads(summary or "{}")
        tags = js.get("tags") or []
        docs.append({
            "cluster_id": cid,
            "title": js.get("title") or title or "(no title)",
            "title_zh": js.get("title_zh") or "",
            "text": js.get("context","") or "",
            "context_zh": js.get("context_zh","") or "",
            "key_points": js.get("key_points") or [],
            "key_points_zh": js.get("key_points_zh") or [],
            "tags": tags,
            "priority": (js.get("priority") or "low"),
            "links": js.get("links") or [],
            "projects": [t for t in tags if t in ["LLVM","GCC","Rust","Swift","V8","GraalVM","Cranelift/Wasmtime","Zig"]],
            "topics":   [t for t in tags if t in ["Release","Regression","Performance","Backend/Target","IR/Pass","Toolchain","Weekly","Commits","Patches"]],
            "arches":   [t for t in tags if t in ["RISC-V","ARM64","x86_64","PowerPC","MIPS","WASM","GPU"]],
            "lang": js.get("lang","en"),
            "date": get_latest_ts(conn, cid),
        })
    return docs


# --------------------------- 主流程 ---------------------------
@step("Index & Search Page")
def main():
    ensure_tables()
    conn = pg_conn()

    with status("[index] 构建文档列表 …", spinner="dots"):
        docs = build_docs(conn)
    conn.close()
    kv_line("[index] 文档数量", total=len(docs))

    meili_ok = False
    public_key = os.getenv("MEILI_PUBLIC_KEY", "")

    try:
        # 1) 健康检查
        wait_for_meili(HOST, MASTER)

        # 2) 建立客户端/索引
        client = meilisearch.Client(HOST, MASTER)
        idx = client.index(INDEX)
        try:
            client.create_index(INDEX, {"primaryKey":"cluster_id"})
            success(f"[index] 索引 {INDEX} 已创建")
        except Exception:
            debug(f"[index] 索引 {INDEX} 已存在")

        # 3) 推送文档
        if docs:
            with status("[index] 推送文档到 Meili …", spinner="dots"):
                idx.add_documents(docs)
            idx.update_searchable_attributes(["title","title_zh","text","context_zh","key_points","key_points_zh","tags"])
            idx.update_filterable_attributes(["projects","topics","arches","priority","date","lang","tags"])
            idx.update_sortable_attributes(["date"])
            success(f"[index] indexed {len(docs)} docs -> {HOST} index={INDEX}")

        # 4) 创建公开 key（若未设置）
        if not public_key:
            try:
                # Meilisearch 新版本要求携带 expiresAt；用 UTC、到秒、Z 结尾的 RFC3339
                expires_at = (datetime.now(timezone.utc)  timedelta(days=PUBLIC_EXP_DAYS)) \
                        .replace(microsecond=0).isoformat().replace("00:00", "Z")
                key = client.create_key({
                    "name": "public-search",
                    "description": "Read-only client key for browser search",
                    "actions": ["search"],
                    "indexes": [INDEX],
                    "expiresAt": expires_at,
                })
                public_key = key.get("key", "")
                success(f"[index] 已创建 public key（有效期 {PUBLIC_EXP_DAYS} 天）")
            except Exception as e:
                warn(f"[index] public key 创建失败: {e}")
                public_key = ""

        meili_ok = True

    except Exception as e:
        error(f"[index] 处理失败: {e}")
        meili_ok = False

    # 5) 渲染搜索页（即使 meili 不可用也要输出）
    env = env_jinja()
    os.makedirs("web/dist", exist_ok=True)
    html = env.get_template("search.html.j2").render(
        meili_host=(HOST if meili_ok else ""),
        meili_key=(public_key if meili_ok else ""),
    )
    with open("web/dist/search.html","w",encoding="utf-8") as f:
        f.write(html)
    success(f"[search] -> web/dist/search.html (meili_ok={meili_ok})")


# --------------------------- CLI 入口 ---------------------------
if __name__ == "__main__":
    run_cli(main)