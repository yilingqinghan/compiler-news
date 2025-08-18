# pipelines/index_search.py
# -*- coding: utf-8 -*-
import os, json, time, requests, meilisearch
from jinja2 import Environment, FileSystemLoader, select_autoescape
from dotenv import load_dotenv
from datetime import datetime
from pipelines.util import pg_conn, ensure_tables

load_dotenv()
HOST   = os.getenv("MEILI_HOST", "http://localhost:7700").rstrip("/")
MASTER = os.getenv("MEILI_MASTER_KEY", "master_key_change_me")
INDEX  = "intel_clusters"

def env_jinja():
    return Environment(loader=FileSystemLoader("web/templates"),
                       autoescape=select_autoescape(["html"]))

def wait_for_meili(host, master, tries=18, base_delay=0.4):
    last_err = None
    url = host.rstrip("/") + "/health"
    headers = {"Authorization": f"Bearer {master}", "X-Meili-API-Key": master}
    for i in range(tries):
        try:
            r = requests.get(url, headers=headers, timeout=2.5)
            if r.ok and (r.json().get("status") in ("available","healthy","ready")):
                return
            last_err = RuntimeError(f"health={r.text}")
        except Exception as e:
            last_err = e
        time.sleep(base_delay * (1.5 ** i))
    raise RuntimeError(
        f"Meilisearch 未就绪或无法访问（HOST={host}）。请确认服务已启动。最近一次错误：{last_err}"
    )

def get_latest_ts(conn, cid):
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
    cur = conn.cursor()
    cur.execute("SELECT cluster_id, title, summary FROM clusters ORDER BY created_at DESC;")
    rows = cur.fetchall(); cur.close()
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

def main():
    ensure_tables()
    conn = pg_conn()
    docs = build_docs(conn)
    conn.close()

    meili_ok = False
    public_key = os.getenv("MEILI_PUBLIC_KEY", "")

    try:
        wait_for_meili(HOST, MASTER)
        client = meilisearch.Client(HOST, MASTER)
        idx = client.index(INDEX)
        # 创建索引（已存在则忽略）
        try:
            client.create_index(INDEX, {"primaryKey":"cluster_id"})
        except Exception:
            pass

        if docs:
            idx.add_documents(docs)
            idx.update_searchable_attributes(["title","title_zh","text","context_zh","key_points","key_points_zh","tags"])
            idx.update_filterable_attributes(["projects","topics","arches","priority","date","lang","tags"])
            idx.update_sortable_attributes(["date"])

        if not public_key:
            try:
                key = client.create_key({
                    "name":"public-search",
                    "actions":["search"],
                    "indexes":[INDEX]
                })
                public_key = key.get("key","")
            except Exception:
                public_key = ""

        meili_ok = True
        print(f"[index] indexed {len(docs)} docs -> {HOST} index={INDEX}")
    except Exception as e:
        print("[index] WARN:", e)
        meili_ok = False

    # 渲染检索页（即使 meili 不可用也输出）
    env = env_jinja()
    os.makedirs("web/dist", exist_ok=True)
    html = env.get_template("search.html.j2").render(
        meili_host=(HOST if meili_ok else ""),
        meili_key=(public_key if meili_ok else ""),
    )
    with open("web/dist/search.html","w",encoding="utf-8") as f: f.write(html)
    print("[search] -> web/dist/search.html (meili_ok=", meili_ok, ")")

if __name__ == "__main__":
    main()