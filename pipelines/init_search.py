import os, json, meilisearch
from pipelines.util import pg_conn
from dotenv import load_dotenv
load_dotenv()

HOST = os.getenv("MEILI_HOST", "http://localhost:7700")
MASTER = os.getenv("MEILI_MASTER_KEY", "master_key_change_me")
INDEX = "intel_clusters"

def get_latest_ts(conn, cid):
    cur = conn.cursor()
    cur.execute("""
      SELECT MAX(a.ts) FROM clusters c
      JOIN articles_clean a ON a.id=c.id
      WHERE c.cluster_id=%s
    """, (cid,))
    ts = cur.fetchone()[0]
    cur.close()
    return (ts.isoformat() if ts else None)

def main():
    client = meilisearch.Client(HOST, MASTER)
    idx = client.index(INDEX)
    try:
        idx.get_raw_info()
    except Exception:
        client.create_index(INDEX, {"primaryKey":"cluster_id"})
        idx = client.index(INDEX)

    conn = pg_conn(); cur = conn.cursor()
    cur.execute("SELECT cluster_id, title, summary FROM clusters ORDER BY created_at DESC;")
    rows = cur.fetchall(); cur.close(); conn.close()

    docs = []
    for cid, title, summary in rows:
        js = summary if isinstance(summary, dict) else json.loads(summary or "{}")
        doc = {
            "cluster_id": cid,
            "title": js.get("title") or title,
            "text": js.get("context",""),
            "key_points": js.get("key_points", []),
            "tags": js.get("tags", []),
            "priority": js.get("priority","low"),
            "links": js.get("links", []),
            "date": None,
            "projects": [],
            "topics": [],
            "arches": []
        }
        # 从 tags 拆回三个域
        for t in doc["tags"]:
            if t in ["LLVM","GCC","Rust","Swift","Linux Kernel","V8","GraalVM","Cranelift/Wasmtime","Others"]:
                doc["projects"].append(t)
        # 简单推断 topics/arches（如果 tags 里就有）
        for k in ["Release","Regression","Performance","Backend/Target","IR/Pass","Toolchain"]:
            if k in doc["tags"]: doc["topics"].append(k)
        for k in ["RISC-V","ARM64","x86_64","PowerPC","MIPS","WASM","GPU"]:
            if k in doc["tags"]: doc["arches"].append(k)

        doc["date"] = get_latest_ts(pg_conn(), cid)
        docs.append(doc)

    if docs:
        idx.add_documents(docs)

    # 索引设置：可检索/可筛选/可排序
    idx.update_searchable_attributes(["title","text","key_points","tags"])
    idx.update_filterable_attributes(["projects","topics","arches","priority","date"])
    idx.update_sortable_attributes(["date"])

    # 生成一个只读 search key（打印出来供前端用）
    try:
        key = client.create_key({
            "name": "public-search",
            "description":"public search key",
            "actions":["search"],
            "indexes":[INDEX]
        })
        print("[search-key]", key.get("key"))
    except Exception as e:
        print("[search-key] create failed:", e)

    print(f"[index] {len(docs)} docs -> {HOST} index={INDEX}")

if __name__ == "__main__":
    main()