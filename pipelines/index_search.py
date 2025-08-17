import os
import json
import time
from typing import List, Dict, Any

import meilisearch
from meilisearch.errors import MeilisearchApiError
from dotenv import load_dotenv

from pipelines.util import pg_conn

load_dotenv()

HOST = os.getenv("MEILI_HOST", "http://localhost:7700").rstrip("/")
MASTER = os.getenv("MEILI_MASTER_KEY", "master_key_change_me")
INDEX = "intel_clusters"


def wait_for_meili(client: meilisearch.Client, timeout: float = 30.0) -> None:
    """阻塞直到 Meilisearch 可用，否则抛错。"""
    deadline = time.time() + timeout
    last_err = None
    while time.time() < deadline:
        try:
            h = client.health()  # {'status': 'available'}
            if isinstance(h, dict) and h.get("status") == "available":
                return
        except Exception as e:
            last_err = e
        time.sleep(1.0)
    raise RuntimeError(
        f"Meilisearch 未就绪或无法访问（HOST={HOST}）。"
        f"请确认容器/服务已启动并端口可达。最近一次错误：{last_err}"
    )


def ensure_index(client: meilisearch.Client, uid: str, primary_key: str = "cluster_id"):
    """确保索引存在；不存在时创建。返回 Index 对象。"""
    try:
        return client.get_index(uid)
    except MeilisearchApiError as e:
        # 可能是 404 不存在；其他错误继续抛出
        msg = str(e).lower()
        if "404" in msg or "not found" in msg:
            task = client.create_index(uid, {"primaryKey": primary_key})
            # 等待任务完成（部分 SDK 需要轮询；这里做一个简易等待）
            time.sleep(0.3)
            return client.get_index(uid)
        raise


def get_latest_ts(conn, cid: str):
    cur = conn.cursor()
    cur.execute(
        """
        SELECT MAX(a.ts) FROM clusters c
        JOIN articles_clean a ON a.id=c.id
        WHERE c.cluster_id=%s
        """,
        (cid,),
    )
    ts = cur.fetchone()[0]
    cur.close()
    return ts.isoformat() if ts else None


def build_docs(conn) -> List[Dict[str, Any]]:
    cur = conn.cursor()
    cur.execute(
        "SELECT cluster_id, title, summary FROM clusters ORDER BY created_at DESC;"
    )
    rows = cur.fetchall()
    cur.close()

    docs = []
    for cid, title, summary in rows:
        js = summary if isinstance(summary, dict) else json.loads(summary or "{}")
        tags = js.get("tags", []) or []

        doc = {
            "cluster_id": cid,
            "title": js.get("title") or title or "(no title)",
            "text": js.get("context", "") or "",
            "key_points": js.get("key_points", []) or [],
            "tags": tags,
            "priority": js.get("priority", "low"),
            "links": js.get("links", []) or [],
            "projects": [],
            "topics": [],
            "arches": [],
            "date": get_latest_ts(conn, cid),
        }

        # 从 tags 粗分回 projects/topics/arches
        for t in tags:
            if t in [
                "LLVM",
                "GCC",
                "Rust",
                "Swift",
                "Linux Kernel",
                "V8",
                "GraalVM",
                "Cranelift/Wasmtime",
                "Others",
            ]:
                doc["projects"].append(t)

        for k in [
            "Release",
            "Regression",
            "Performance",
            "Backend/Target",
            "IR/Pass",
            "Toolchain",
        ]:
            if k in tags:
                doc["topics"].append(k)

        for k in ["RISC-V", "ARM64", "x86_64", "PowerPC", "MIPS", "WASM", "GPU"]:
            if k in tags:
                doc["arches"].append(k)

        docs.append(doc)
    return docs


def main():
    # 1) 连接 Meili 并等待可用
    client = meilisearch.Client(HOST, MASTER)
    wait_for_meili(client)

    # 2) 确保索引存在
    idx = ensure_index(client, INDEX, primary_key="cluster_id")

    # 3) 准备文档（一次数据库连接）
    conn = pg_conn()
    docs = build_docs(conn)
    conn.close()

    if not docs:
        print("[index] 没有可索引的文档（clusters 为空），跳过。")
        return

    # 4) 写入文档
    idx.add_documents(docs)  # primaryKey 已在建索引时设置

    # 5) 设置索引属性
    idx.update_searchable_attributes(["title", "text", "key_points", "tags"])
    idx.update_filterable_attributes(["projects", "topics", "arches", "priority", "date"])
    idx.update_sortable_attributes(["date"])

    # 6) 创建只读 search key（用于前端）
    try:
        key_obj = client.create_key(
            {
                "name": "public-search",
                "description": "public search key for intel_clusters",
                "actions": ["search"],
                "indexes": [INDEX],
            }
        )
        # 新版 SDK 返回字典里是 'key'；若老版不同，这里再兜底取 'uid'
        pub_key = key_obj.get("key") or key_obj.get("uid")
        if pub_key:
            print("[search-key]", pub_key)
        else:
            print("[search-key] 创建成功但未返回 key 字段，请在 Meili 控制台检查。")
    except MeilisearchApiError as e:
        print("[search-key] 创建失败：", e)

    print(f"[index] upsert {len(docs)} docs -> {HOST} index={INDEX}")


if __name__ == "__main__":
    main()