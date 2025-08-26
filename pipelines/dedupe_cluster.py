# pipelines/dedupe_cluster.py
# =======================================================================
#  文章去重与粗粒度聚类（基于 TF-IDF + 余弦相似度）
#  - 统一风格日志：阶段耗时、spinner、进度条、表格汇总
#  - 可配置参数：阈值/样本上限/特征上限/词法等
#  - 不修改表结构；结果写入 clusters 表（与旧版兼容）
# =======================================================================

from __future__ import annotations
import os
import json
from typing import List, Tuple

from pipelines.util import ensure_tables, pg_conn, sha1, run_cli
from pipelines.logging_utils import (
    info, debug, warn, error, success, kv_line, kv_table,
    status, new_progress, step
)

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity


# ------------------------- 可配置参数 -------------------------
# 可通过环境变量覆盖，方便快速调参/回归
CLUSTER_LIMIT       = int(os.getenv("CLUSTER_LIMIT", "500"))     # 从 articles_clean 取多少最新样本
SIM_THRESHOLD       = float(os.getenv("CLUSTER_SIM_TH", "0.45")) # 余弦相似度阈值
TFIDF_MAX_FEATURES  = int(os.getenv("TFIDF_MAX_FEATURES", "20000"))
TFIDF_NGRAM_MIN     = int(os.getenv("TFIDF_NGRAM_MIN", "1"))
TFIDF_NGRAM_MAX     = int(os.getenv("TFIDF_NGRAM_MAX", "2"))
TFIDF_STOP_WORDS    = os.getenv("TFIDF_STOP_WORDS", "english")   # "english" / None


# ------------------------- 主流程 -------------------------
@step("Deduplicate & Cluster")
def main():
    # 0) 准备：表与连接
    ensure_tables()
    conn = pg_conn()
    cur = conn.cursor()

    kv_line("[cluster] 参数",
            limit=CLUSTER_LIMIT,
            th=SIM_THRESHOLD,
            max_features=TFIDF_MAX_FEATURES,
            ngram=f"({TFIDF_NGRAM_MIN},{TFIDF_NGRAM_MAX})",
            stop_words=TFIDF_STOP_WORDS)

    # 1) 读取样本
    cur.execute(
        "SELECT id, title, text FROM articles_clean "
        "ORDER BY ts DESC "
        "LIMIT %s;", (CLUSTER_LIMIT,)
    )
    rows: List[Tuple[str, str, str]] = cur.fetchall()
    if not rows:
        warn("[cluster] 没有可聚类的文档（articles_clean 为空）")
        cur.close(); conn.close()
        return

    ids    = [r[0] for r in rows]
    titles = [r[1] or "" for r in rows]
    texts  = [r[2] or "" for r in rows]
    docs   = [(titles[i] + "\n" + texts[i]).strip() for i in range(len(rows))]

    kv_line("[cluster] 样本",
            rows=len(rows),
            title_empty=sum(1 for t in titles if not t),
            text_empty=sum(1 for t in texts if not t))

    # 2) 文本向量化（TF-IDF）
    with status("[cluster] 向量化 TF-IDF …", spinner="dots"):
        vec = TfidfVectorizer(
            max_features=TFIDF_MAX_FEATURES,
            ngram_range=(TFIDF_NGRAM_MIN, TFIDF_NGRAM_MAX),
            stop_words=None if TFIDF_STOP_WORDS in ("", "None", "none") else TFIDF_STOP_WORDS,
        )
        X = vec.fit_transform(docs)
    debug(f"[cluster] TF-IDF shape={X.shape}")

    # 3) 计算两两相似度（可选：大矩阵时考虑近邻稀疏近似）
    with status("[cluster] 计算余弦相似度 …", spinner="dots"):
        sim = cosine_similarity(X)  # 注意：O(N^2)；N=500 -> 250k 条，OK
    debug("[cluster] similarity computed")

    # 4) 贪心分组（阈值连通）
    groups: List[List[int]] = []
    seen = set()

    with new_progress() as progress:
        task = progress.add_task("基于阈值的贪心分组", total=len(rows))
        for i in range(len(rows)):
            progress.advance(task, 1)
            if i in seen:
                continue
            g = [i]; seen.add(i)
            # 提示：此处可换成“找 topk 相似 + 阈值”以控复杂度
            for j in range(i + 1, len(rows)):
                if j in seen:
                    continue
                if sim[i, j] >= SIM_THRESHOLD:
                    g.append(j); seen.add(j)
            groups.append(g)

    # 5) 写入 clusters 表
    cur2 = conn.cursor()
    created = 0

    # 进度条按“簇内成员”推进，更直观
    total_links = sum(len(g) for g in groups)
    with new_progress() as progress:
        t_write = progress.add_task("写入 clusters", total=total_links)
        for g in groups:
            if not g:
                continue
            # 用所有成员 id 拼接做内容签名，sha1 截断 16 保持稳定
            cid = "c_" + sha1("".join(ids[k] for k in g))[:16]
            for k in g:
                cur2.execute(
                    """
                    INSERT INTO clusters (cluster_id, id, title, summary)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING;
                    """,
                    (cid, ids[k], titles[k], json.dumps({}))
                )
                created += cur2.rowcount or 0
                progress.advance(t_write, 1)

    conn.commit()
    cur2.close(); cur.close(); conn.close()

    # 6) 汇总输出
    sizes = sorted((len(g) for g in groups), reverse=True)
    kv_table("[cluster] 汇总", {
        "groups": len(groups),
        "links_written": created,
        "largest_group": sizes[0] if sizes else 0,
        "median_group": (sizes[len(sizes)//2] if sizes else 0),
        "singleton_groups": sum(1 for s in sizes if s == 1),
    })
    success(f"[cluster] updated rows ~{created}, clusters={len(groups)}")


# ------------------------- CLI 入口 -------------------------
if __name__ == "__main__":
    run_cli(main)