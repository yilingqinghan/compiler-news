import json
from pipelines.util import ensure_tables, pg_conn, sha1
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

def main():
    ensure_tables()
    conn = pg_conn()
    cur = conn.cursor()
    cur.execute("SELECT id, title, text FROM articles_clean ORDER BY ts DESC LIMIT 500;")
    rows = cur.fetchall()
    ids = [r[0] for r in rows]
    titles = [r[1] or "" for r in rows]
    texts = [r[2] or "" for r in rows]

    docs = [(titles[i] + "\n" + texts[i]).strip() for i in range(len(rows))]
    if not docs:
        print("[cluster] no docs"); return

    vec = TfidfVectorizer(max_features=20000, ngram_range=(1,2), stop_words="english")
    X = vec.fit_transform(docs)
    sim = cosine_similarity(X)

    TH = 0.45
    seen = set(); groups = []
    for i in range(len(rows)):
        if i in seen: continue
        g = [i]; seen.add(i)
        for j in range(i+1, len(rows)):
            if j in seen: continue
            if sim[i, j] >= TH:
                g.append(j); seen.add(j)
        groups.append(g)

    cur2 = conn.cursor(); created = 0
    for g in groups:
        if not g: continue
        cid = "c_" + sha1("".join(ids[k] for k in g))[:16]
        for k in g:
            cur2.execute("""
            INSERT INTO clusters (cluster_id, id, title, summary)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING;
            """, (cid, ids[k], titles[k], json.dumps({})))
            created += 1
    conn.commit(); cur2.close(); cur.close(); conn.close()
    print(f"[cluster] updated rows ~{created}, clusters={len(groups)}")

from pipelines.util import run_cli
if __name__ == "__main__":
    run_cli(main)
