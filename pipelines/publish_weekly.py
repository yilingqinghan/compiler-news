# pipelines/publish_weekly.py
import os, json
from datetime import datetime, timedelta
from collections import defaultdict, Counter
from jinja2 import Environment, FileSystemLoader, select_autoescape
from pipelines.util import ensure_tables, pg_conn
from dotenv import load_dotenv

load_dotenv()
TIME_WINDOW_DAYS = int(os.getenv("TIME_WINDOW_DAYS", "7"))

env = Environment(loader=FileSystemLoader("web/templates"),
                  autoescape=select_autoescape(["html"]))

def _priority_rank(p): return {"high":0,"medium":1,"low":2}.get(p or "low", 2)

def _window():
    end = datetime.now()
    start = end - timedelta(days=TIME_WINDOW_DAYS)
    return start, end

def _cluster_stats(conn, cid):
    cur = conn.cursor()
    cur.execute("""
      SELECT a.ts, a.url, a.title, a.metadata, a.text
      FROM clusters c JOIN articles_clean a ON a.id=c.id
      WHERE c.cluster_id=%s
    """, (cid,))
    rows = cur.fetchall(); cur.close()
    latest = None; sources = []; ok = False
    tags = {"projects":set(),"topics":set(),"arches":set(),"priority":set()}
    for ts, url, title, md, text in rows:
        if ts and (latest is None or ts>latest): latest = ts
        try:
            m = md if isinstance(md, dict) else json.loads(md or "{}")
        except Exception:
            m = {}
        for k in tags.keys():
            v = m.get(k, []); v = [v] if isinstance(v, str) else (v or [])
            for x in v: tags[k].add(x)
        src = m.get("source") or ""
        if src: sources.append(src)
        if (url and len(url)>5) and (text and len(text)>60): ok = True
    priority = (list(tags["priority"]) or ["low"])[0]
    return latest, sorted(tags["projects"] or {"Others"}), priority, Counter(sources), ok

def main():
    ensure_tables()
    conn = pg_conn(); cur = conn.cursor()
    start, end = _window()

    # ✅ 用 DISTINCT ON 选每个 cluster 最新一条，避免 DISTINCT + ORDER BY 冲突
    cur.execute("""
      SELECT DISTINCT ON (c.cluster_id) c.cluster_id, c.title, c.summary
      FROM clusters c
      JOIN articles_clean a ON a.id = c.id
      WHERE a.ts >= %s AND a.ts < %s
      ORDER BY c.cluster_id, c.created_at DESC
    """, (start, end))
    rows = cur.fetchall(); cur.close()

    enriched = []
    source_counter = Counter()
    for cid, title, summary in rows:
        js = summary if isinstance(summary, dict) else json.loads(summary or "{}")
        latest_ts, projects, priority, src_cnt, ok = _cluster_stats(conn, cid)
        source_counter.update(src_cnt)

        links = js.get("links") or []
        # 过滤“只有标题/没链接/没正文”的项
        if not ok or not links:
            continue

        enriched.append({
            "cluster_id": cid,
            "title": js.get("title") or title or "(no title)",
            "summary": js,
            "projects": projects,
            "priority": priority,
            "latest_ts": latest_ts or datetime.min
        })

    groups = defaultdict(list)
    for it in enriched:
        key = "LLVM 专区" if ("LLVM" in it["projects"]) else (it["projects"][0] if it["projects"] else "Others")
        groups[key].append(it)

    for g in groups:
        groups[g].sort(key=lambda x: (_priority_rank(x["priority"]), x["latest_ts"]), reverse=False)
        groups[g].sort(key=lambda x: x["latest_ts"], reverse=True)

    top = sorted(enriched, key=lambda x: (_priority_rank(x["priority"]), x["latest_ts"]), reverse=False)
    top = sorted(top, key=lambda x: x["latest_ts"], reverse=True)[:8]

    today = datetime.now().strftime("%Y-%m-%d")
    html = env.get_template("weekly.html.j2").render(
        date=today,
        start=start.strftime("%Y-%m-%d"),
        end=end.strftime("%Y-%m-%d"),
        groups=groups,
        top=top,
        sources=source_counter.most_common()
    )
    os.makedirs("web/dist", exist_ok=True)
    out = f"web/dist/weekly-{today}.html"
    with open(out,"w",encoding="utf-8") as f: f.write(html)
    with open("web/dist/index.html","w",encoding="utf-8") as f:
        f.write(f'<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">'
                f'<body style="font-family:-apple-system,Segoe UI,Roboto,Helvetica Neue,Arial">'
                f'<p><a href="{out.split("/")[-1]}">打开最新周报</a></p><p><a href="search.html">检索</a></p></body>')
    print("[weekly] ->", out, f"(window {start.date()} ~ {end.date()}, days={TIME_WINDOW_DAYS})")

if __name__ == "__main__":
    main()