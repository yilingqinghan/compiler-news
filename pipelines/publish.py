import os, json, requests
from datetime import date, datetime
from collections import defaultdict
from jinja2 import Environment, FileSystemLoader, select_autoescape
from pipelines.util import ensure_tables, pg_conn

SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK","")

env = Environment(
    loader=FileSystemLoader("web/templates"),
    autoescape=select_autoescape(["html"]),
)

def _priority_rank(p): return {"high":0,"medium":1,"low":2}.get(p or "low", 2)

def _cluster_meta(conn, cid):
    # 统计该簇的最新时间与标签合并
    cur = conn.cursor()
    cur.execute("""
      SELECT a.ts, a.metadata
      FROM clusters c
      JOIN articles_clean a ON a.id=c.id
      WHERE c.cluster_id=%s
    """, (cid,))
    rows = cur.fetchall(); cur.close()
    latest_ts = None; tags = {"projects":set(),"topics":set(),"arches":set(),"priority":set()}
    for ts, md in rows:
        if ts and (latest_ts is None or ts>latest_ts): latest_ts = ts
        try:
            m = md if isinstance(md, dict) else json.loads(md or "{}")
            for k in tags.keys():
                v = m.get(k, [])
                if isinstance(v, str): v=[v]
                for x in v: tags[k].add(x)
        except Exception: pass
    priority = (list(tags["priority"]) or ["low"])[0]
    return latest_ts, sorted(tags["projects"] or {"Others"}), priority

def render_pages(grouped):
    today = date.today().isoformat()
    daily = env.get_template("daily.html.j2").render(date=today, groups=grouped)
    index = env.get_template("index.html.j2").render(last_date=today)
    os.makedirs("web/dist", exist_ok=True)
    with open(f"web/dist/{today}.html","w",encoding="utf-8") as f: f.write(daily)
    with open("web/dist/index.html","w",encoding="utf-8") as f: f.write(index)
    return f"web/dist/{today}.html"

def notify_slack(items, daily_path):
    if not SLACK_WEBHOOK: return
    top = items[:3]
    lines = [f"*编译器每日情报*（{datetime.now().strftime('%Y-%m-%d')}）"]
    for it in top:
        s = it["summary"]
        t = s.get("title") or it["title"] or "(no title)"
        link = (s.get("links") or [""])[0]
        lines.append(f"• {t} {link}")
    lines.append(f"\n日报：{daily_path}")
    try: requests.post(SLACK_WEBHOOK, json={"text":"\n".join(lines)}, timeout=15)
    except Exception as ex: print("slack error:", ex)

def main():
    ensure_tables()
    conn = pg_conn(); cur = conn.cursor()
    cur.execute("""
      SELECT c.cluster_id, c.title, c.summary
      FROM clusters c
      ORDER BY c.created_at DESC
    """)
    rows = cur.fetchall(); cur.close()

    enriched = []
    for cid, title, summary in rows:
        js = summary if isinstance(summary, dict) else json.loads(summary or "{}")
        ts, projects, priority = _cluster_meta(conn, cid)
        enriched.append({
            "cluster_id": cid,
            "title": title,
            "summary": js,
            "projects": projects,
            "priority": priority,
            "latest_ts": ts or datetime.min
        })

    # 分组：按项目（取列表第一个当“主项目”）
    grouped = defaultdict(list)
    for it in enriched:
        key = it["projects"][0] if it["projects"] else "Others"
        grouped[key].append(it)

    # 每组内排序：优先级 -> 时间新
    for k in grouped:
        grouped[k].sort(key=lambda x: (_priority_rank(x["priority"]), x["latest_ts"]), reverse=False)
        grouped[k].sort(key=lambda x: x["latest_ts"], reverse=True)  # 同优先级时按时间

    daily_path = render_pages(grouped)
    # 选 Top3 推送（从所有组里抽最新的）
    all_sorted = sorted(enriched, key=lambda x: (_priority_rank(x["priority"]), x["latest_ts"]), reverse=False)
    all_sorted = sorted(all_sorted, key=lambda x: x["latest_ts"], reverse=True)
    notify_slack(all_sorted, daily_path)
    print("[publish] ok ->", daily_path)

if __name__ == "__main__":
    main()