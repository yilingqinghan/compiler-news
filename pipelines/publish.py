import os, json, requests
from datetime import date, datetime
from jinja2 import Environment, FileSystemLoader, select_autoescape
from pipelines.util import ensure_tables, pg_conn

SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK","")

env = Environment(
    loader=FileSystemLoader("web/templates"),
    autoescape=select_autoescape(["html"]),
)

def render_pages(items):
    today = date.today().isoformat()
    daily = env.get_template("daily.html.j2").render(date=today, items=items)
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
        s = it.get("summary", {})
        t = s.get("title") or it.get("title") or "(no title)"
        link = (s.get("links") or it.get("links") or [""])[0]
        lines.append(f"• {t} {link}")
    lines.append(f"\n日报：{daily_path}")
    try:
        requests.post(SLACK_WEBHOOK, json={"text":"\n".join(lines)}, timeout=15)
    except Exception as ex:
        print("slack error:", ex)

def main():
    ensure_tables()
    conn = pg_conn(); cur = conn.cursor()
    cur.execute("""
    SELECT c.cluster_id, c.title, c.summary
    FROM clusters c
    ORDER BY c.created_at DESC
    LIMIT 50;
    """)
    rows = cur.fetchall(); cur.close(); conn.close()

    items = []
    for cid, title, summary in rows:
        js = summary if isinstance(summary, dict) else json.loads(summary or "{}")
        items.append({"cluster_id": cid, "title": title, "summary": js})

    daily_path = render_pages(items)
    notify_slack(items, daily_path)
    print("[publish]", daily_path, "web/dist/index.html")

if __name__ == "__main__":
    main()
