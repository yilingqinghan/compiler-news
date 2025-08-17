import yaml, feedparser, requests
from datetime import datetime
from pipelines.util import ensure_tables, pg_conn, sha1

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"

def entry_html(e):
    # content:encoded 优先，其次 summary
    if hasattr(e, "content") and e.content:
        for c in e.content:
            v = getattr(c, "value", None)
            if v: return v
    return getattr(e, "summary", "") or ""

def main():
    ensure_tables()
    with open("sources.yml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    feeds = cfg.get("feeds", [])
    session = requests.Session(); session.headers.update({"User-Agent": UA})
    conn = pg_conn()
    added = 0

    for f in feeds:
        src = f["name"]; url = f["url"]
        parsed = feedparser.parse(url)
        for e in parsed.entries:
            link = e.get("link") or e.get("id")
            if not link: continue
            title = (e.get("title") or "").strip()
            if hasattr(e, "published_parsed") and e.published_parsed:
                ts = datetime(*e.published_parsed[:6])
            elif hasattr(e, "updated_parsed") and e.updated_parsed:
                ts = datetime(*e.updated_parsed[:6])
            else:
                ts = datetime.utcnow()

            html = ""
            # 先试抓原文
            try:
                resp = session.get(link, timeout=20)
                if resp.ok and len(resp.text) > 200:
                    html = resp.text
            except Exception:
                pass
            # 抓不到就用 feed 自带的 content/summary 兜底
            if not html or len(html) < 200:
                html = entry_html(e)

            cur = conn.cursor()
            cur.execute("""
            INSERT INTO articles_raw (id, source, url, title, ts, raw_html)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING;
            """, (sha1(link), src, link, title, ts, html))
            conn.commit()
            cur.close()
            added += 1

    conn.close()
    print(f"[ingest_rss] added ~{added} items")

if __name__ == "__main__":
    main()