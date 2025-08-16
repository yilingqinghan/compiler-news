import yaml, feedparser, requests
from datetime import datetime
from pipelines.util import ensure_tables, pg_conn, sha1

def fetch_feed(url: str):
    return feedparser.parse(url)

def upsert_raw(conn, item):
    cur = conn.cursor()
    cur.execute("""
    INSERT INTO articles_raw (id, source, url, title, ts, raw_html)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (id) DO NOTHING;
    """, (item["id"], item["source"], item["url"], item["title"], item["ts"], item["raw_html"]))
    conn.commit(); cur.close()

def main():
    ensure_tables()
    with open("sources.yml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    feeds = cfg.get("feeds", [])
    session = requests.Session()
    conn = pg_conn()
    added = 0

    for f in feeds:
        src = f["name"]; url = f["url"]
        parsed = fetch_feed(url)
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
            try:
                resp = session.get(link, timeout=20)
                resp.raise_for_status()
                html = resp.text
            except Exception:
                pass

            upsert_raw(conn, {
                "id": sha1(link),
                "source": src,
                "url": link,
                "title": title,
                "ts": ts,
                "raw_html": html
            })
            added += 1

    conn.close()
    print(f"[ingest_rss] added ~{added} items")

if __name__ == "__main__":
    main()
