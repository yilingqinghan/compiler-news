import os
import yaml
import feedparser
import requests
from urllib.parse import urlparse
from datetime import datetime, timezone
from pipelines.util import ensure_tables, pg_conn, sha1

# ---- 可配置项（来自 .env，给默认值） ----
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "20"))
MAX_ITEMS_PER_FEED = int(os.getenv("MAX_ITEMS_PER_FEED", "200"))
USER_AGENT = os.getenv(
    "REQUEST_UA",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari"
)

def _load_sources():
    """同时兼容 sources.yml 顶层为 rss: 或 feeds: 的两种写法；可选 allow_hosts 白名单。"""
    with open("sources.yml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    feeds = cfg.get("rss") or cfg.get("feeds") or []
    allow_hosts = set(cfg.get("allow_hosts") or [])
    return feeds, allow_hosts

def _host_allowed(url: str, allow_hosts: set) -> bool:
    if not allow_hosts:
        return True
    try:
        h = urlparse(url).hostname or ""
        # 允许子域：任何以白名单尾缀结尾的 host
        return any(h == ah or h.endswith("." + ah) for ah in allow_hosts)
    except Exception:
        return False

def _entry_primary_link(e) -> str:
    # 优先 entry.link；其次 entry.id；再看 links 数组里的 alternate/self
    link = getattr(e, "link", None) or getattr(e, "id", None)
    if link:
        return link
    try:
        for li in getattr(e, "links", []) or []:
            if li.get("rel") in (None, "alternate", "self"):
                return li.get("href")
    except Exception:
        pass
    return None

def _entry_title(e) -> str:
    t = getattr(e, "title", None) or ""
    t = t.strip()
    return t or "(no title)"

def _entry_ts(e):
    # 优先 published_parsed，其次 updated_parsed；再兜底为现在
    try:
        if getattr(e, "published_parsed", None):
            return datetime(*e.published_parsed[:6])
        if getattr(e, "updated_parsed", None):
            return datetime(*e.updated_parsed[:6])
    except Exception:
        pass
    return datetime.now(timezone.utc)

def _entry_html_payload(e) -> str:
    # feed 自带内容兜底：content:encoded / content / summary
    # feedparser 把 content:encoded 解析进 e.content[].value
    try:
        if hasattr(e, "content") and e.content:
            for c in e.content:
                v = c.get("value") if isinstance(c, dict) else getattr(c, "value", None)
                if v:
                    return v
    except Exception:
        pass
    return getattr(e, "summary", "") or ""

def main():
    ensure_tables()
    feeds, allow_hosts = _load_sources()
    print(f"[ingest_rss] loaded sources: {len(feeds)} ({'rss' if feeds else 'none'})")
    if allow_hosts:
        print(f"[ingest_rss] allow_hosts = {sorted(allow_hosts)}")

    if not feeds:
        print("[ingest_rss] no sources found (check sources.yml: use 'rss:' or 'feeds:').")
        return

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    conn = pg_conn()
    total_seen = 0
    inserted = 0
    skipped_host = 0
    fetched_ok = 0
    used_feed_body = 0

    for f in feeds:
        src = str(f.get("name") or "unnamed").strip()
        url = f.get("url")
        if not url:
            print(f"[ingest_rss] WARN: source '{src}' missing url, skip.")
            continue

        parsed = feedparser.parse(url)
        entries = parsed.entries or []
        if not entries:
            print(f"[ingest_rss] WARN: empty feed: {src} -> {url}")
            continue

        # 只取前 N 条（可配置）
        entries = entries[:MAX_ITEMS_PER_FEED]
        print(f"[ingest_rss] {src}: {len(entries)} items (url={url})")

        for e in entries:
            link = _entry_primary_link(e)
            if not link:
                # 没有链接就没法去重/抓原文，直接略过
                continue
            if not _host_allowed(link, allow_hosts):
                skipped_host += 1
                continue

            title = _entry_title(e)
            ts = _entry_ts(e)

            # 先尝试抓原文
            html = ""
            try:
                resp = session.get(link, timeout=REQUEST_TIMEOUT, allow_redirects=True)
                if resp.ok and resp.text and len(resp.text) > 200:
                    html = resp.text
                    fetched_ok += 1
            except Exception:
                pass

            # 抓不到原文就用 feed 自带的内容兜底
            if not html or len(html) < 200:
                html = _entry_html_payload(e)
                if html:
                    used_feed_body += 1

            # 入库（ON CONFLICT DO NOTHING）
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO articles_raw (id, source, url, title, ts, raw_html)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING;
                """,
                (sha1(link), src, link, title, ts, html),
            )
            conn.commit()
            # rowcount 在 INSERT DO NOTHING 时 1=插入，0=已存在
            if cur.rowcount and cur.rowcount > 0:
                inserted += 1
            cur.close()
            total_seen += 1

    conn.close()
    print(
        "[ingest_rss] done: seen=%d, inserted=%d, fetched_ok=%d, used_feed_body=%d, skipped_host=%d"
        % (total_seen, inserted, fetched_ok, used_feed_body, skipped_host)
    )

if __name__ == "__main__":
    main()