import os
import yaml
import feedparser
import requests
from urllib.parse import urlparse
from datetime import datetime, timezone
import time, calendar, re
from pipelines.util import ensure_tables, pg_conn, sha1

# ---- 可配置项（来自 .env，给默认值） ----
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "20"))
MAX_ITEMS_PER_FEED = int(os.getenv("MAX_ITEMS_PER_FEED", "200"))
USER_AGENT = os.getenv(
    "REQUEST_UA",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari"
)
TIME_WINDOW_DAYS = int(os.getenv("TIME_WINDOW_DAYS", "7"))
USE_GITHUB_API = os.getenv("USE_GITHUB_API", "1") == "1"  # 默认开启；设为 0 仅用纯 RSS
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "").strip()
MAX_PAGES = int(os.getenv("MAX_PAGES", "5"))  # 分页最多抓多少页

def _load_sources():
    """同时兼容 sources.yml 顶层为 rss: 或 feeds: 的两种写法；可选 allow_hosts 白名单。"""
    with open("sources.yml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    feeds = cfg.get("rss") or cfg.get("feeds") or []
    allow_hosts = set(cfg.get("allow_hosts") or [])
    return feeds, allow_hosts

def _epoch(st):
    if not st:
        return 0
    return int(calendar.timegm(st))

def _within_window(epoch_ts: int, now_epoch: int, days: int) -> bool:
    return epoch_ts >= (now_epoch - days * 86400)

def _append_page(url: str, page: int) -> str:
    if page <= 1:
        return url
    sep = "&" if "?" in url else "?"
    print(f"{url}{sep}page={page}")
    return f"{url}{sep}page={page}"

def _fetch_feed_pages(url: str, max_pages: int, days_window: int) -> list:
    seen = set()
    items = []
    now_ep = int(time.time())

    for p in range(1, max_pages + 1):
        u = _append_page(url, p)
        print(f"[ingest_rss][debug] fetch page={p} url={u}")
        feed = feedparser.parse(u)
        if feed.bozo:
            print(f"[ingest_rss][warn] bozo on page={p}: {getattr(feed, 'bozo_exception', None)}")
            break

        page_entries = feed.entries or []
        page_total = len(page_entries)
        page_new = 0
        any_new = False

        for e in page_entries:
            eid = getattr(e, "id", None) or getattr(e, "link", None)
            if not eid or eid in seen:
                continue
            seen.add(eid)

            ts_ep = _epoch(getattr(e, "published_parsed", None) or getattr(e, "updated_parsed", None))
            if ts_ep and _within_window(ts_ep, now_ep, days_window):
                items.append(e)
                page_new += 1
                any_new = True

        print(f"[ingest_rss][debug] page={p} stats: total={page_total}, in_window={page_new}")

        # 这一页有条目但没有任何在窗口内的 -> 后续更旧，早停
        if page_total > 0 and not any_new:
            print(f"[ingest_rss][debug] early stop at page={p} (page has no in-window items)")
            break

        if page_total == 0:
            break

    print(f"[ingest_rss][debug] kept {len(items)} items within {days_window}d window for {url}")
    return items

def fetch_feed_smart(url: str) -> list:
    """按源策略选择是否翻页"""
    h = (urlparse(url).hostname or "").lower()
    # Discourse (llvm / swift 等)
    if h in ("discourse.llvm.org", "forums.swift.org"):
        return _fetch_feed_pages(url, max_pages=MAX_PAGES, days_window=TIME_WINDOW_DAYS)
    # GitHub Atom（commits/releases）
    if h == "github.com" and url.endswith((".atom", ".xml")):
        return _fetch_feed_pages(url, max_pages=MAX_PAGES, days_window=TIME_WINDOW_DAYS)
    # Sourceware inbox（gcc-*）
    if h == "inbox.sourceware.org" and url.endswith(".atom"):
        return _fetch_feed_pages(url, max_pages=MAX_PAGES, days_window=TIME_WINDOW_DAYS)
    # 默认一页
    feed = feedparser.parse(url)
    return list(feed.entries or [])

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

# -------- GitHub REST 作为“辅助源”（可选） --------
_GH_COMMITS_RE  = re.compile(r"https?://github\.com/(?P<owner>[^/]+)/(?P<repo>[^/]+)/commits/(?P<branch>.+)\.atom$")
_GH_RELEASES_RE = re.compile(r"https?://github\.com/(?P<owner>[^/]+)/(?P<repo>[^/]+)/releases\.atom$")
_GH_PULLS_RE    = re.compile(r"https?://github\.com/(?P<owner>[^/]+)/(?P<repo>[^/]+)/pulls\.atom$")
_GH_ISSUES_RE   = re.compile(r"https?://github\.com/(?P<owner>[^/]+)/(?P<repo>[^/]+)/issues\.atom$")

def _gh_headers():
    h = {"Accept": "application/vnd.github+json", "User-Agent": USER_AGENT}
    if GITHUB_TOKEN:
        h["Authorization"] = f"Bearer {GITHUB_TOKEN}"
    return h

def _to_fp_entry(id_: str, link: str, title: str, ts_iso: str):
    """把 GitHub API 返回转成 feedparser entry 近似对象（只用到的字段）"""
    class E: pass
    e = E()
    e.id = id_
    e.link = link
    e.title = title
    # 将 ISO 时间转 struct_time（UTC）
    try:
        dt = datetime.fromisoformat(ts_iso.replace("Z","+00:00"))
        e.published_parsed = dt.utctimetuple()
    except Exception:
        e.published_parsed = None
    e.updated_parsed = e.published_parsed
    e.summary = ""
    e.content = []
    return e

def _fetch_github_commits(owner: str, repo: str, branch: str, days: int, max_pages: int) -> list:
    import requests
    now_ep = int(time.time())
    out = []
    per_page = 100
    for p in range(1, max_pages + 1):
        url = f"https://api.github.com/repos/{owner}/{repo}/commits?sha={branch}&per_page={per_page}&page={p}"
        r = requests.get(url, headers=_gh_headers(), timeout=REQUEST_TIMEOUT)
        if r.status_code >= 400:
            break
        page = r.json() or []
        new = 0; early = False
        for c in page:
            ts = c.get("commit", {}).get("committer", {}).get("date") or c.get("commit", {}).get("author", {}).get("date")
            if ts:
                try:
                    ep = int(datetime.fromisoformat(ts.replace("Z","+00:00")).timestamp())
                    if not _within_window(ep, now_ep, days):
                        early = True
                        continue
                except Exception:
                    pass
            sha = c.get("sha")
            link = c.get("html_url") or (f"https://github.com/{owner}/{repo}/commit/{sha}" if sha else None)
            title = (c.get("commit", {}).get("message") or "").splitlines()[0][:200]
            if sha and link:
                out.append(_to_fp_entry(sha, link, title, ts or ""))
                new += 1
        if new == 0 or early:
            break
    return out

def _fetch_github_releases(owner: str, repo: str, days: int, max_pages: int) -> list:
    import requests
    now_ep = int(time.time())
    out = []
    per_page = 100
    for p in range(1, max_pages + 1):
        url = f"https://api.github.com/repos/{owner}/{repo}/releases?per_page={per_page}&page={p}"
        r = requests.get(url, headers=_gh_headers(), timeout=REQUEST_TIMEOUT)
        if r.status_code >= 400:
            break
        page = r.json() or []
        new = 0; early = False
        for rel in page:
            ts = rel.get("published_at") or rel.get("created_at")
            if ts:
                try:
                    ep = int(datetime.fromisoformat(ts.replace("Z","+00:00")).timestamp())
                    if not _within_window(ep, now_ep, days):
                        early = True
                        continue
                except Exception:
                    pass
            rid = str(rel.get("id") or rel.get("tag_name") or rel.get("html_url"))
            link = rel.get("html_url")
            title = rel.get("name") or rel.get("tag_name") or "(release)"
            if rid and link:
                out.append(_to_fp_entry(rid, link, title, ts or ""))
                new += 1
        if new == 0 or early:
            break
    return out

def _fetch_github_pulls(owner: str, repo: str, days: int, max_pages: int) -> list:
    import requests
    now_ep = int(time.time())
    out = []
    per_page = 100
    for p in range(1, max_pages + 1):
        url = f"https://api.github.com/repos/{owner}/{repo}/pulls?state=all&sort=updated&direction=desc&per_page={per_page}&page={p}"
        r = requests.get(url, headers=_gh_headers(), timeout=REQUEST_TIMEOUT)
        if r.status_code >= 400:
            break
        page = r.json() or []
        new = 0; early = False
        for pr in page:
            ts = pr.get("merged_at") or pr.get("closed_at") or pr.get("updated_at") or pr.get("created_at")
            if ts:
                try:
                    ep = int(datetime.fromisoformat(ts.replace("Z","+00:00")).timestamp())
                    if not _within_window(ep, now_ep, days):
                        early = True
                        continue
                except Exception:
                    pass
            rid = str(pr.get("id") or pr.get("number"))
            link = pr.get("html_url")
            title = pr.get("title") or "(pull request)"
            if rid and link:
                out.append(_to_fp_entry(rid, link, title, ts or ""))
                new += 1
        if new == 0 or early:
            break
    return out

def _fetch_github_issues(owner: str, repo: str, days: int, max_pages: int) -> list:
    import requests
    now_ep = int(time.time())
    out = []
    per_page = 100
    for p in range(1, max_pages + 1):
        url = f"https://api.github.com/repos/{owner}/{repo}/issues?state=all&sort=updated&direction=desc&per_page={per_page}&page={p}"
        r = requests.get(url, headers=_gh_headers(), timeout=REQUEST_TIMEOUT)
        if r.status_code >= 400:
            break
        page = r.json() or []
        new = 0; early = False
        for it in page:
            # 排除 PR（issues API 会返回 PR）
            if "pull_request" in it:
                continue
            ts = it.get("closed_at") or it.get("updated_at") or it.get("created_at")
            if ts:
                try:
                    ep = int(datetime.fromisoformat(ts.replace("Z","+00:00")).timestamp())
                    if not _within_window(ep, now_ep, days):
                        early = True
                        continue
                except Exception:
                    pass
            rid = str(it.get("id") or it.get("number"))
            link = it.get("html_url")
            title = it.get("title") or "(issue)"
            if rid and link:
                out.append(_to_fp_entry(rid, link, title, ts or ""))
                new += 1
        if new == 0 or early:
            break
    return out

def maybe_fetch_github_via_api(url: str) -> list | None:
    """当 URL 是 GitHub commits/releases 的 Atom 时，且允许使用 API，则用 REST 拿更全数据"""
    if not USE_GITHUB_API:
        return None
    m = _GH_COMMITS_RE.match(url)
    if m:
        gd = m.groupdict()
        return _fetch_github_commits(gd["owner"], gd["repo"], gd["branch"], TIME_WINDOW_DAYS, MAX_PAGES)
    m = _GH_RELEASES_RE.match(url)
    if m:
        gd = m.groupdict()
        return _fetch_github_releases(gd["owner"], gd["repo"], TIME_WINDOW_DAYS, MAX_PAGES)
    m = _GH_PULLS_RE.match(url)
    if m:
        gd = m.groupdict()
        return _fetch_github_pulls(gd["owner"], gd["repo"], TIME_WINDOW_DAYS, MAX_PAGES)
    m = _GH_ISSUES_RE.match(url)
    if m:
        gd = m.groupdict()
        return _fetch_github_issues(gd["owner"], gd["repo"], TIME_WINDOW_DAYS, MAX_PAGES)
    return None

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

        # 优先用 GitHub API（更全）；否则按源策略分页抓取 RSS
        entries = maybe_fetch_github_via_api(url)
        if entries is None:
            entries = fetch_feed_smart(url)

        if not entries:
            print(f"[ingest_rss] WARN: empty feed: {src} -> {url}")
            continue

        # 统一截断（最终安全阈值）
        if MAX_ITEMS_PER_FEED > 0:
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

from pipelines.util import run_cli
if __name__ == "__main__":
    run_cli(main)