# pipelines/ingest_rss.py
# ======================================================================
#  RSS / Atom 抓取与入库
#  - 智能分页（Discourse / GitHub Atom / Sourceware）
#  - GitHub 辅助：REST API 拉 commits / releases / pulls / issues（可选）
#  - 统一风格日志：彩色、阶段耗时、spinner、进度条、每页统计
#  - 不修改表结构；写入 articles_raw（ON CONFLICT DO NOTHING）
# ======================================================================

from __future__ import annotations
import os
import yaml
import feedparser
import requests
from urllib.parse import urlparse
from datetime import datetime, timezone
import time, calendar, re

from pipelines.util import ensure_tables, pg_conn, sha1, run_cli
from pipelines.logging_utils import (
    info, debug, warn, error, success,
    kv_line, kv_table, status, new_progress, step
)

# --------------------------- 可配置项（.env 优先） ---------------------------
REQUEST_TIMEOUT   = int(os.getenv("REQUEST_TIMEOUT", "20"))
MAX_ITEMS_PER_FEED = int(os.getenv("MAX_ITEMS_PER_FEED", "200"))
USER_AGENT = os.getenv(
    "REQUEST_UA",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari"
)
TIME_WINDOW_DAYS  = int(os.getenv("TIME_WINDOW_DAYS", "7"))
USE_GITHUB_API    = os.getenv("USE_GITHUB_API", "1") == "1"  # 默认开启；0=仅用纯 RSS
GITHUB_TOKEN      = os.getenv("GITHUB_TOKEN", "").strip()
MAX_PAGES         = int(os.getenv("MAX_PAGES", "5"))         # 分页最多抓多少页


# --------------------------- 源配置 ---------------------------
def _load_sources():
    """兼容 sources.yml 顶层为 rss: 或 feeds:；可选 allow_hosts 白名单。"""
    with open("sources.yml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    feeds = cfg.get("rss") or cfg.get("feeds") or []
    allow_hosts = set(cfg.get("allow_hosts") or [])
    return feeds, allow_hosts


# --------------------------- 时间与窗口 ---------------------------
def _epoch(st):
    if not st:
        return 0
    return int(calendar.timegm(st))

def _within_window(epoch_ts: int, now_epoch: int, days: int) -> bool:
    return epoch_ts >= (now_epoch - days * 86400)


# --------------------------- 分页 URL 辅助 ---------------------------
def _append_page(url: str, page: int) -> str:
    """生成分页 URL；page=1 返回原始 URL。"""
    if page <= 1:
        return url
    sep = "&" if "?" in url else "?"
    u = f"{url}{sep}page={page}"
    debug(f"[ingest_rss] page_url: {u}")
    return u


# --------------------------- 分页抓取（带早停） ---------------------------
def _fetch_feed_pages(url: str, max_pages: int, days_window: int) -> list:
    """
    分页拉取 RSS/Atom：
    - 每页统计 total / in_window
    - 早停规则：本页有条目但 in_window=0 -> 后续更旧，停止
    """
    seen = set()
    items = []
    now_ep = int(time.time())

    for p in range(1, max_pages + 1):
        u = _append_page(url, p)
        info(f"[ingest_rss] fetch page={p} url={u}")
        with status(f"[ingest_rss] 解析第 {p} 页 …", spinner="dots"):
            feed = feedparser.parse(u)

        if getattr(feed, "bozo", False):
            warn(f"[ingest_rss] bozo on page={p}: {getattr(feed, 'bozo_exception', None)}")
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

        debug(f"[ingest_rss] page={p} stats: total={page_total}, in_window={page_new}")

        # 这一页有条目但没有任何在窗口内的 -> 后续更旧，早停
        if page_total > 0 and not any_new:
            info(f"[ingest_rss] early stop at page={p}（本页窗口内=0）")
            break

        # 本页没有条目 -> 无更多页
        if page_total == 0:
            break

    info(f"[ingest_rss] kept {len(items)} items within {days_window}d window for {url}")
    return items


# --------------------------- 智能抓取（源策略） ---------------------------
def fetch_feed_smart(url: str) -> list:
    """按 host 策略决定是否分页。"""
    h = (urlparse(url).hostname or "").lower()
    # Discourse（llvm / swift 等）
    if h in ("discourse.llvm.org", "forums.swift.org"):
        return _fetch_feed_pages(url, max_pages=MAX_PAGES, days_window=TIME_WINDOW_DAYS)
    # GitHub Atom（commits / releases / pulls / issues）
    if h == "github.com" and url.endswith((".atom", ".xml")):
        return _fetch_feed_pages(url, max_pages=MAX_PAGES, days_window=TIME_WINDOW_DAYS)
    # Sourceware inbox（gcc-*）
    if h == "inbox.sourceware.org" and url.endswith(".atom"):
        return _fetch_feed_pages(url, max_pages=MAX_PAGES, days_window=TIME_WINDOW_DAYS)
    # 默认：一页
    with status("[ingest_rss] 解析单页 feed …", spinner="dots"):
        feed = feedparser.parse(url)
    return list(feed.entries or [])


# --------------------------- 主机白名单 ---------------------------
def _host_allowed(url: str, allow_hosts: set) -> bool:
    """允许子域：任何以白名单尾缀结尾的 host。"""
    if not allow_hosts:
        return True
    try:
        h = urlparse(url).hostname or ""
        return any(h == ah or h.endswith("." + ah) for ah in allow_hosts)
    except Exception:
        return False


# --------------------------- entry 提取 ---------------------------
def _entry_primary_link(e) -> str | None:
    """优先 entry.link；其次 entry.id；再看 links 数组里的 alternate/self。"""
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
    """优先 published_parsed，其次 updated_parsed；再兜底为现在（UTC）。"""
    try:
        if getattr(e, "published_parsed", None):
            return datetime(*e.published_parsed[:6], tzinfo=timezone.utc)
        if getattr(e, "updated_parsed", None):
            return datetime(*e.updated_parsed[:6], tzinfo=timezone.utc)
    except Exception:
        pass
    return datetime.now(timezone.utc)

def _entry_html_payload(e) -> str:
    """
    feed 自带内容兜底：content:encoded / content / summary
    feedparser 会把 content:encoded 解析进 e.content[].value
    """
    try:
        if hasattr(e, "content") and e.content:
            for c in e.content:
                v = c.get("value") if isinstance(c, dict) else getattr(c, "value", None)
                if v:
                    return v
    except Exception:
        pass
    return getattr(e, "summary", "") or ""


# --------------------------- GitHub REST 作为“辅助源” ---------------------------
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
    """把 GitHub API 返回转成 feedparser entry 近似对象（只用到的字段）。"""
    class E: ...
    e = E()
    e.id = id_
    e.link = link
    e.title = title
    try:
        dt = datetime.fromisoformat(ts_iso.replace("Z", "+00:00"))
        e.published_parsed = dt.utctimetuple()
    except Exception:
        e.published_parsed = None
    e.updated_parsed = e.published_parsed
    e.summary = ""
    e.content = []
    return e

def _fetch_github_commits(owner: str, repo: str, branch: str, days: int, max_pages: int) -> list:
    """REST: /repos/{owner}/{repo}/commits?sha={branch}（按窗口早停）"""
    now_ep = int(time.time())
    out = []
    per_page = 100
    for p in range(1, max_pages + 1):
        url = f"https://api.github.com/repos/{owner}/{repo}/commits?sha={branch}&per_page={per_page}&page={p}"
        with status(f"[github] commits p{p} …", spinner="line"):
            r = requests.get(url, headers=_gh_headers(), timeout=REQUEST_TIMEOUT)
        if r.status_code >= 400:
            warn(f"[github] commits http {r.status_code}: {url}")
            break
        page = r.json() or []
        page_new = 0; early = False
        for c in page:
            ts = c.get("commit", {}).get("committer", {}).get("date") or c.get("commit", {}).get("author", {}).get("date")
            if ts:
                try:
                    ep = int(datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp())
                    if not _within_window(ep, now_ep, days):  # 修复：days 正确传入
                        early = True
                        continue
                except Exception:
                    pass
            sha = c.get("sha")
            link = c.get("html_url") or (f"https://github.com/{owner}/{repo}/commit/{sha}" if sha else None)
            title = (c.get("commit", {}).get("message") or "").splitlines()[0][:200]
            if sha and link:
                out.append(_to_fp_entry(sha, link, title, ts or ""))
                page_new += 1
        info(f"[github] commits page={p} stats: total={len(page)}, in_window={page_new}")
        if page_new == 0 or early:
            break
    return out

def _fetch_github_releases(owner: str, repo: str, days: int, max_pages: int) -> list:
    now_ep = int(time.time())
    out = []
    per_page = 100
    for p in range(1, max_pages + 1):
        url = f"https://api.github.com/repos/{owner}/{repo}/releases?per_page={per_page}&page={p}"
        with status(f"[github] releases p{p} …", spinner="line"):
            r = requests.get(url, headers=_gh_headers(), timeout=REQUEST_TIMEOUT)
        if r.status_code >= 400:
            warn(f"[github] releases http {r.status_code}: {url}")
            break
        page = r.json() or []
        page_new = 0; early = False
        for rel in page:
            ts = rel.get("published_at") or rel.get("created_at")
            if ts:
                try:
                    ep = int(datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp())
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
                page_new += 1
        info(f"[github] releases page={p} stats: total={len(page)}, in_window={page_new}")
        if page_new == 0 or early:
            break
    return out

def _fetch_github_pulls(owner: str, repo: str, days: int, max_pages: int) -> list:
    now_ep = int(time.time())
    out = []
    per_page = 100
    for p in range(1, max_pages + 1):
        url = f"https://api.github.com/repos/{owner}/{repo}/pulls?state=all&sort=updated&direction=desc&per_page={per_page}&page={p}"
        with status(f"[github] pulls p{p} …", spinner="line"):
            r = requests.get(url, headers=_gh_headers(), timeout=REQUEST_TIMEOUT)
        if r.status_code >= 400:
            warn(f"[github] pulls http {r.status_code}: {url}")
            break
        page = r.json() or []
        page_new = 0; early = False
        for pr in page:
            ts = pr.get("merged_at") or pr.get("closed_at") or pr.get("updated_at") or pr.get("created_at")
            if ts:
                try:
                    ep = int(datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp())
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
                page_new += 1
        info(f"[github] pulls page={p} stats: total={len(page)}, in_window={page_new}")
        if page_new == 0 or early:
            break
    return out

def _fetch_github_issues(owner: str, repo: str, days: int, max_pages: int) -> list:
    now_ep = int(time.time())
    out = []
    per_page = 100
    for p in range(1, max_pages + 1):
        url = f"https://api.github.com/repos/{owner}/{repo}/issues?state=all&sort=updated&direction=desc&per_page={per_page}&page={p}"
        with status(f"[github] issues p{p} …", spinner="line"):
            r = requests.get(url, headers=_gh_headers(), timeout=REQUEST_TIMEOUT)
        if r.status_code >= 400:
            warn(f"[github] issues http {r.status_code}: {url}")
            break
        page = r.json() or []
        page_new = 0; early = False
        for it in page:
            # 排除 PR（issues API 也会返回 PR）
            if "pull_request" in it:
                continue
            ts = it.get("closed_at") or it.get("updated_at") or it.get("created_at")
            if ts:
                try:
                    ep = int(datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp())
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
                page_new += 1
        info(f"[github] issues page={p} stats: total={len(page)}, in_window={page_new}")
        if page_new == 0 or early:
            break
    return out

def maybe_fetch_github_via_api(url: str) -> list | None:
    """
    当 URL 是 GitHub commits/releases/pulls/issues 的 Atom 时，
    且允许使用 API（USE_GITHUB_API=1），则用 REST 拉更全数据。
    """
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


# --------------------------- 主流程 ---------------------------
@step("Ingest RSS")
def main():
    ensure_tables()
    feeds, allow_hosts = _load_sources()
    kv_line("[ingest_rss] 源加载", sources=len(feeds), mode=("rss" if feeds else "none"))
    if allow_hosts:
        kv_line("[ingest_rss] allow_hosts", hosts=", ".join(sorted(allow_hosts)))

    if not feeds:
        warn("[ingest_rss] 未发现任何源（检查 sources.yml: rss:/feeds:）")
        return

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    conn = pg_conn()
    total_seen = inserted = skipped_host = fetched_ok = used_feed_body = 0

    # 总进度：按源推进
    with new_progress() as progress:
        t_all = progress.add_task("抓取 RSS 源", total=len(feeds))

        for f in feeds:
            src = str(f.get("name") or "unnamed").strip()
            url = f.get("url")
            if not url:
                warn(f"[ingest_rss] 源缺少 url，跳过：{src}")
                progress.advance(t_all)
                continue

            # 子任务：本源条目进度
            t_src = progress.add_task(f"{src}", total=None)

            # 选择策略（GitHub API 或 Smart RSS）
            with status(f"[{src}] 选择抓取策略 …", spinner="bouncingBar"):
                entries = maybe_fetch_github_via_api(url)
                strategy = "GitHub API" if entries is not None else "Smart RSS"
                if entries is None:
                    entries = fetch_feed_smart(url)
                kv_line(f"[{src}] 抓取策略", strategy=strategy)

            if not entries:
                warn(f"[ingest_rss] 空 feed：{src} -> {url}")
                progress.update(t_src, completed=1, total=1)
                progress.advance(t_all)
                continue

            # 统一截断（最终安全阈值）
            if MAX_ITEMS_PER_FEED > 0:
                entries = entries[:MAX_ITEMS_PER_FEED]

            info(f"[{src}] 条目数：{len(entries)}（url={url}）")
            progress.update(t_src, total=len(entries), completed=0)

            # 条目循环：抓原文 -> 兜底 -> 入库
            for i, e in enumerate(entries, 1):
                progress.update(t_src, advance=1)

                link = _entry_primary_link(e)
                if not link:
                    debug(f"[{src}] 条目无链接，略过")
                    continue
                if not _host_allowed(link, allow_hosts):
                    skipped_host += 1
                    continue

                title = _entry_title(e)
                ts = _entry_ts(e)

                html = ""
                try:
                    with status(f"[{src}] 抓原文 {i}/{len(entries)}", spinner="line"):
                        resp = session.get(link, timeout=REQUEST_TIMEOUT, allow_redirects=True)
                    if resp.ok and resp.text and len(resp.text) > 200:
                        html = resp.text
                        fetched_ok += 1
                except Exception as ex:
                    debug(f"[{src}] 抓原文异常：{ex}")

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
                if cur.rowcount and cur.rowcount > 0:
                    inserted += 1
                cur.close()
                total_seen += 1

            progress.advance(t_all)
            success(f"[{src}] 完成 ✓")

    conn.close()
    kv_table("[ingest_rss] 汇总", {
        "seen": total_seen,
        "inserted": inserted,
        "fetched_ok": fetched_ok,
        "used_feed_body": used_feed_body,
        "skipped_host": skipped_host,
        "window_days": TIME_WINDOW_DAYS,
        "max_pages": MAX_PAGES,
    })


# --------------------------- CLI 入口 ---------------------------
if __name__ == "__main__":
    run_cli(main)