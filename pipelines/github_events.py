# compiler-intel / github_events.py
# ======================================================================
# GitHub 事件抓取（Issues / PRs / Releases / Commits）
# - 统一风格日志：阶段耗时、spinner、进度条、汇总表
# - 默认保持原行为；可选开启分页（环境变量）
# - 不修改表结构；写入 articles_raw（与旧版兼容）
# ======================================================================

from __future__ import annotations
import os
import yaml
import requests
from typing import Dict, Iterable, Tuple, Optional
from datetime import datetime, timezone

from pipelines.util import ensure_tables, pg_conn, sha1, run_cli
from pipelines.logging_utils import (
    info, debug, warn, error, success,
    kv_line, kv_table, status, new_progress, step
)

# --------------------------- 配置项（可环境变量覆盖） ---------------------------
GITHUB_TOKEN       = os.getenv("GITHUB_TOKEN")
GITHUB_PAGINATE    = os.getenv("GITHUB_PAGINATE", "0") == "1"   # 1 开分页
GITHUB_MAX_PAGES   = int(os.getenv("GITHUB_MAX_PAGES", "3"))    # 最多翻几页
REQ_TIMEOUT        = int(os.getenv("GITHUB_REQ_TIMEOUT", "20")) # 请求超时秒

# --------------------------- HTTP 头 ---------------------------
def _headers() -> Dict[str, str]:
    """统一 GitHub API 请求头（带可选 token）"""
    h = {"Accept": "application/vnd.github+json"}
    if GITHUB_TOKEN:
        h["Authorization"] = f"Bearer {GITHUB_TOKEN}"
    return h

# --------------------------- DB 写入 ---------------------------
def _insert_article(conn, src: str, link: str, title: str, ts_iso: str, html: str = "") -> int:
    """写入 articles_raw；返回写入行数（0=已存在）"""
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO articles_raw (id, source, url, title, ts, raw_html)
        VALUES (%s,%s,%s,%s,%s,%s)
        ON CONFLICT (id) DO NOTHING;
        """,
        (sha1(link), src, link, title or "(no title)", ts_iso, html or "")
    )
    conn.commit()
    n = cur.rowcount or 0
    cur.close()
    return n

# --------------------------- Link 分页解析 ---------------------------
def _parse_next_link(resp: requests.Response) -> Optional[str]:
    """
    从 Link 头里解析 rel="next"；若无则返回 None
    形如：<https://api.github.com/...&page=2>; rel="next", <...>; rel="last"
    """
    link = resp.headers.get("Link", "")
    if not link:
        return None
    # 粗解析：分号分片，找 rel="next"
    parts = [p.strip() for p in link.split(",")]
    for p in parts:
        if 'rel="next"' in p:
            # <URL>; rel="next"
            lt = p.find("<")
            gt = p.find(">")
            if 0 <= lt < gt:
                return p[lt + 1:gt]
    return None

# --------------------------- GitHub 抓取（含可选分页） ---------------------------
def _fetch_github_json_stream(url: str) -> Iterable[Tuple[requests.Response, dict]]:
    """
    访问 GitHub API，yield (resp, item)。
    默认仅首页；若 GITHUB_PAGINATE=1，则按 Link: rel="next" 翻到 GITHUB_MAX_PAGES。
    """
    page = 1
    cur_url = url
    while True:
        with status(f"[GitHub] GET {cur_url}", spinner="bouncingBar"):
            resp = requests.get(cur_url, headers=_headers(), timeout=REQ_TIMEOUT)

        # 速率信息（有就打出来，方便观察额度）
        rl_rem = resp.headers.get("X-RateLimit-Remaining")
        rl_res = resp.headers.get("X-RateLimit-Reset")
        if rl_rem is not None:
            kv_line("[github] rate limit", remaining=rl_rem, reset=rl_res or "-")

        if resp.status_code != 200:
            warn(f"[github] http {resp.status_code} -> {cur_url}")
            break

        data = resp.json() if resp.content else []
        if isinstance(data, dict):
            # 某些资源（例如单对象）直接是 dict；统一成列表
            data = [data]

        for item in data or []:
            yield resp, item

        # 分页控制
        if not GITHUB_PAGINATE:
            break
        if page >= GITHUB_MAX_PAGES:
            debug(f"[github] reach max pages ({GITHUB_MAX_PAGES}) for: {url}")
            break
        next_url = _parse_next_link(resp)
        if not next_url:
            break
        page += 1
        cur_url = next_url

# --------------------------- 主流程 ---------------------------
@step("GitHub Events Ingest")
def main():
    # 0) 准备环境 & 读取配置
    ensure_tables()
    with status("[github_events] 读取 sources.yml …", spinner="dots"):
        with open("sources.yml", "r", encoding="utf-8") as f:
            repos = (yaml.safe_load(f) or {}).get("github_repos", []) or []

    if not repos:
        warn("[github_events] sources.yml 未配置 github_repos，跳过")
        return

    kv_line("[github_events] 参数",
            paginate=int(GITHUB_PAGINATE),
            max_pages=GITHUB_MAX_PAGES,
            timeout=REQ_TIMEOUT,
            token=("yes" if bool(GITHUB_TOKEN) else "no"),
            repos=len(repos))

    # 1) 数据库连接
    conn = pg_conn()

    # 2) 统计计数
    n_repos = 0
    n_requests = 0
    n_http_ok = 0
    n_insert = 0
    n_errors = 0

    # 3) 进度条：按仓库推进
    with new_progress() as progress:
        task_all = progress.add_task("抓取 GitHub 仓库事件", total=len(repos))

        for r in repos:
            n_repos += 1
            owner, repo = r.get("owner"), r.get("repo")
            if not owner or not repo:
                warn(f"[github_events] 配置项缺少 owner/repo：{r}")
                progress.advance(task_all)
                continue

            base = f"https://api.github.com/repos/{owner}/{repo}"
            src  = f"github:{owner}/{repo}"

            # 当前仓库的 4 类资源
            endpoints = [
                ("pulls",    f"{base}/pulls?state=all&per_page=50"),
                ("issues",   f"{base}/issues?state=all&per_page=50"),
                ("releases", f"{base}/releases"),
                ("commits",  f"{base}/commits?per_page=30"),
            ]

            kv_line("[github_events] 仓库", owner=owner, repo=repo)

            # 子任务：当前仓库处理（总量未知，用 spinner+累计即可）
            with status(f"[{owner}/{repo}] 抓取 4 类资源 …", spinner="dots"):
                pass  # 仅视觉提示

            for kind, url in endpoints:
                try:
                    debug(f"[github] {owner}/{repo} <- {kind} {url}")

                    # 按流式迭代（可分页）
                    for resp, e in _fetch_github_json_stream(url):
                        n_requests += 1
                        if resp.status_code == 200:
                            n_http_ok += 1

                        link = e.get("html_url") or e.get("url")
                        if not link:
                            continue

                        # 标题优先级：title > name > commit.message（第一行）
                        msg = (e.get("commit", {}) or {}).get("message", "") or ""
                        title = (e.get("title") or e.get("name") or msg).split("\n")[0] if (e or {}) else "(no title)"

                        # 时间优先级：created_at > published_at > commit.author.date > now
                        ts = (e.get("created_at")
                              or e.get("published_at")
                              or (e.get("commit", {}) or {}).get("author", {}).get("date")
                              or datetime.now(timezone.utc).isoformat())

                        html = e.get("body") or ""
                        wrote = _insert_article(conn, src, link, title, ts, html)
                        n_insert += wrote

                    # endpoint 小结（方便定位某类资源是否总是空）
                    kv_line(f"[{owner}/{repo}] {kind}", done="ok")

                except Exception as ex:
                    n_errors += 1
                    error(f"[github_events] 抓取异常：{owner}/{repo} {kind} -> {ex}")

            progress.advance(task_all)
            success(f"[{owner}/{repo}] 完成 ✓")

    conn.close()

    # 4) 汇总
    kv_table("[github_events] 汇总", {
        "repos": n_repos,
        "requests": n_requests,
        "http_ok": n_http_ok,
        "inserted": n_insert,
        "errors": n_errors,
        "paginate": "on" if GITHUB_PAGINATE else "off",
        "max_pages": GITHUB_MAX_PAGES if GITHUB_PAGINATE else "-",
    })
    success("[github_events] done")

# --------------------------- CLI 入口 ---------------------------
if __name__ == "__main__":
    run_cli(main)