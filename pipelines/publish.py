# pipelines/publish.py
# =============================================================================
#  每日发布页面生成 & 可选 Slack 通知
#  - 从 clusters 读取摘要，聚合为「项目 -> 簇列表」
#  - 渲染 web/templates/{daily,index}.html.j2 到 web/dist/
#  - 统一风格日志：阶段耗时、spinner、进度条、汇总面板
#  - 行为不变；仅增强可观测性与健壮性
# =============================================================================

from __future__ import annotations
import os, json, requests
from datetime import date, datetime
from collections import defaultdict
from typing import Dict, Any, List, Tuple

from jinja2 import Environment, FileSystemLoader, select_autoescape
from pipelines.util import ensure_tables, pg_conn, run_cli
from pipelines.logging_utils import (
    info, debug, warn, error, success, kv_line, kv_table,
    status, new_progress, step
)

# --------------------------- 环境变量 ---------------------------
SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK", "")      # 为空则不通知
REQ_TIMEOUT   = int(os.getenv("PUBLISH_REQ_TIMEOUT", "15"))

# --------------------------- Jinja 环境 ---------------------------
env = Environment(
    loader=FileSystemLoader("web/templates"),
    autoescape=select_autoescape(["html"]),
)

# --------------------------- 工具函数 ---------------------------
def _priority_rank(p: str | None) -> int:
    """将 priority 文本映射为排序权重（数字越小优先级越高）"""
    return {"high": 0, "medium": 1, "low": 2}.get((p or "low").lower(), 2)

def _safe_json(obj: Any) -> Dict[str, Any]:
    """summary 可能为 JSON 文本或 dict；统一解析为 dict"""
    try:
        return obj if isinstance(obj, dict) else (json.loads(obj or "{}") or {})
    except Exception:
        return {}

def _cluster_meta(conn, cid: str) -> Tuple[datetime | None, List[str], str]:
    """
    统计该簇的最新时间与标签合并
    返回: (latest_ts, projects[], priority)
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT a.ts, a.metadata
        FROM clusters c
        JOIN articles_clean a ON a.id=c.id
        WHERE c.cluster_id=%s
        """,
        (cid,),
    )
    rows = cur.fetchall()
    cur.close()

    latest_ts: datetime | None = None
    tags = {"projects": set(), "topics": set(), "arches": set(), "priority": set()}

    for ts, md in rows:
        if ts and (latest_ts is None or ts > latest_ts):
            latest_ts = ts
        m = _safe_json(md)
        for k in tags.keys():
            v = m.get(k, [])
            if isinstance(v, str):
                v = [v]
            for x in v:
                if x:
                    tags[k].add(x)

    projects = sorted(tags["projects"] or {"Others"})
    priority = (list(tags["priority"]) or ["low"])[0]
    return latest_ts, projects, priority

def _render_pages(grouped: Dict[str, List[Dict[str, Any]]]) -> str:
    """渲染每日与索引页面；返回每日页面路径"""
    today = date.today().isoformat()
    daily = env.get_template("daily.html.j2").render(date=today, groups=grouped)
    index = env.get_template("index.html.j2").render(last_date=today)

    os.makedirs("web/dist", exist_ok=True)
    with open(f"web/dist/{today}.html", "w", encoding="utf-8") as f:
        f.write(daily)
    with open("web/dist/index.html", "w", encoding="utf-8") as f:
        f.write(index)
    return f"web/dist/{today}.html"

def _notify_slack(items: List[Dict[str, Any]], daily_path: str) -> None:
    """推送 Slack 文本（取 Top3）；SLACK_WEBHOOK 为空则直接返回"""
    if not SLACK_WEBHOOK:
        debug("[publish] 未配置 SLACK_WEBHOOK，跳过 Slack 通知")
        return

    top = items[:3]
    lines = [f"*编译器每日情报*（{datetime.now().strftime('%Y-%m-%d')}）"]
    for it in top:
        s = it.get("summary", {}) or {}
        t = s.get("title") or it.get("title") or "(no title)"
        link = (s.get("links") or [""])[0]
        lines.append(f"• {t} {link}")
    lines.append(f"\n日报：{daily_path}")

    try:
        resp = requests.post(SLACK_WEBHOOK, json={"text": "\n".join(lines)}, timeout=REQ_TIMEOUT)
        if resp.ok:
            success("[publish] Slack 通知已发送")
        else:
            warn(f"[publish] Slack 返回非 2xx：{resp.status_code} {resp.text[:200]}")
    except Exception as ex:
        error(f"[publish] Slack 发送失败：{ex}")

# --------------------------- 主流程 ---------------------------
@step("Publish Daily Pages")
def main():
    # 0) 准备
    ensure_tables()
    conn = pg_conn()
    cur = conn.cursor()

    # 1) 读取 clusters
    with status("[publish] 读取 clusters …", spinner="dots"):
        cur.execute(
            """
            SELECT c.cluster_id, c.title, c.summary
            FROM clusters c
            ORDER BY c.created_at DESC
            """
        )
        rows = cur.fetchall()
    cur.close()

    if not rows:
        warn("[publish] clusters 为空，今日无内容可发布")
        conn.close()
        return

    info(f"[publish] 读取到 {len(rows)} 个簇，开始聚合元信息")

    # 2) 聚合：附加 latest_ts / projects / priority
    enriched: List[Dict[str, Any]] = []
    # 用进度条观察聚合进度
    with new_progress() as progress:
        task = progress.add_task("聚合簇元信息", total=len(rows))
        for cid, title, summary in rows:
            js = _safe_json(summary)
            ts, projects, priority = _cluster_meta(conn, cid)
            enriched.append({
                "cluster_id": cid,
                "title": title,
                "summary": js,
                "projects": projects,
                "priority": priority,
                "latest_ts": ts or datetime.min,
            })
            progress.advance(task, 1)

    # 3) 分组：按项目（取列表第一个为“主项目”）
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for it in enriched:
        key = it["projects"][0] if it["projects"] else "Others"
        grouped[key].append(it)

    # 4) 每组排序：优先级 -> 时间新
    for k in grouped:
        # 先按优先级升序（high/medium/low => 0/1/2）
        grouped[k].sort(key=lambda x: _priority_rank(x["priority"]))
        # 同优先级内按时间降序
        grouped[k].sort(key=lambda x: x["latest_ts"], reverse=True)

    # 5) 渲染页面
    with status("[publish] 渲染每日与索引页面 …", spinner="dots"):
        daily_path = _render_pages(grouped)
    success(f"[publish] 页面输出 -> {daily_path}")

    # 6) Top3 汇总并可选通知 Slack
    # 先全局按优先级，再按时间降序
    all_sorted = sorted(
        enriched,
        key=lambda x: (_priority_rank(x["priority"]), -x["latest_ts"].timestamp()),
    )
    _notify_slack(all_sorted, daily_path)

    # 7) 汇总面板
    sizes = {k: len(v) for k, v in grouped.items()}
    kv_table("[publish] 汇总", {
        "groups": len(grouped),
        "total_clusters": len(enriched),
        "top_group": (max(sizes, key=sizes.get) if sizes else "-"),
        "top_group_size": (max(sizes.values()) if sizes else 0),
        "output_daily": daily_path,
    })

    conn.close()
    success("[publish] ok")

# --------------------------- CLI 入口 ---------------------------
if __name__ == "__main__":
    run_cli(main)