# -*- coding: utf-8 -*-
# ======================================================================
#  周报发布（weekly）
#  - 统一风格日志：阶段耗时、spinner、关键参数/指标表、成功/错误提示
#  - 不改表结构与业务逻辑，仅替换零散 print 为统一日志
#  - 环境变量：TIME_WINDOW_DAYS / WINDOW_MODE / WEEK_START / LLM_PROVIDER 等
# ======================================================================

import os, json
from datetime import datetime, timedelta
from collections import defaultdict, Counter
import requests
from dotenv import load_dotenv
from jinja2 import Environment, FileSystemLoader, select_autoescape
from pipelines.util import ensure_tables, pg_conn, run_cli

# 日志工具（基于 rich）
from pipelines.logging_utils import (
    info, debug, warn, error, success,
    kv_line, kv_table, status, step
)

load_dotenv()
TIME_WINDOW_DAYS = int(os.getenv("TIME_WINDOW_DAYS", "7"))

LLM_PROVIDER = os.getenv("LLM_PROVIDER", "ollama").lower()
OLLAMA_HOST  = os.getenv("OLLAMA_HOST", "http://localhost:11434").rstrip("/")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.1")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_BASE    = os.getenv("OPENAI_BASE", "https://api.openai.com/v1").rstrip("/")
OPENAI_MODEL   = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

env = Environment(loader=FileSystemLoader("web/templates"),
                  autoescape=select_autoescape(["html"]))

# --------------------------- HTTP/LLM 调用 ---------------------------

def _post_json(url, payload, headers=None, timeout=120):
    r = requests.post(url, json=payload, headers=headers or {}, timeout=timeout)
    r.raise_for_status()
    return r

def llm_generate(prompt: str) -> str:
    """统一的 LLM 生成：支持 OLLAMA / OPENAI；异常上抛由调用处处理"""
    if LLM_PROVIDER == "ollama":
        r = _post_json(f"{OLLAMA_HOST}/api/generate",
                       {"model": OLLAMA_MODEL, "prompt": prompt, "stream": False})
        return (r.json().get("response") or "").strip()
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    data = {"model": OPENAI_MODEL, "messages":[{"role":"user","content":prompt}], "temperature": 0.2}
    r = _post_json(f"{OPENAI_BASE}/chat/completions", data, headers=headers)
    return r.json()["choices"][0]["message"]["content"].strip()

def weekly_overview(items):
    """基于 Top 项构造材料，调用 LLM 生成 300-500 字中文综述。失败时返回空串。"""
    if not items:
        return ""
    materials = ""
    for it in items[:20]:
        s = it["summary"]
        title = s.get("title_zh") or s.get("title") or ""
        line  = s.get("one_liner_zh") or s.get("one_liner") or (s.get("context_zh") or s.get("context") or "")[:80]
        materials += f"- {title}：{line}\n"
    prompt = ("你是编译器周报撰稿人。基于以下要点写 300-500 字中文综述，按“总体-细分-展望”组织：\n" + materials)
    try:
        with status("[weekly] 生成周报综述（LLM）…", spinner="dots"):
            return llm_generate(prompt).strip()
    except Exception as ex:
        warn(f"[weekly] LLM 生成综述失败：{ex}")
        return ""

# --------------------------- 统计/工具 ---------------------------

def _cluster_stats(conn, cid):
    """聚合簇的时间/来源/标签等统计，返回：(latest_ts, projects, priority, source_counter, ok)"""
    cur = conn.cursor()
    cur.execute("""
      SELECT a.ts, a.url, a.title, a.metadata, a.text
      FROM clusters c JOIN articles_clean a ON a.id=c.id
      WHERE c.cluster_id=%s
    """, (cid,))
    rows = cur.fetchall(); cur.close()

    latest=None; sources=[]; ok=False
    tags={"projects":set(),"topics":set(),"arches":set(),"priority":set()}
    for ts, url, title, md, text in rows:
        if ts and (latest is None or ts>latest): latest=ts
        try:
            m = md if isinstance(md, dict) else json.loads(md or "{}")
        except Exception:
            m={}
        for k in tags.keys():
            for x in (m.get(k) or []): tags[k].add(x)
        src = m.get("source") or ""
        if src: sources.append(src)
        if (url and len(url)>5) and (text and len(text)>60): ok=True
    priority=(list(tags["priority"]) or ["low"])[0]
    return latest, sorted(tags["projects"] or {"Others"}), priority, Counter(sources), ok

def _first_valid_link(js):
    """从 summary.links 里挑第一个 http(s) 链接"""
    for u in (js.get("links") or []):
        if isinstance(u, str) and (u.startswith("http://") or u.startswith("https://")) and u.lower() not in ("#error","#"):
            return u
    return None

def _window():
    """
    计算时间窗口：
      WINDOW_MODE:
        - rolling      : 滚动 TIME_WINDOW_DAYS 天（默认）
        - week_to_date : 本周一 00:00 -> 现在
        - last_week    : 上周一 00:00 -> 上周日 23:59
      WEEK_START: 一周起始（1=周一, 0=周日）。默认 1。
    """
    mode = os.getenv("WINDOW_MODE", "rolling").lower()
    week_start = int(os.getenv("WEEK_START", "1"))  # 1=Mon, 0=Sun
    now = datetime.now()

    if mode == "week_to_date":
        py_week_start = (week_start - 1) % 7  # Python: Monday=0..Sunday=6
        delta = (now.weekday() - py_week_start) % 7
        start = (now - timedelta(days=delta)).replace(hour=0, minute=0, second=0, microsecond=0)
        end = now
        return start, end

    if mode == "last_week":
        py_week_start = (week_start - 1) % 7
        delta = (now.weekday() - py_week_start) % 7
        this_week_start = (now - timedelta(days=delta)).replace(hour=0, minute=0, second=0, microsecond=0)
        start = this_week_start - timedelta(days=7)
        end = this_week_start - timedelta(microseconds=1)
        return start, end

    # 默认：滚动 TIME_WINDOW_DAYS 天
    days = int(os.getenv("TIME_WINDOW_DAYS", "7"))
    start = now - timedelta(days=days)
    end = now
    return start, end

def _md_escape(s):
    return (s or "").replace("\n","\n").strip()

# --------------------------- 导出 ---------------------------

def export_markdown(out_md_path, start, end, top, groups, overview):
    """导出 Markdown（简洁）"""
    lines=[]
    lines.append(f"# 编译器周报 {start} ~ {end}\n")
    if overview:
        lines.append("## 本周综述\n")
        lines.append(overview+"\n")
    lines.append("## 封面 Top\n")
    for it in top:
        s=it["summary"]; link=_first_valid_link(s) or ""
        one=s.get("one_liner_zh") or s.get("one_liner") or ""
        lines.append(f"- **{_md_escape(s.get('title_zh') or s.get('title'))}**  {one}  [{'原文' if link else ''}]({link})")
    for g,items in groups.items():
        lines.append(f"\n## {g}\n")
        for it in items:
            s=it["summary"]; link=_first_valid_link(s) or ""
            one=s.get("one_liner_zh") or s.get("one_liner") or ""
            lines.append(f"- **{_md_escape(s.get('title_zh') or s.get('title'))}**  {one}  [{'原文' if link else ''}]({link})")
    with open(out_md_path,"w",encoding="utf-8") as f:
        f.write("\n".join(lines))

def export_rss(out_xml_path, date_iso, items):
    """导出简易 RSS 2.0（只含 title/description/link/guid）"""
    from xml.sax.saxutils import escape
    def itag(tag, txt): return f"<{tag}>{escape(txt or '')}</{tag}>"
    rss = []
    rss.append('<?xml version="1.0" encoding="UTF-8"?>')
    rss.append('<rss version="2.0"><channel>')
    rss.append(itag("title","编译器周报"))
    rss.append(itag("description","最近一周聚合（LLVM/GCC/Rust/Swift/Wasmtime 等）"))
    rss.append(itag("link",""))
    rss.append(itag("pubDate", date_iso))
    for it in items:
        s=it["summary"]; link = _first_valid_link(s) or ""
        title = (s.get("title_zh") or s.get("title") or "")
        desc  = (s.get("one_liner_zh") or s.get("one_liner") or s.get("context_zh") or s.get("context") or "")
        rss.append("<item>")
        rss.append(itag("title", title))
        rss.append(itag("description", desc))
        rss.append(itag("link", link))
        rss.append(itag("guid", link or f"cluster:{it['cluster_id']}"))
        rss.append("</item>")
    rss.append("</channel></rss>")
    with open(out_xml_path,"w",encoding="utf-8") as f:
        f.write("\n".join(rss))

# --------------------------- 主流程 ---------------------------

@step("Publish Weekly")
def main():
    # 参数概览
    kv_line("[weekly] 参数",
            window_mode=os.getenv("WINDOW_MODE","rolling"),
            time_window_days=TIME_WINDOW_DAYS,
            week_start=os.getenv("WEEK_START","1"),
            llm=LLM_PROVIDER,
            model=(OLLAMA_MODEL if LLM_PROVIDER=="ollama" else OPENAI_MODEL))

    ensure_tables()
    conn = pg_conn()
    cur = conn.cursor()

    # 计算时间窗口
    start, end = _window()
    kv_line("[weekly] 时间窗口", start=start.strftime("%Y-%m-%d %H:%M"), end=end.strftime("%Y-%m-%d %H:%M"))

    # 查询窗口内的簇（按簇 id 去重取最新）
    with status("[weekly] 读取窗口内 clusters …", spinner="dots"):
        cur.execute("""
          SELECT DISTINCT ON (c.cluster_id) c.cluster_id, c.title, c.summary
          FROM clusters c
          JOIN articles_clean a ON a.id = c.id
          WHERE a.ts >= %s AND a.ts < %s
          ORDER BY c.cluster_id, c.created_at DESC
        """, (start, end))
        rows = cur.fetchall()
    cur.close()
    info(f"[weekly] 命中簇：{len(rows)}")

    # 逐簇补充统计信息并过滤
    enriched=[]; source_counter=Counter(); all_arches=set(); all_projects=set()
    with status("[weekly] 聚合簇统计与过滤 …", spinner="dots"):
        for cid, title, summary in rows:
            js = summary if isinstance(summary, dict) else json.loads(summary or "{}")
            latest_ts, projects, priority, src_cnt, ok = _cluster_stats(conn, cid)
            source_counter.update(src_cnt)
            link0 = _first_valid_link(js)
            ctx = (js.get("context_zh") or js.get("context") or "")
            if not ok or not link0 or len(ctx) < 20:
                continue
            importance = int(js.get("importance") or 50)
            for p in projects: all_projects.add(p)
            for a in (js.get("tags") or []):
                if a in ("RISC-V","ARM64","x86_64","WASM","GPU"): all_arches.add(a)
            enriched.append({
                "cluster_id": cid,
                "title": js.get("title") or title or "(no title)",
                "summary": js,
                "projects": projects,
                "priority": priority,
                "importance": importance,
                "latest_ts": latest_ts or datetime.min
            })

    info(f"[weekly] 通过过滤：{len(enriched)}")

    # 分组与排序
    groups=defaultdict(list)
    for it in enriched:
        if "LLVM" in it["projects"]:
            k="LLVM 专区"
        elif "GCC" in it["projects"]:
            k="GCC"
        else:
            k = it["projects"][0] if it["projects"] else "Others"
        groups[k].append(it)
    for g in groups:
        groups[g].sort(key=lambda x: x["latest_ts"], reverse=True)

    top = sorted(enriched, key=lambda x: x["latest_ts"], reverse=True)[:8]

    proj_counter=Counter()
    for it in enriched:
        for p in it["projects"]:
            proj_counter[p]+=1

    # LLM 综述
    overview = weekly_overview(top)

    # 导航顺序
    nav_groups = ["封面 Top"] + [k for k in ["LLVM 专区","GCC"] if k in groups] + [k for k in groups.keys() if k not in ("LLVM 专区","GCC")]

    # 渲染 HTML
    today = datetime.now().strftime("%Y-%m-%d")
    with status("[weekly] 渲染 weekly.html …", spinner="dots"):
        html = env.get_template("weekly.html.j2").render(
            date=today, start=start.strftime("%Y-%m-%d"), end=end.strftime("%Y-%m-%d"),
            groups=groups, nav_groups=nav_groups, top=top,
            sources=source_counter.most_common(),
            proj_counts=sorted(proj_counter.items(), key=lambda x: x[1], reverse=True),
            overview=overview,
            arches=sorted(all_arches), projects=sorted(all_projects),
        )
    os.makedirs("web/dist", exist_ok=True)
    out_html = f"web/dist/weekly-{today}.html"
    with open(out_html,"w",encoding="utf-8") as f:
        f.write(html)
    success(f"[weekly] HTML -> {out_html}")

    # 导出 Markdown & RSS
    out_md  = f"web/dist/weekly-{today}.md"
    out_xml = f"web/dist/weekly-{today}.xml"
    with status("[weekly] 导出 Markdown / RSS …", spinner="dots"):
        export_markdown(out_md, start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"), top, groups, overview)
        export_rss(out_xml, datetime.now().isoformat(), top + [i for g in groups.values() for i in g][:40])
    success(f"[weekly] Markdown -> {out_md}")
    success(f"[weekly] RSS -> {out_xml}")

    # 首页（简单直出）
    index_path = "web/dist/index.html"
    with status("[weekly] 写入首页 index.html …", spinner="dots"):
        with open(index_path,"w",encoding="utf-8") as f:
            f.write(
                '<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">'
                '<body style="font-family:-apple-system,Segoe UI,Roboto,Helvetica Neue,Arial">'
                f'<p><a href="{out_html.split("/")[-1]}">打开最新周报</a></p>'
                f'<p><a href="search.html">检索</a></p>'
                f'<p><a href="{out_md.split("/")[-1]}">导出 Markdown</a> · <a href="{out_xml.split("/")[-1]}">RSS</a></p>'
                '</body>'
            )
    success(f"[weekly] Index -> {index_path}")

    # 汇总面板
    kv_table("[weekly] 汇总", {
        "clusters_in_window": len(rows),
        "kept_after_filter": len(enriched),
        "groups": len(groups),
        "top": len(top),
        "projects": len(all_projects),
        "arches": len(all_arches),
        "window_days": TIME_WINDOW_DAYS,
        "window_mode": os.getenv("WINDOW_MODE","rolling"),
    })

    # 结束
    success(f"[weekly] 完成（window {start.date()} ~ {end.date()}, days={TIME_WINDOW_DAYS})")
    conn.close()

# --------------------------- CLI 入口 ---------------------------
if __name__ == "__main__":
    run_cli(main)