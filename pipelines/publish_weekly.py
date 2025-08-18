# -*- coding: utf-8 -*-
"""
Publish weekly report for last TIME_WINDOW_DAYS (default 7).
Sort by model importance, group by project, and prepare stats for charts.
"""
import os, json
from datetime import datetime, timedelta
from collections import defaultdict, Counter

import requests
from dotenv import load_dotenv
from jinja2 import Environment, FileSystemLoader, select_autoescape

from pipelines.util import ensure_tables, pg_conn

load_dotenv()
TIME_WINDOW_DAYS = int(os.getenv("TIME_WINDOW_DAYS", "7"))

# LLM（用于一周总览）
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "ollama").lower()
OLLAMA_HOST  = os.getenv("OLLAMA_HOST", "http://localhost:11434").rstrip("/")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.1")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_BASE    = os.getenv("OPENAI_BASE", "https://api.openai.com/v1").rstrip("/")
OPENAI_MODEL   = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

env = Environment(loader=FileSystemLoader("web/templates"),
                  autoescape=select_autoescape(["html"]))

def _window():
    end = datetime.now()
    start = end - timedelta(days=TIME_WINDOW_DAYS)
    return start, end

def _post_json(url, payload, headers=None, timeout=120):
    r = requests.post(url, json=payload, headers=headers or {}, timeout=timeout)
    r.raise_for_status()
    return r

def llm_generate(prompt: str) -> str:
    if LLM_PROVIDER == "ollama":
        r = _post_json(f"{OLLAMA_HOST}/api/generate",
                       {"model": OLLAMA_MODEL, "prompt": prompt, "stream": False})
        return (r.json().get("response") or "").strip()
    else:
        headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
        data = {"model": OPENAI_MODEL, "messages":[{"role":"user","content":prompt}], "temperature": 0.2}
        r = _post_json(f"{OPENAI_BASE}/chat/completions", data, headers=headers)
        return r.json()["choices"][0]["message"]["content"].strip()

def weekly_overview(items):
    if not items: return ""
    materials = ""
    for it in items[:20]:
        s = it["summary"]
        title = s.get("title_zh") or s.get("title") or ""
        ctx = (s.get("context_zh") or s.get("context") or "")[:160]
        materials += f"- {title}：{ctx}\n"
    prompt = ("你是编译器周报撰稿人。基于以下要点写 300-500 字中文综述，按“总体-细分-展望”组织：\n" + materials)
    try:
        return llm_generate(prompt).strip()
    except Exception:
        return ""

def _cluster_stats(conn, cid):
    cur = conn.cursor()
    cur.execute("""
      SELECT a.ts, a.url, a.title, a.metadata, a.text
      FROM clusters c JOIN articles_clean a ON a.id=c.id
      WHERE c.cluster_id=%s
    """, (cid,))
    rows = cur.fetchall(); cur.close()
    latest = None; sources = []; ok = False
    tags = {"projects":set(),"topics":set(),"arches":set(),"priority":set()}
    for ts, url, title, md, text in rows:
        if ts and (latest is None or ts>latest): latest = ts
        try:
            m = md if isinstance(md, dict) else json.loads(md or "{}")
        except Exception:
            m = {}
        for k in tags.keys():
            v = m.get(k, []); v = [v] if isinstance(v, str) else (v or [])
            for x in v: tags[k].add(x)
        src = m.get("source") or ""
        if src: sources.append(src)
        if (url and len(url)>5) and (text and len(text)>60): ok = True
    priority = (list(tags["priority"]) or ["low"])[0]
    return latest, sorted(tags["projects"] or {"Others"}), priority, Counter(sources), ok

def _first_valid_link(js):
    for u in (js.get("links") or []):
        if isinstance(u, str) and (u.startswith("http://") or u.startswith("https://")) and u.lower() not in ("#error","#"):
            return u
    return None

def main():
    ensure_tables()
    conn = pg_conn(); cur = conn.cursor()
    start, end = _window()

    # 时间窗内取每个 cluster 最新一条
    cur.execute("""
      SELECT DISTINCT ON (c.cluster_id) c.cluster_id, c.title, c.summary
      FROM clusters c
      JOIN articles_clean a ON a.id = c.id
      WHERE a.ts >= %s AND a.ts < %s
      ORDER BY c.cluster_id, c.created_at DESC
    """, (start, end))
    rows = cur.fetchall(); cur.close()

    enriched = []
    source_counter = Counter()
    all_arches = set()
    all_projects = set()

    for cid, title, summary in rows:
        js = summary if isinstance(summary, dict) else json.loads(summary or "{}")
        latest_ts, projects, priority, src_cnt, ok = _cluster_stats(conn, cid)
        source_counter.update(src_cnt)

        # 过滤空内容/无链接
        link0 = _first_valid_link(js)
        ctx = (js.get("context_zh") or js.get("context") or "")
        if not ok or not link0 or len(ctx) < 20:
            continue

        importance = js.get("importance")
        try:
            importance = int(importance)
        except Exception:
            importance = 50
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

    # 分组：LLVM/GCC 置顶，其他按项目名
    groups = defaultdict(list)
    for it in enriched:
        if "LLVM" in it["projects"]:
            k = "LLVM 专区"
        elif "GCC" in it["projects"]:
            k = "GCC"
        else:
            k = it["projects"][0] if it["projects"] else "Others"
        groups[k].append(it)

    # 组内按 重要性 -> 时间 排序
    for g in groups:
        groups[g].sort(key=lambda x: (-(x["importance"]), x["latest_ts"]), reverse=False)
        groups[g].sort(key=lambda x: x["latest_ts"], reverse=True)

    # 封面 Top：按重要性与时间
    top = sorted(enriched, key=lambda x: (-(x["importance"]), x["latest_ts"]), reverse=False)
    top = sorted(top, key=lambda x: x["latest_ts"], reverse=True)[:8]

    # 统计
    proj_counter = Counter()
    for it in enriched:
        for p in it["projects"]:
            proj_counter[p] += 1

    today = datetime.now().strftime("%Y-%m-%d")
    overview = weekly_overview(top)

    # 导航用的顺序列表
    nav_groups = ["封面 Top"] + [k for k in ["LLVM 专区","GCC"] if k in groups] + [k for k in groups.keys() if k not in ("LLVM 专区","GCC")]

    html = env.get_template("weekly.html.j2").render(
        date=today,
        start=start.strftime("%Y-%m-%d"),
        end=end.strftime("%Y-%m-%d"),
        groups=groups,
        nav_groups=nav_groups,
        top=top,
        sources=source_counter.most_common(),
        proj_counts=sorted(proj_counter.items(), key=lambda x: x[1], reverse=True),
        overview=overview,
        arches=sorted(all_arches),
        projects=sorted(all_projects),
    )
    os.makedirs("web/dist", exist_ok=True)
    out = f"web/dist/weekly-{today}.html"
    with open(out,"w",encoding="utf-8") as f: f.write(html)

    with open("web/dist/index.html","w",encoding="utf-8") as f:
        f.write(f'<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">'
                f'<body style="font-family:-apple-system,Segoe UI,Roboto,Helvetica Neue,Arial">'
                f'<p><a href="{out.split("/")[-1]}">打开最新周报</a></p><p><a href="search.html">检索</a></p></body>')
    print("[weekly] ->", out, f"(window {start.date()} ~ {end.date()}, days={TIME_WINDOW_DAYS})")

    conn.close()

if __name__ == "__main__":
    main()