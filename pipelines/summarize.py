# -*- coding: utf-8 -*-
"""
Summarize clusters within a time window, bilingual output, model-based importance,
and write back to `clusters.summary`.

Env:
- TIME_WINDOW_DAYS=7
- LLM_PROVIDER=ollama|openai  (default: ollama)
- OLLAMA_HOST=http://localhost:11434
- OLLAMA_MODEL=llama3.1
- OPENAI_API_KEY=sk-...
- OPENAI_BASE=https://api.openai.com/v1
- OPENAI_MODEL=gpt-4o-mini
"""
import os, json
from datetime import datetime, timedelta

import requests
from dotenv import load_dotenv
from langdetect import detect

from pipelines.util import ensure_tables, pg_conn

load_dotenv()

TIME_WINDOW_DAYS = int(os.getenv("TIME_WINDOW_DAYS", "7"))

LLM_PROVIDER = os.getenv("LLM_PROVIDER", "ollama").lower()
# Ollama
OLLAMA_HOST  = os.getenv("OLLAMA_HOST", "http://localhost:11434").rstrip("/")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.1")
# OpenAI-compatible
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_BASE    = os.getenv("OPENAI_BASE", "https://api.openai.com/v1").rstrip("/")
OPENAI_MODEL   = os.getenv("OPENAI_MODEL", "gpt-4o-mini")


# ==== Prompt 与字段规范 ====
FIELDS_HINT = (
    "只输出一个 JSON 对象，键包含："
    "title(string), context(string), key_points(array of string), impact(string), "
    "links(array of string), tags(array of string), priority(one of high/medium/low), "
    "importance(integer 0-100)。"
    "高优先级与高分准则：安全/CVE、性能回归、ABI/行为破坏、重大发布/弃用、跨平台破坏；"
    "低优先级：NFC/格式化/typo/微小重构/无行为变化的提交。"
    "不要输出解释、Markdown 代码块或 schema。"
)

PROMPT_TMPL = """
你是资深编译器情报分析师。以下是同一事件簇的材料（已截断）。请据此输出结构化总结。
{fields_hint}

材料：
{materials}
""".strip()

DIGEST_PROMPT_TMPL = """
请基于下面信息，用**专业中文**给出“读者友好”的 2~4 条要点解读（不是直译），
强调：发生了什么、为什么重要、受影响对象（如平台/后端/工具链）、是否需要跟进。
只输出 JSON：{{"digest_zh":["...","..."]}}。

信息：
标题：{title}
上下文：{context}
要点：
{points}
""".strip()


# ==== HTTP/LLM 基础 ====
def _post_json(url, payload, headers=None, timeout=180):
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
        data = {"model": OPENAI_MODEL,
                "messages":[{"role":"user","content":prompt}],
                "temperature": 0.1}
        r = _post_json(f"{OPENAI_BASE}/chat/completions", data, headers=headers)
        return r.json()["choices"][0]["message"]["content"].strip()


# ==== JSON 抽取 ====
def _iter_json_candidates(s: str):
    depth, start = 0, -1
    for i, ch in enumerate(s or ""):
        if ch == "{":
            if depth == 0: start = i
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0 and start >= 0:
                chunk = s[start:i+1]
                try:
                    js = json.loads(chunk)
                    if isinstance(js, dict): yield js
                except Exception:
                    pass

def _best_json(s: str):
    for js in _iter_json_candidates(s):
        if "title" in js and "key_points" in js:
            return js
    return None


# ==== 兜底/清洗 ====
def _sanitize_links(links):
    out = []
    for u in (links or []):
        if not u: continue
        u = str(u).strip()
        if not (u.startswith("http://") or u.startswith("https://")): continue
        if u.lower() in ("#error", "#"): continue
        out.append(u)
    # 去重保序
    return list(dict.fromkeys(out))

def _fallback_summary(title, links, text, tags):
    title = title or "(no title)"
    text = (text or "").replace("\n", " ")
    ctx = text[:220]
    pts = []
    for seg in text.split("."):
        seg = (seg or "").strip()
        if len(seg) > 20:
            pts.append(seg)
        if len(pts) >= 4: break
    pri = "high" if any((x or "").lower() in ("regression","cve","abi","弃用") for x in (tags or [])) else "medium"
    return {
        "title": title,
        "context": ctx,
        "key_points": pts or ([ctx] if ctx else []),
        "impact": "",
        "links": _sanitize_links(links),
        "tags": list(dict.fromkeys([str(t) for t in (tags or []) if t])),
        "priority": pri,
        "importance": 80 if pri=="high" else (60 if pri=="medium" else 30)
    }

def _normalize_summary(js: dict, fallback_title: str, links: list, tags_union: list) -> dict:
    js = dict(js or {})
    js["title"] = js.get("title") or fallback_title or "(no title)"
    js["context"] = js.get("context") or ""
    js["key_points"] = [str(x) for x in (js.get("key_points") or [])]
    js["impact"] = js.get("impact") or ""
    js["links"] = _sanitize_links((js.get("links") or []) + (links or []))
    js["tags"]  = list(dict.fromkeys([str(t) for t in ((js.get("tags") or []) + (tags_union or [])) if t]))
    pri = (js.get("priority") or "low").lower()
    if pri not in ("high","medium","low"): pri = "low"
    js["priority"] = pri
    imp = js.get("importance")
    try:
        imp = int(imp)
    except Exception:
        imp = None
    if imp is None:
        imp = 90 if pri=="high" else (70 if pri=="medium" else 40)
    js["importance"] = max(0, min(100, int(imp)))
    return js


# ==== 翻译/解读 ====
def zh_translate(title: str, context: str, points: list) -> dict:
    title = title or ""
    context = context or ""
    pts = [str(p) for p in (points or [])][:8]
    src = f"标题: {title}\n上下文: {context[:800]}\n要点:\n" + "\n".join(f"- {p}" for p in pts)
    prompt = ("请把以上英文技术内容翻成专业中文，保留专有名词。只输出 JSON："
              '{"title_zh": "...", "context_zh": "...", "key_points_zh": ["...","..."]}')
    try:
        out = llm_generate(prompt + "\n\n" + src)
        for js in _iter_json_candidates(out):
            if all(k in js for k in ("title_zh","context_zh","key_points_zh")):
                js["title_zh"] = js.get("title_zh") or title
                js["context_zh"] = js.get("context_zh") or context
                js["key_points_zh"] = [str(p) for p in (js.get("key_points_zh") or pts)]
                return js
    except Exception:
        pass
    return {"title_zh": title, "context_zh": context, "key_points_zh": pts}

def make_digest_zh(title_zh: str, context_zh: str, key_points_zh: list):
    points = "\n".join(f"- {p}" for p in (key_points_zh or [])[:6])
    prompt = DIGEST_PROMPT_TMPL.format(title=title_zh or "", context=(context_zh or "")[:800], points=points)
    try:
        out = llm_generate(prompt)
        for js in _iter_json_candidates(out):
            if "digest_zh" in js:
                js["digest_zh"] = [str(x) for x in (js.get("digest_zh") or []) if x]
                if js["digest_zh"]:
                    return js
    except Exception:
        pass
    # 兜底：截取 2 条关键句
    pts = [str(x) for x in (key_points_zh or []) if len(str(x))>10][:2]
    if not pts:
        pts = [ (context_zh or "")[:120] ]
    return {"digest_zh": pts}


# ==== 时间窗 ====
def _window():
    end = datetime.now()
    start = end - timedelta(days=TIME_WINDOW_DAYS)
    return start, end


# ==== MAIN ====
def main():
    ensure_tables()
    conn = pg_conn()
    start, end = _window()

    cur = conn.cursor()
    cur.execute("""
      SELECT c.cluster_id,
             array_agg(c.id) AS ids,
             MIN(c.created_at) AS first_created
      FROM clusters c
      JOIN articles_clean a ON a.id = c.id
      WHERE a.ts >= %s AND a.ts < %s
      GROUP BY c.cluster_id
      ORDER BY first_created DESC
      LIMIT 100;
    """, (start, end))
    groups = cur.fetchall()
    print(f"[summarize] groups in window {start.date()} ~ {end.date()}: {len(groups)}")
    if not groups:
        cur.close(); conn.close()
        print("[summarize] no clusters in window, exit.")
        return

    cur_write = conn.cursor()
    for cid, id_list, _first_created in groups:
        q = conn.cursor()
        q.execute("SELECT title, url, text, metadata FROM articles_clean WHERE id = ANY(%s)", (id_list,))
        rows = q.fetchall()
        q.close()

        materials = ""
        links = []
        tags_union = set()
        for title, url, text, metadata in rows:
            materials += f"\n- {title}\n  {url}\n  {(text or '')[:1200]}...\n"
            if url: links.append(url)
            try:
                md = metadata if isinstance(metadata, dict) else json.loads(metadata or "{}")
                for k in ("projects","topics","arches"):
                    for t in (md.get(k) or []):
                        if t: tags_union.add(t)
                if md.get("priority"): tags_union.add(md["priority"])
            except Exception:
                pass

        prompt = PROMPT_TMPL.format(fields_hint=FIELDS_HINT, materials=materials[:8000])
        raw = llm_generate(prompt)
        js = _best_json(raw)

        if not js:
            title0 = rows[0][0] if rows else "(no title)"
            text0  = rows[0][2] if rows else ""
            js = _fallback_summary(title0, links, text0, list(tags_union))
        else:
            title0 = rows[0][0] if rows else "(no title)"
            js = _normalize_summary(js, title0, links, list(tags_union))

        # 语言 & 翻译
        try:
            lang = detect((js.get("title") or "") + " " + (js.get("context") or ""))
        except Exception:
            lang = "en"
        js["lang"] = lang
        if lang != "zh":
            tr = zh_translate(js.get("title",""), js.get("context",""), js.get("key_points",[]))
            js.update(tr)
        else:
            js["title_zh"] = js.get("title","")
            js["context_zh"] = js.get("context","")
            js["key_points_zh"] = js.get("key_points",[])

        # 读者友好“解读”
        digest = make_digest_zh(js.get("title_zh",""), js.get("context_zh",""), js.get("key_points_zh",[]))
        js.update(digest)

        payload = json.dumps(js, ensure_ascii=False)
        for _id in id_list:
            cur_write.execute("UPDATE clusters SET summary=%s WHERE id=%s", (payload, _id))
        conn.commit()

    cur_write.close()
    cur.close()
    conn.close()
    print(f"[summarize] done (window: {start.date()} ~ {end.date()}, days={TIME_WINDOW_DAYS})")


if __name__ == "__main__":
    main()