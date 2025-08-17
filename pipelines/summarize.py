# pipelines/summarize.py
import os, json, time, requests
from datetime import datetime, timedelta
from dotenv import load_dotenv
from pipelines.util import ensure_tables, pg_conn

load_dotenv()

LLM_PROVIDER = os.getenv("LLM_PROVIDER", "ollama")
OLLAMA_HOST  = os.getenv("OLLAMA_HOST", "http://localhost:11434").rstrip("/")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.1")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_BASE    = os.getenv("OPENAI_BASE", "https://api.openai.com/v1").rstrip("/")
OPENAI_MODEL   = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

TIME_WINDOW_DAYS = int(os.getenv("TIME_WINDOW_DAYS", "7"))

FIELDS_HINT = (
    "只输出一个 JSON 对象，包含："
    "title(string), context(string), key_points(array of string), impact(string), "
    "links(array of string), tags(array of string), priority(one of high/medium/low)。"
    "不要输出解释、不要输出代码块、不要输出 schema。"
)

PROMPT_TMPL = """
你是资深编译器领域情报分析师。以下是同一事件簇的材料（已截断）。基于材料做结构化总结。
{fields_hint}

材料：
{materials}
"""

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

def _iter_json_candidates(s: str):
    # 从文本中提取所有 {...} 片段尝试解析
    depth, start = 0, -1
    for i, ch in enumerate(s):
        if ch == "{":
            if depth == 0: start = i
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0 and start >= 0:
                chunk = s[start:i+1]
                try:
                    js = json.loads(chunk)
                    yield js
                except Exception:
                    pass

def _best_json(s: str):
    for js in _iter_json_candidates(s):
        if isinstance(js, dict) and "title" in js and "key_points" in js:
            return js
    return None

def _fallback_summary(title, links, text, tags):
    text = (text or "").replace("\n", " ")
    ctx = text[:220]
    pts = []
    for seg in text.split("."):
        seg = seg.strip()
        if len(seg) > 20:
            pts.append(seg)
        if len(pts) >= 4: break
    pri = "high" if any(x.lower() in ("regression","cve","abi","弃用") for x in tags) else "medium"
    return {
        "title": title or "(no title)",
        "context": ctx,
        "key_points": pts or ([ctx] if ctx else []),
        "impact": "",
        "links": list(dict.fromkeys(links or [])),
        "tags": list(dict.fromkeys(tags or [])),
        "priority": pri
    }

def _window():
    end = datetime.now()
    start = end - timedelta(days=TIME_WINDOW_DAYS)
    return start, end

def main():
    ensure_tables()
    conn = pg_conn()
    start, end = _window()

    cur = conn.cursor()
    # ✅ 修复：不用 DISTINCT；把 MIN(created_at) 选出来起别名 first_created 再排序
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
        print("[summarize] no clusters in window, exit.")
        cur.close(); conn.close()
        return

    cur2 = conn.cursor()
    for cid, id_list, _first_created in groups:
        in_cur = conn.cursor()
        in_cur.execute("SELECT title, url, text, metadata FROM articles_clean WHERE id = ANY(%s)", (id_list,))
        rows = in_cur.fetchall()
        in_cur.close()

        # 组装材料 & 汇总原始链接/标签
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
                        tags_union.add(t)
                if md.get("priority"):
                    tags_union.add(md["priority"])
            except Exception:
                pass

        prompt = PROMPT_TMPL.format(fields_hint=FIELDS_HINT, materials=materials[:8000])
        raw = llm_generate(prompt)
        js = _best_json(raw)

        if not js:
            # 兜底（避免出现 "(parse failed)"）
            title0 = rows[0][0] if rows else "(no title)"
            text0  = rows[0][2] if rows else ""
            js = _fallback_summary(title0, links, text0, list(tags_union))
        else:
            # 字段与去重合并
            js.setdefault("title", rows[0][0] if rows else "(no title)")
            js.setdefault("context", "")
            js.setdefault("key_points", [])
            js.setdefault("impact", "")
            js.setdefault("links", [])
            js.setdefault("tags", [])
            js.setdefault("priority", "low")
            js["links"] = list(dict.fromkeys(list(js.get("links") or []) + links))
            js["tags"]  = list(dict.fromkeys(list(js.get("tags") or []) + list(tags_union)))

        for _id in id_list:
            cur2.execute(
                "UPDATE clusters SET summary=%s WHERE id=%s",
                (json.dumps(js, ensure_ascii=False), _id)
            )
        conn.commit()

    cur2.close()
    cur.close()
    conn.close()
    print(f"[summarize] done (window: {start.date()} ~ {end.date()}, days={TIME_WINDOW_DAYS})")

if __name__ == "__main__":
    main()