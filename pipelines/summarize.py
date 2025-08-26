# -*- coding: utf-8 -*-
"""
Summarize clusters within a time window.
Outputs bilingual fields + digest + one_liner + importance reasoning.

Env:
- TIME_WINDOW_DAYS=7
- LLM_PROVIDER=ollama|openai  (default: ollama)
- OLLAMA_HOST=http://localhost:11434
- OLLAMA_MODEL=llama3.1
- OPENAI_API_KEY=...
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
OLLAMA_HOST  = os.getenv("OLLAMA_HOST", "http://localhost:11434").rstrip("/")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.1")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_BASE    = os.getenv("OPENAI_BASE", "https://api.openai.com/v1").rstrip("/")
OPENAI_MODEL   = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

FIELDS_HINT = (
    "Return ONE JSON object with keys: "
    "title(string), context(string), key_points(array of string), impact(string), "
    "links(array of string), tags(array of string), priority(one of high/medium/low), "
    "importance(integer 0-100), one_liner(string), importance_reason(string). "
    "High priority indicators: security/CVE, perf regression, ABI/behavior breaking, major release/deprecation, cross-platform breakage. "
    "Low priority: NFC/format/typo/refactor without behavior change."
)

PROMPT_TMPL = """
You are a senior compiler-intel analyst. The following materials describe one cluster (truncated).
Please produce a compact structured summary. {fields_hint}
DO NOT print any explanations or code fences.

Materials:
{materials}
""".strip()

DIGEST_PROMPT_TMPL = """
用专业中文给出 2~4 条“读者友好”的要点解读（不是直译），强调：发生了什么、为何重要、影响范围、是否需要跟进。
只输出 JSON：{{"digest_zh":["...","..."]}}

信息：
标题：{title}
上下文：{context}
要点：
{points}
""".strip()

def _post_json(url, payload, headers=None, timeout=180):
    r = requests.post(url, json=payload, headers=headers or {}, timeout=timeout)
    r.raise_for_status(); return r

def llm_generate(prompt: str) -> str:
    if LLM_PROVIDER == "ollama":
        r = _post_json(f"{OLLAMA_HOST}/api/generate",
                       {"model": OLLAMA_MODEL, "prompt": prompt, "stream": False})
        return (r.json().get("response") or "").strip()
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    data = {"model": OPENAI_MODEL, "messages":[{"role":"user","content":prompt}], "temperature": 0.1}
    r = _post_json(f"{OPENAI_BASE}/chat/completions", data, headers=headers)
    return r.json()["choices"][0]["message"]["content"].strip()

def _iter_json_candidates(s: str):
    depth, start = 0, -1
    for i, ch in enumerate(s or ""):
        if ch == "{":
            if depth == 0: start = i
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0 and start >= 0:
                txt = s[start:i+1]
                try:
                    js = json.loads(txt)
                    if isinstance(js, dict): yield js
                except Exception:
                    pass

def _best_json(s: str):
    for js in _iter_json_candidates(s):
        if "title" in js and "key_points" in js:
            return js
    return None

def _sanitize_links(links):
    out=[]
    for u in (links or []):
        if not u: continue
        u=str(u).strip()
        if not (u.startswith("http://") or u.startswith("https://")): continue
        if u.lower() in ("#error","#"): continue
        out.append(u)
    return list(dict.fromkeys(out))

def _fallback_summary(title, links, text, tags):
    title = title or "(no title)"
    text = (text or "").replace("\n"," ")
    ctx = text[:220]
    pts = []
    for seg in text.split("."):
        seg = (seg or "").strip()
        if len(seg) > 20: pts.append(seg)
        if len(pts) >= 4: break
    pri = "high" if any((x or "").lower() in ("regression","cve","abi","弃用") for x in (tags or [])) else "medium"
    one = (pts[0] if pts else ctx) or title
    reason = "疑似重要变更（启发式）" if pri=="high" else "一般性改动（启发式）"
    return {
        "title": title,
        "context": ctx,
        "key_points": pts or ([ctx] if ctx else []),
        "impact": "",
        "links": _sanitize_links(links),
        "tags": list(dict.fromkeys([str(t) for t in (tags or []) if t])),
        "priority": pri,
        "importance": 90 if pri=="high" else (70 if pri=="medium" else 40),
        "one_liner": one,
        "importance_reason": reason
    }

def _normalize_summary(js: dict, fallback_title: str, links: list, tags_union: list) -> dict:
    js = dict(js or {})
    js["title"]    = js.get("title") or fallback_title or "(no title)"
    js["context"]  = js.get("context") or ""
    js["key_points"] = [str(x) for x in (js.get("key_points") or [])]
    js["impact"]   = js.get("impact") or ""
    js["links"]    = _sanitize_links((js.get("links") or []) + (links or []))
    js["tags"]     = list(dict.fromkeys([str(t) for t in ((js.get("tags") or []) + (tags_union or [])) if t]))
    pri = (js.get("priority") or "low").lower()
    if pri not in ("high","medium","low"): pri = "low"
    js["priority"] = pri
    try:
        imp = int(js.get("importance"))
    except Exception:
        imp = 90 if pri=="high" else (70 if pri=="medium" else 40)
    js["importance"] = max(0, min(100, int(imp)))
    js["one_liner"] = js.get("one_liner") or (js["key_points"][0] if js["key_points"] else js["context"][:120])
    js["importance_reason"] = js.get("importance_reason") or ("高风险/重要变更（启发式）" if pri=="high" else "一般性改动（启发式）")
    return js

def zh_translate(title: str, context: str, points: list, one_liner: str, reason: str) -> dict:
    title = title or ""; context = context or ""; pts = [str(p) for p in (points or [])][:8]
    src = (
        f"标题: {title}\n"
        f"上下文: {context[:800]}\n"
        f"要点:\n" + "\n".join(f"- {p}" for p in pts) + "\n"
        f"一句话: {one_liner}\n"
        f"重要性理由: {reason}\n"
    )
    prompt = ("把以上英文技术内容翻成专业中文（保留专有名词），只输出 JSON："
              '{"title_zh":"...","context_zh":"...","key_points_zh":["..."],'
              '"one_liner_zh":"...","importance_reason_zh":"..."}')
    try:
        out = llm_generate(prompt + "\n\n" + src)
        for js in _iter_json_candidates(out):
            if all(k in js for k in ("title_zh","context_zh","key_points_zh","one_liner_zh","importance_reason_zh")):
                js["key_points_zh"] = [str(p) for p in (js.get("key_points_zh") or pts)]
                for k in ("title_zh","context_zh","one_liner_zh","importance_reason_zh"):
                    js[k] = js.get(k) or (one_liner if k=="one_liner_zh" else reason if k=="importance_reason_zh" else "")
                return js
    except Exception:
        pass
    return {
        "title_zh": title, "context_zh": context,
        "key_points_zh": pts, "one_liner_zh": one_liner or title,
        "importance_reason_zh": reason or ""
    }

def make_digest_zh(title_zh: str, context_zh: str, key_points_zh: list):
    pts = "\n".join(f"- {p}" for p in (key_points_zh or [])[:6])
    prompt = DIGEST_PROMPT_TMPL.format(title=title_zh or "", context=(context_zh or "")[:800], points=pts)
    try:
        out = llm_generate(prompt)
        for js in _iter_json_candidates(out):
            if "digest_zh" in js:
                js["digest_zh"] = [str(x) for x in (js.get("digest_zh") or []) if x]
                if js["digest_zh"]: return js
    except Exception:
        pass
    return {"digest_zh": [p for p in (key_points_zh or [])[:2]] or [ (context_zh or "")[:100] ]}

def _window():
    end = datetime.now(); start = end - timedelta(days=TIME_WINDOW_DAYS); return start, end

def main():
    ensure_tables()
    conn = pg_conn(); start, end = _window()
    cur = conn.cursor()
    cur.execute("""
      SELECT c.cluster_id, array_agg(c.id) AS ids, MIN(c.created_at) AS first_created
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
        cur.close(); conn.close(); print("[summarize] no clusters in window, exit."); return

    cur_write = conn.cursor()
    for cid, id_list, _fc in groups:
        q = conn.cursor()
        q.execute("SELECT title, url, text, metadata FROM articles_clean WHERE id = ANY(%s)", (id_list,))
        rows = q.fetchall(); q.close()

        materials = ""; links=[]; tags_union=set()
        for title, url, text, md in rows:
            materials += f"\n- {title}\n  {url}\n  {(text or '')[:1200]}...\n"
            if url: links.append(url)
            try:
                m = md if isinstance(md, dict) else json.loads(md or "{}")
                for k in ("projects","topics","arches"):
                    for t in (m.get(k) or []):
                        if t: tags_union.add(t)
                if m.get("priority"): tags_union.add(m["priority"])
            except Exception:
                pass

        raw = llm_generate(PROMPT_TMPL.format(fields_hint=FIELDS_HINT, materials=materials[:8000]))
        js = _best_json(raw)
        if not js:
            t0 = rows[0][0] if rows else "(no title)"
            txt= rows[0][2] if rows else ""
            js = _fallback_summary(t0, links, txt, list(tags_union))
        else:
            t0 = rows[0][0] if rows else "(no title)"
            js = _normalize_summary(js, t0, links, list(tags_union))

        # lang + zh fields
        try:
            lang = detect((js.get("title") or "") + " " + (js.get("context") or ""))
        except Exception:
            lang = "en"
        js["lang"] = lang
        if lang != "zh":
            tr = zh_translate(js.get("title",""), js.get("context",""), js.get("key_points",[]),
                              js.get("one_liner",""), js.get("importance_reason",""))
            js.update(tr)
        else:
            js["title_zh"] = js.get("title","")
            js["context_zh"] = js.get("context","")
            js["key_points_zh"] = js.get("key_points",[])
            js["one_liner_zh"] = js.get("one_liner","") or js.get("title","")
            js["importance_reason_zh"] = js.get("importance_reason","")

        # digest
        digest = make_digest_zh(js.get("title_zh",""), js.get("context_zh",""), js.get("key_points_zh",[]))
        js.update(digest)

        payload = json.dumps(js, ensure_ascii=False)
        for _id in id_list:
            cur_write.execute("UPDATE clusters SET summary=%s WHERE id=%s", (payload, _id))
        conn.commit()

    cur_write.close(); cur.close(); conn.close()
    print(f"[summarize] done (window: {start.date()} ~ {end.date()}, days={TIME_WINDOW_DAYS})")

if __name__ == "__main__":
    main()