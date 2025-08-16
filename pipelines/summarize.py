# -*- coding: utf-8 -*-
"""
Summarize clusters into structured JSON with tag/priority merge.

Usage:
  python pipelines/summarize.py

Env (.env):
  LLM_PROVIDER=ollama|openai
  # Ollama
  OLLAMA_HOST=http://localhost:11434
  OLLAMA_MODEL=llama3.1
  # OpenAI (or compatible)
  OPENAI_API_KEY=sk-...
  OPENAI_BASE=https://api.openai.com/v1
  OPENAI_MODEL=gpt-4o-mini
  OPENAI_FORCE_JSON=0|1   # optional: try response_format=json_object
"""

import os
import re
import json
import time
import requests
from typing import Dict, List, Tuple, Any, Set

from dotenv import load_dotenv
from pipelines.util import ensure_tables, pg_conn

load_dotenv()

# --------------------
# LLM config
# --------------------
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "ollama").lower()

# Ollama
OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.1")

# OpenAI-compatible
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_BASE = os.getenv("OPENAI_BASE", "https://api.openai.com/v1")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
OPENAI_FORCE_JSON = os.getenv("OPENAI_FORCE_JSON", "0") == "1"

# --------------------
# Prompt & schema
# --------------------
SCHEMA: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "title": {"type": "string"},
        "context": {"type": "string"},
        "key_points": {"type": "array", "items": {"type": "string"}},
        "impact": {"type": "string"},
        "links": {"type": "array", "items": {"type": "string"}},
        "tags": {"type": "array", "items": {"type": "string"}},
        "priority": {"type": "string"},  # high | medium | low
    },
    "required": ["title", "key_points", "links"],
}

PROMPT_TMPL = """
你是资深编译器领域情报分析师。
请针对同一事件簇的多条材料生成**结构化 JSON**（严格符合此 Schema）：{schema}

- 重点包含：项目/组件、目标架构、优化/IR/Pass 名称、版本/Release、回归/性能影响、相关 PR/Commit/讨论链接。
- 如果材料信息不足，也必须给出尽量有用的 title 与 key_points。
- 合理给出 priority：high（安全/回归/ABI/重大）、medium（版本/后端/IR/性能优化）、low（杂项/周报等）。

材料（多来源，可能存在重复/噪音，需去重与整合）：
{materials}

只输出 JSON，不要任何解释。
"""

# 适配不同模型的上下文限制：材料字符预算
MAX_MATERIAL_CHARS = 9000  # 总预算
PER_ITEM_CHARS = 1000      # 每条材料截断


# --------------------
# Helpers
# --------------------
def _strip_code_fences(s: str) -> str:
    """
    Remove leading ```json ... ``` fences if present.
    """
    s = s.strip()
    if s.startswith("```"):
        # remove first fence
        s = re.sub(r"^```(?:json)?\s*", "", s)
        # remove trailing fence
        s = re.sub(r"\s*```$", "", s)
    return s.strip()


def _extract_json_block(s: str) -> str:
    """
    From arbitrary text, try to extract the outermost { ... } JSON object.
    """
    s = _strip_code_fences(s)
    start = s.find("{")
    end = s.rfind("}")
    if start != -1 and end != -1 and end > start:
        return s[start : end + 1]
    return s  # fallback: maybe it's already clean JSON or plain text


def _safe_json_loads(s: str) -> Dict[str, Any]:
    """
    Try hard to parse JSON. Fallback to minimal structure.
    """
    try:
        return json.loads(_extract_json_block(s))
    except Exception:
        return {
            "title": "(parse failed)",
            "key_points": [s[:200]],
            "links": [],
            "tags": [],
            "context": "",
            "impact": "",
            "priority": "low",
        }


def _dedupe_keep_order(seq: List[Any]) -> List[Any]:
    seen: Set[Any] = set()
    out: List[Any] = []
    for x in seq:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def _prior_norm(p: str) -> str:
    p = (p or "").strip().lower()
    if p.startswith("h"):
        return "high"
    if p.startswith("m"):
        return "medium"
    if p.startswith("l"):
        return "low"
    return "low"


def _build_materials(conn, id_list: List[str]) -> Tuple[str, List[str], Dict[str, Set[str]]]:
    """
    Collect materials text, links and tag union from articles in a cluster.
    Returns:
      materials_str, links, tag_union
    """
    cur = conn.cursor()
    cur.execute(
        "SELECT title, url, text, metadata FROM articles_clean WHERE id = ANY(%s)",
        (id_list,),
    )
    rows = cur.fetchall()
    cur.close()

    links: List[str] = []
    tag_union = {"projects": set(), "topics": set(), "arches": set(), "priority": set()}

    mats = []
    for title, url, text, metadata in rows:
        txt = (text or "")[:PER_ITEM_CHARS]
        title = title or "(no title)"
        mats.append(f"- {title}\n  {url}\n  {txt}...\n")
        if url:
            links.append(url)

        # merge tags from metadata
        try:
            md = metadata if isinstance(metadata, dict) else json.loads(metadata or "{}")
            for k in ["projects", "topics", "arches", "priority"]:
                v = md.get(k, [])
                if isinstance(v, str):
                    v = [v]
                for x in v or []:
                    tag_union.setdefault(k, set()).add(str(x))
        except Exception:
            pass

    # budget: clip total materials
    materials_str = "".join(mats)
    if len(materials_str) > MAX_MATERIAL_CHARS:
        materials_str = materials_str[:MAX_MATERIAL_CHARS]

    return materials_str, _dedupe_keep_order(links), tag_union


def _finalize_summary(js: Dict[str, Any], links: List[str], tag_union: Dict[str, Set[str]]) -> Dict[str, Any]:
    """
    Normalize fields, dedupe links, merge tags + priority.
    """
    js = js or {}
    js.setdefault("title", "(no title)")
    js.setdefault("context", "")
    js.setdefault("key_points", [])
    js.setdefault("impact", "")
    js.setdefault("links", [])
    js.setdefault("tags", [])
    js.setdefault("priority", "low")

    # merge & dedupe links
    merged_links = _dedupe_keep_order([*(js.get("links") or []), *links])
    js["links"] = [str(u) for u in merged_links][:20]

    # merge tags (LLM tags + projects/topics/arches)
    llm_tags = set(map(str, js.get("tags") or []))
    merged_tags = set(llm_tags)
    for k in ["projects", "topics", "arches"]:
        merged_tags |= set(map(str, tag_union.get(k, set())))
    js["tags"] = sorted(merged_tags)

    # priority: prefer metadata union if present, else LLM
    meta_prior = list(tag_union.get("priority", set()))
    pri = meta_prior[0] if meta_prior else js.get("priority", "low")
    js["priority"] = _prior_norm(pri)

    return js


# --------------------
# LLM callers
# --------------------
def _ollama_complete(prompt: str) -> str:
    payload = {
        "model": OLLAMA_MODEL,
        "prompt": prompt,
        "stream": False,
        "options": {
            # 更稳健的生成
            "temperature": 0.2,
            "top_p": 0.9,
            "num_ctx": 8192,
            # "num_predict": 1024,  # 需要可打开
        },
    }
    r = requests.post(f"{OLLAMA_HOST}/api/generate", json=payload, timeout=180)
    r.raise_for_status()
    return r.json().get("response", "").strip()


def _openai_complete(prompt: str) -> str:
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    data: Dict[str, Any] = {
        "model": OPENAI_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.2,
    }
    if OPENAI_FORCE_JSON:
        # 新接口可用时更稳，旧服务可能不支持
        data["response_format"] = {"type": "json_object"}

    r = requests.post(f"{OPENAI_BASE}/chat/completions", headers=headers, json=data, timeout=180)
    r.raise_for_status()
    return r.json()["choices"][0]["message"]["content"]


def llm_summarize(prompt: str) -> Dict[str, Any]:
    if LLM_PROVIDER == "openai":
        raw = _openai_complete(prompt)
    else:
        raw = _ollama_complete(prompt)
    return _safe_json_loads(raw)


# --------------------
# Main pipeline
# --------------------
def main():
    ensure_tables()
    conn = pg_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT cluster_id, array_agg(id) AS ids
        FROM clusters
        GROUP BY cluster_id
        ORDER BY MIN(created_at) DESC
        LIMIT 20;
        """
    )
    groups = cur.fetchall()
    cur.close()

    upd = conn.cursor()
    for cid, id_list in groups:
        # 1) build materials + tag_union
        materials, links, tag_union = _build_materials(conn, id_list)

        # 2) call LLM
        prompt = PROMPT_TMPL.format(schema=json.dumps(SCHEMA, ensure_ascii=False), materials=materials)
        js = llm_summarize(prompt)

        # 3) finalize (merge links/tags/priority)
        js = _finalize_summary(js, links, tag_union)

        # 4) write back to all members in cluster
        payload = json.dumps(js, ensure_ascii=False)
        for _id in id_list:
            upd.execute("UPDATE clusters SET summary=%s WHERE id=%s", (payload, _id))
        conn.commit()

    upd.close()
    conn.close()
    print("[summarize] done")


if __name__ == "__main__":
    main()