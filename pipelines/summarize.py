# pipelines/summarize.py
# ======================================================================
#  簇摘要生成（LLM）
#  - 统一日志：阶段耗时、spinner、进度条、汇总面板
#  - 稳健：所有 JSON 字段用 .get() 并带默认值，杜绝 KeyError（修复 "digest_zh" 报错）
#  - 失败降级：单簇失败仅计数，不中断流程；可选择跳过已有摘要
#  - 窗口：rolling / week_to_date / last_week
#  - Provider：OLLAMA / OPENAI
# ======================================================================

from __future__ import annotations
import os, json
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Tuple
import requests

from pipelines.util import ensure_tables, pg_conn, run_cli
from pipelines.logging_utils import (
    info, debug, warn, error, success,
    kv_line, kv_table, status, new_progress, step
)

# --------------------------- 配置（可被环境变量覆盖） ---------------------------
TIME_WINDOW_DAYS = int(os.getenv("TIME_WINDOW_DAYS", "7"))
WINDOW_MODE      = os.getenv("WINDOW_MODE", "rolling").lower()
WEEK_START       = int(os.getenv("WEEK_START", "1"))  # 1=Mon, 0=Sun

LLM_PROVIDER     = os.getenv("LLM_PROVIDER", "ollama").lower()
# OLLAMA
OLLAMA_HOST      = os.getenv("OLLAMA_HOST", "http://localhost:11434").rstrip("/")
OLLAMA_MODEL     = os.getenv("OLLAMA_MODEL", "qwen2.5:3b-instruct")
# OPENAI
OPENAI_API_KEY   = os.getenv("OPENAI_API_KEY", "")
OPENAI_BASE      = os.getenv("OPENAI_BASE", "https://api.openai.com/v1").rstrip("/")
OPENAI_MODEL     = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

# 行为开关
SKIP_IF_EXISTS   = os.getenv("SUM_SKIP_IF_EXISTS", "1") == "1"   # 若已有摘要则跳过
BATCH_LIMIT      = int(os.getenv("SUM_BATCH_LIMIT", "1000"))     # 窗口内最多处理多少簇
MAX_ART_PER_CLU  = int(os.getenv("SUM_MAX_ART_PER_CLUSTER", "12")) # 每簇最多取多少篇生成材料
REQ_TIMEOUT      = int(os.getenv("SUM_REQ_TIMEOUT", "120"))


# --------------------------- 时间窗口 ---------------------------
def _window() -> Tuple[datetime, datetime]:
    now = datetime.now()
    if WINDOW_MODE == "week_to_date":
        py_week_start = (WEEK_START - 1) % 7  # Monday=0..Sunday=6
        delta = (now.weekday() - py_week_start) % 7
        start = (now - timedelta(days=delta)).replace(hour=0, minute=0, second=0, microsecond=0)
        return start, now
    if WINDOW_MODE == "last_week":
        py_week_start = (WEEK_START - 1) % 7
        delta = (now.weekday() - py_week_start) % 7
        this_week_start = (now - timedelta(days=delta)).replace(hour=0, minute=0, second=0, microsecond=0)
        start = this_week_start - timedelta(days=7)
        end = this_week_start - timedelta(microseconds=1)
        return start, end
    # rolling
    return now - timedelta(days=TIME_WINDOW_DAYS), now


# --------------------------- LLM 封装 ---------------------------
def _post_json(url, payload, headers=None, timeout=REQ_TIMEOUT):
    r = requests.post(url, json=payload, headers=headers or {}, timeout=timeout)
    r.raise_for_status()
    return r

def llm_chat(prompt_zh: str, prompt_en: str = "") -> Dict[str, str]:
    """
    返回 {"one_liner_zh": ..., "digest_zh": ..., "one_liner": ..., "digest": ...}
    - 允许任一方向为空串；调用处负责默认值。
    """
    sys_zh = (
        "你是编译器周报的编辑。请：\n"
        "1) 用中文给出一句话要点（<=40字，客观陈述，不用感叹号）\n"
        "2) 用中文给出 3-5 行要点摘要（以 - 开头的列表，每行不超过28字）\n"
        "仅输出 JSON：{\"one_liner_zh\":\"...\",\"digest_zh\":[\"...\",\"...\",...]}\n"
    )
    sys_en = (
        "You are an editor. Please:\n"
        "1) Provide a one-sentence key point (<=20 words)\n"
        "2) Provide 3-5 bullet points (each <=18 words)\n"
        "Output JSON only: {\"one_liner\":\"...\",\"digest\":[\"...\",\"...\",...]}\n"
    )

    out: Dict[str, str] = {"one_liner_zh":"", "one_liner":"", "digest_zh":"", "digest":""}

    try:
        if LLM_PROVIDER == "ollama":
            with status("[summ] 调用 OLLAMA (ZH)…", spinner="dots"):
                r1 = _post_json(f"{OLLAMA_HOST}/api/chat", {
                    "model": OLLAMA_MODEL,
                    "messages": [{"role":"system","content":sys_zh},{"role":"user","content":prompt_zh}],
                    "stream": False, "options": {"temperature": 0.2}
                })
            zh = r1.json().get("message", {}).get("content", "") or r1.json().get("response","")
            try:
                js = json.loads(zh)
                out["one_liner_zh"] = js.get("one_liner_zh","").strip()
                digest_zh = js.get("digest_zh") or []
                if isinstance(digest_zh, list):
                    out["digest_zh"] = "\n".join(f"- {x.strip()}" for x in digest_zh if str(x).strip())
                else:
                    out["digest_zh"] = str(digest_zh).strip()
            except Exception:
                out["one_liner_zh"] = zh.strip()[:80]
                out["digest_zh"] = ""

            with status("[summ] 调用 OLLAMA (EN)…", spinner="dots"):
                r2 = _post_json(f"{OLLAMA_HOST}/api/chat", {
                    "model": OLLAMA_MODEL,
                    "messages": [{"role":"system","content":sys_en},{"role":"user","content":prompt_en or prompt_zh}],
                    "stream": False, "options": {"temperature": 0.2}
                })
            en = r2.json().get("message", {}).get("content", "") or r2.json().get("response","")
            try:
                js = json.loads(en)
                out["one_liner"] = js.get("one_liner","").strip()
                digest = js.get("digest") or []
                if isinstance(digest, list):
                    out["digest"] = "\n".join(f"- {x.strip()}" for x in digest if str(x).strip())
                else:
                    out["digest"] = str(digest).strip()
            except Exception:
                out["one_liner"] = en.strip().split("\n")[0][:80]
                out["digest"] = ""
            return out

        # OPENAI
        headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
        def _openai(messages):
            r = _post_json(f"{OPENAI_BASE}/chat/completions",
                           {"model": OPENAI_MODEL, "messages": messages, "temperature": 0.2},
                           headers=headers)
            return r.json()["choices"][0]["message"]["content"].strip()

        with status("[summ] 调用 OPENAI (ZH)…", spinner="dots"):
            zh = _openai([{"role":"system","content":sys_zh},{"role":"user","content":prompt_zh}])
        try:
            js = json.loads(zh)
            out["one_liner_zh"] = js.get("one_liner_zh","").strip()
            digest_zh = js.get("digest_zh") or []
            out["digest_zh"] = "\n".join(f"- {x.strip()}" for x in digest_zh if str(x).strip()) \
                               if isinstance(digest_zh, list) else str(digest_zh).strip()
        except Exception:
            out["one_liner_zh"] = zh.strip()[:80]
            out["digest_zh"] = ""

        with status("[summ] 调用 OPENAI (EN)…", spinner="dots"):
            en = _openai([{"role":"system","content":sys_en},{"role":"user","content":prompt_en or prompt_zh}])
        try:
            js = json.loads(en)
            out["one_liner"] = js.get("one_liner","").strip()
            digest = js.get("digest") or []
            out["digest"] = "\n".join(f"- {x.strip()}" for x in digest if str(x).strip()) \
                            if isinstance(digest, list) else str(digest).strip()
        except Exception:
            out["one_liner"] = en.strip().split("\n")[0][:80]
            out["digest"] = ""
        return out

    except Exception as ex:
        warn(f"[summ] LLM 调用失败：{ex}")
        return out  # 返回空/部分字段，调用处兜底


# --------------------------- 数据准备 ---------------------------
def _fetch_clusters_in_window(conn, start: datetime, end: datetime) -> List[Tuple[str, str, Dict[str,Any]]]:
    """
    返回 [(cluster_id, title, summary_json), ...]
    只取窗口内有文章的簇。
    """
    cur = conn.cursor()
    cur.execute("""
      SELECT DISTINCT ON (c.cluster_id) c.cluster_id, c.title, c.summary
      FROM clusters c
      JOIN articles_clean a ON a.id = c.id
      WHERE a.ts >= %s AND a.ts < %s
      ORDER BY c.cluster_id, c.created_at DESC
      LIMIT %s;
    """, (start, end, BATCH_LIMIT))
    rows = cur.fetchall()
    cur.close()

    out = []
    for cid, title, summary in rows:
        js = summary if isinstance(summary, dict) else json.loads(summary or "{}")
        out.append((cid, title or "", js))
    return out


def _fetch_cluster_materials(conn, cid: str) -> List[Dict[str,Any]]:
    """
    取该簇关联的若干篇文章，按时间倒序，生成材料：
      - title / url / text / metadata(tags/projects/arches/priority)
    """
    cur = conn.cursor()
    cur.execute("""
      SELECT a.title, a.url, a.text, a.metadata, a.ts
      FROM clusters c
      JOIN articles_clean a ON a.id = c.id
      WHERE c.cluster_id=%s
      ORDER BY a.ts DESC
      LIMIT %s;
    """, (cid, MAX_ART_PER_CLU))
    rows = cur.fetchall()
    cur.close()

    mats = []
    for title, url, text, md, ts in rows:
        meta = md if isinstance(md, dict) else (json.loads(md or "{}"))
        mats.append({
            "title": title or "",
            "url": url or "",
            "text": (text or "").strip(),
            "projects": meta.get("projects") or [],
            "topics": meta.get("topics") or [],
            "arches": meta.get("arches") or [],
            "priority": (meta.get("priority") or "low"),
            "ts": ts
        })
    return mats


# --------------------------- 合并 & 写库 ---------------------------
def _safe_merge(old: Dict[str,Any], new: Dict[str,Any]) -> Dict[str,Any]:
    """
    安全合并 summary：所有字段用 get + 默认值，不抛 KeyError。
    优先保留 new 中的非空字段；old 作为回退。
    """
    old = old or {}
    merged = dict(old)  # 先拷贝旧的，逐项覆盖

    # 标准字段（皆可缺省）
    fields = [
        "title","title_zh",
        "one_liner","one_liner_zh",
        "digest","digest_zh",
        "context","context_zh",
        "key_points","key_points_zh",
        "links","tags","projects","topics","arches",
        "priority","importance","lang"
    ]
    for k in fields:
        nv = new.get(k, None) if new else None
        ov = old.get(k, None)
        if nv in (None, "", [], {}):
            merged[k] = ov
        else:
            merged[k] = nv

    # 保底：基本标题
    if not merged.get("title"):
        merged["title"] = old.get("title") or "(no title)"
    if not merged.get("lang"):
        merged["lang"] = old.get("lang") or "en"

    return merged


def _update_cluster_summary(conn, cid: str, summary_obj: Dict[str,Any]) -> int:
    """
    将同一 cluster_id 的所有行的 summary 更新为相同内容，保证一致性。
    返回受影响行数。
    """
    cur = conn.cursor()
    cur.execute("""
      UPDATE clusters SET summary=%s WHERE cluster_id=%s;
    """, (json.dumps(summary_obj, ensure_ascii=False), cid))
    n = cur.rowcount or 0
    conn.commit()
    cur.close()
    return n


# --------------------------- 主流程 ---------------------------
@step("Summarize Clusters")
def main():
    ensure_tables()
    start, end = _window()
    kv_line("[summ] 参数",
            window_mode=WINDOW_MODE, time_window_days=TIME_WINDOW_DAYS, week_start=WEEK_START,
            llm=LLM_PROVIDER, model=(OLLAMA_MODEL if LLM_PROVIDER=="ollama" else OPENAI_MODEL))

    kv_line("[summ] 时间窗口",
            start=start.strftime("%Y-%m-%d %H:%M"), end=end.strftime("%Y-%m-%d %H:%M"))

    conn = pg_conn()

    with status("[summ] 读取窗口内簇列表 …", spinner="dots"):
        clusters = _fetch_clusters_in_window(conn, start, end)
    info(f"[summ] 命中簇：{len(clusters)}")

    processed = 0
    failures  = 0
    skipped   = 0

    # 进度条：每簇一个刻度
    with new_progress() as progress:
        task = progress.add_task("生成摘要", total=len(clusters))

        for cid, title, old_sum in clusters:
            try:
                # 跳过已有摘要（可通过环境变量关闭跳过）
                if SKIP_IF_EXISTS and (old_sum.get("one_liner_zh") or old_sum.get("digest_zh")
                                       or old_sum.get("one_liner") or old_sum.get("digest")):
                    skipped += 1
                    progress.advance(task, 1)
                    continue

                # 取材料
                mats = _fetch_cluster_materials(conn, cid)
                if not mats:
                    skipped += 1
                    progress.advance(task, 1)
                    continue

                # 构造 prompt（中英文双份；英文可选）
                bullets = []
                for m in mats:
                    tline = (m["text"][:220] + "…") if len(m["text"]) > 220 else m["text"]
                    bullets.append(f"- {m['title']}\n  {tline}\n  link: {m['url']}")
                prompt_zh = (
                    "以下是某个技术主题的若干条资讯/提交。请抽取核心结论与变化：\n"
                    + "\n".join(bullets)
                )
                prompt_en = (
                    "Several items on a technical topic. Extract the essence and summarize:\n"
                    + "\n".join(bullets)
                )

                # 调 LLM
                out = llm_chat(prompt_zh, prompt_en)

                # 组装新的 summary 片段（字段都可缺省）
                new_part = {
                    "title": old_sum.get("title") or title or "(no title)",
                    "title_zh": old_sum.get("title_zh") or "",
                    "one_liner": out.get("one_liner",""),
                    "one_liner_zh": out.get("one_liner_zh",""),
                    "digest": out.get("digest",""),
                    "digest_zh": out.get("digest_zh",""),
                    "links": old_sum.get("links") or [],
                    "tags": old_sum.get("tags") or [],
                    "projects": old_sum.get("projects") or [],
                    "topics": old_sum.get("topics") or [],
                    "arches": old_sum.get("arches") or [],
                    "priority": old_sum.get("priority") or "low",
                    "importance": old_sum.get("importance") or 50,
                    "lang": old_sum.get("lang") or "en",
                    "context": old_sum.get("context") or "",
                    "context_zh": old_sum.get("context_zh") or "",
                    "key_points": old_sum.get("key_points") or [],
                    "key_points_zh": old_sum.get("key_points_zh") or [],
                }

                merged = _safe_merge(old_sum, new_part)
                n = _update_cluster_summary(conn, cid, merged)
                processed += (1 if n > 0 else 0)
            except Exception as ex:
                failures += 1
                error(f"[summ] 处理簇失败：{cid} -> {ex}")
            finally:
                progress.advance(task, 1)

    kv_table("[summ] 汇总", {
        "clusters_in_window": len(clusters),
        "processed": processed,
        "skipped_exists": skipped,
        "failures": failures,
        "window_days": TIME_WINDOW_DAYS,
        "window_mode": WINDOW_MODE,
    })
    success(f"[summ] done (window: {start.date()} ~ {end.date()}, days={TIME_WINDOW_DAYS})")


# --------------------------- CLI 入口 ---------------------------
if __name__ == "__main__":
    run_cli(main)