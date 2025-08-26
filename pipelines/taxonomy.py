# pipelines/taxonomy.py
# ======================================================================
#  文章分类（项目 / 主题 / 架构 / 优先级）
#  - 统一风格日志：debug 级别输出命中线索，TRACE 可额外展开
#  - 兼容原行为：classify() 返回结构不变
#  - 稳健性：taxonomy.yml 缺失或字段为空时安全降级
# ======================================================================

from __future__ import annotations
import os
import re
import yaml
import tldextract
from typing import Dict, List, Tuple

# 统一风格日志
from pipelines.logging_utils import debug, info, warn, error, kv_line

# 是否输出更详细的命中线索（环境变量开关）
TRACE = os.getenv("TAXONOMY_TRACE", "0") == "1"

# --------------------------- 配置加载 ---------------------------

def _load_taxonomy() -> Dict:
    """读取 taxonomy.yml；失败时给出告警并返回空骨架。"""
    try:
        with open("taxonomy.yml", "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
            # 关键字段兜底为空结构，避免 KeyError
            data.setdefault("projects", {})
            data.setdefault("topics", {})
            data.setdefault("arches", {})
            data.setdefault("priority_rules", {"high": [], "medium": []})
            data.setdefault("source_hints", {})
            data.setdefault("host_hints", {})
            data.setdefault("noise_low", [])
            kv_line("[taxonomy] 规则加载",
                    projects=len(data["projects"]),
                    topics=len(data["topics"]),
                    arches=len(data["arches"]))
            return data
    except FileNotFoundError:
        warn("[taxonomy] 找不到 taxonomy.yml，使用空规则（所有分类均可能为 Others/空）")
    except Exception as ex:
        error(f"[taxonomy] 读取 taxonomy.yml 失败：{ex}；使用空规则")
    # 返回安全的空骨架
    return {
        "projects": {},
        "topics": {},
        "arches": {},
        "priority_rules": {"high": [], "medium": []},
        "source_hints": {},
        "host_hints": {},
        "noise_low": [],
    }

TAX = _load_taxonomy()

# --------------------------- 小工具 ---------------------------

def _match_any(pats: List[str], text: str) -> Tuple[bool, str]:
    """
    是否命中任一正则；返回 (matched, pattern)。
    - 为了可观测性，返回命中的第一个 pattern 用于 TRACE。
    """
    if not pats:
        return False, ""
    for p in pats:
        try:
            if re.search(p, text or "", re.I):
                return True, p
        except re.error as ex:
            # 非法正则不抛出，记录一次并忽略
            debug(f"[taxonomy] 无法编译正则：{p} -> {ex}")
    return False, ""

def _host(url: str) -> str:
    """提取 host（domain.suffix），失败返回空串。"""
    try:
        ext = tldextract.extract(url or "")
        return ".".join([p for p in [ext.domain, ext.suffix] if p])
    except Exception as ex:
        debug(f"[taxonomy] 提取 host 失败：{ex}")
        return ""

# --------------------------- 核心分类 ---------------------------

def classify(title: str, text: str, url: str, source: str) -> Dict:
    """
    依据 taxonomy.yml 规则，对文章做轻量分类：
      - projects：来源提示 / host 提示 / 正则规则
      - topics / arches：正则规则
      - priority：high > medium > low，且命中噪声降权 -> low
    日志：
      - DEBUG：打印 host 与最终分类结果
      - TAXONOMY_TRACE=1：额外打印每一类命中的“规则线索”
    """
    # 拼接检索 blob（标题/正文/链接/源名）
    blob = " ".join([title or "", text or "", url or "", source or ""])
    host = _host(url)
    debug(f"[taxonomy] host={host} src={source or ''}")

    # ---- projects: source_hints / host_hints / regex ----
    projects_hits = []

    # 1) 源名提示
    for proj, hints in (TAX.get("source_hints") or {}).items():
        if any((source or "").lower().find((h or "").lower()) >= 0 for h in (hints or [])):
            projects_hits.append(("source_hint", proj))

    # 2) Host 提示
    for proj, hints in (TAX.get("host_hints") or {}).items():
        if any((h or "") and (h in (host or "")) for h in (hints or [])):
            projects_hits.append(("host_hint", proj))

    # 3) 正则（blob）
    for proj, pats in (TAX.get("projects") or {}).items():
        ok, pat = _match_any(pats or [], blob)
        if ok:
            projects_hits.append(("regex", proj if isinstance(proj, str) else str(proj)))

    # 去重、排序、兜底
    projects = sorted({p for _, p in projects_hits}) or ["Others"]

    # ---- topics / arches: 正则 ----
    topics_hits = [(k, _match_any(pats or [], blob)[1]) for k, pats in (TAX.get("topics") or {}).items()
                   if _match_any(pats or [], blob)[0]]
    arches_hits = [(k, _match_any(pats or [], blob)[1]) for k, pats in (TAX.get("arches") or {}).items()
                   if _match_any(pats or [], blob)[0]]

    topics = sorted({k for k, _ in topics_hits})
    arches = sorted({k for k, _ in arches_hits})

    # ---- priority: high > medium > low；噪声降权 ----
    pri = "low"
    high_ok, high_pat = _match_any((TAX.get("priority_rules") or {}).get("high") or [], blob)
    med_ok, med_pat   = _match_any((TAX.get("priority_rules") or {}).get("medium") or [], blob)
    if high_ok:
        pri = "high"
    elif med_ok:
        pri = "medium"

    # 噪声降权：匹配到任意 noise_low 则强制 low
    noise_ok, noise_pat = _match_any(TAX.get("noise_low") or [], blob)
    if noise_ok:
        pri = "low"

    # ---- 结果 ----
    result = {"projects": projects, "topics": topics, "arches": arches, "priority": pri}

    # 基本结果日志（debug）
    kv_line("[taxonomy] 分类结果",
            projects=",".join(projects) if projects else "-",
            topics=",".join(topics) if topics else "-",
            arches=",".join(arches) if arches else "-",
            priority=pri)

    # 详细命中线索（可选）
    if TRACE:
        if projects_hits:
            debug("[taxonomy][trace] projects 命中： " +
                  " ; ".join(f"{kind}:{proj}" for kind, proj in projects_hits))
        if topics_hits:
            debug("[taxonomy][trace] topics 命中： " +
                  " ; ".join(f"{k}(/re/..)" for k, _ in topics_hits))
        if arches_hits:
            debug("[taxonomy][trace] arches 命中： " +
                  " ; ".join(f"{k}(/re/..)" for k, _ in arches_hits))
        if high_ok or med_ok or noise_ok:
            debug("[taxonomy][trace] priority 线索： " +
                  (f"high({high_pat}) " if high_ok else "") +
                  (f"medium({med_pat}) " if (not high_ok and med_ok) else "") +
                  (f"noise_low({noise_pat}) -> force low" if noise_ok else ""))

    return result