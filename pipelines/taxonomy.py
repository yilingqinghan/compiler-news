import re, yaml
from typing import Dict, List, Tuple

with open("taxonomy.yml", "r", encoding="utf-8") as f:
    TAX = yaml.safe_load(f)

def _match_any(patterns: List[str], text: str) -> bool:
    return any(re.search(p, text or "", flags=re.IGNORECASE) for p in patterns)

def classify(title: str, text: str, url: str, source: str) -> Dict:
    blob = " ".join([title or "", text or "", url or "", source or ""])
    projects = [k for k, pats in TAX["projects"].items() if _match_any(pats, blob)]
    if not projects: projects = ["Others"]

    topics = [k for k, pats in TAX["topics"].items() if _match_any(pats, blob)]
    arches = [k for k, pats in TAX["arches"].items() if _match_any(pats, blob)]

    # 简单优先级：命中高>中>低；加上“回归/安全/弃用/ABI”等词权重
    pri = "low"
    txt = f"{title} {text}"
    if any(re.search(x, txt, re.I) for x in TAX["priority_rules"]["high"]):   pri = "high"
    elif any(re.search(x, txt, re.I) for x in TAX["priority_rules"]["medium"]): pri = "medium"

    return {
        "projects": sorted(set(projects)),
        "topics": sorted(set(topics)),
        "arches": sorted(set(arches)),
        "priority": pri
    }