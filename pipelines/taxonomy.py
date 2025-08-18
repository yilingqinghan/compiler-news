# pipelines/taxonomy.py
import re, yaml, tldextract
from typing import Dict, List

with open("taxonomy.yml", "r", encoding="utf-8") as f:
    TAX = yaml.safe_load(f)

def _match_any(pats: List[str], text: str) -> bool:
    return any(re.search(p, text or "", re.I) for p in pats)

def _host(url: str) -> str:
    try:
        ext = tldextract.extract(url or "")
        return ".".join([p for p in [ext.domain, ext.suffix] if p])
    except Exception: return ""

def classify(title: str, text: str, url: str, source: str) -> Dict:
    blob = " ".join([title or "", text or "", url or "", source or ""])
    host = _host(url)

    projects = []
    for proj, hints in (TAX.get("source_hints") or {}).items():
        if any(h.lower() in (source or "").lower() for h in hints): projects.append(proj)
    for proj, hints in (TAX.get("host_hints") or {}).items():
        if any(h in (host or "") for h in hints): projects.append(proj)
    for proj, pats in TAX["projects"].items():
        if _match_any(pats, blob): projects.append(proj)
    projects = sorted(set(projects)) or ["Others"]

    topics = sorted({k for k, pats in TAX["topics"].items() if _match_any(pats, blob)})
    arches = sorted({k for k, pats in TAX["arches"].items() if _match_any(pats, blob)})

    pri = "low"
    if any(re.search(x, blob, re.I) for x in TAX["priority_rules"]["high"]): pri = "high"
    elif any(re.search(x, blob, re.I) for x in TAX["priority_rules"]["medium"]): pri = "medium"

    # 噪声降权（NFC/格式化/whitespace/typo/revert…）
    for p in (TAX.get("noise_low") or []):
        if re.search(p, blob, re.I): pri = "low"; break

    return {"projects": projects, "topics": topics, "arches": arches, "priority": pri}