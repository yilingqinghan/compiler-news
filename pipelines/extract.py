import json
from bs4 import BeautifulSoup
from pipelines.util import ensure_tables, pg_conn
from trafilatura import extract as t_extract
from readability import Document
from goose3 import Goose
from pipelines.taxonomy import classify

def textify(html):
    if not html: return ""
    try:
        soup = BeautifulSoup(html, "lxml")
        return " ".join(soup.get_text(" ").split())
    except Exception:
        return html[:500]

def clean_text(html, url):
    for fn in (
        lambda h: t_extract(h, include_tables=True, favor_precision=True, url=url),
        lambda h: Document(h).summary(html_partial=False),
        lambda h: (Goose().extract(raw_html=h).cleaned_text if h else None),
    ):
        try:
            txt = fn(html)
            if txt and len(txt) > 300: return textify(txt)
        except Exception: pass
    # 兜底：把 feed 的 summary/片段文本化
    return textify(html)[:1200]

def main():
    ensure_tables()
    conn = pg_conn(); cur = conn.cursor()
    cur.execute("SELECT id, source, url, title, ts, raw_html FROM articles_raw ORDER BY ts DESC LIMIT 2000;")
    rows = cur.fetchall(); created = 0
    for id_, source, url, title, ts, raw_html in rows:
        cur2 = conn.cursor(); cur2.execute("SELECT 1 FROM articles_clean WHERE id=%s", (id_,))
        if cur2.fetchone(): cur2.close(); continue
        cur2.close()

        text = clean_text(raw_html, url) if (raw_html and len(raw_html) > 20) else ""
        meta = {"source": source}
        meta.update(classify(title or "", text or "", url or "", source or ""))

        cur3 = conn.cursor()
        cur3.execute("""
        INSERT INTO articles_clean (id, source, url, title, ts, text, metadata)
        VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (id) DO NOTHING;
        """, (id_, source, url, title, ts, text, json.dumps(meta, ensure_ascii=False)))
        conn.commit(); cur3.close(); created += 1

    cur.close(); conn.close()
    print(f"[extract] created ~{created} clean rows")

from pipelines.util import run_cli
if __name__ == "__main__":
    run_cli(main)