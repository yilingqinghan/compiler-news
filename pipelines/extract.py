import json
from pipelines.util import ensure_tables, pg_conn
from trafilatura import extract as t_extract
from readability import Document
from goose3 import Goose

def clean_text(html, url):
    try:
        txt = t_extract(html, include_tables=True, favor_precision=True, url=url)
        if txt and len(txt) > 300:
            return txt
    except Exception: pass
    try:
        doc = Document(html)
        txt = doc.summary(html_partial=False)
        if txt and len(txt) > 300:
            return txt
    except Exception: pass
    try:
        g = Goose()
        art = g.extract(raw_html=html)
        if art and art.cleaned_text and len(art.cleaned_text) > 300:
            return art.cleaned_text
    except Exception: pass
    return None

def main():
    ensure_tables()
    conn = pg_conn()
    cur = conn.cursor()
    cur.execute("SELECT id, source, url, title, ts, raw_html FROM articles_raw ORDER BY ts DESC LIMIT 1000;")
    rows = cur.fetchall()
    created = 0
    for id_, source, url, title, ts, raw_html in rows:
        cur2 = conn.cursor()
        cur2.execute("SELECT 1 FROM articles_clean WHERE id=%s", (id_,))
        if cur2.fetchone():
            cur2.close(); continue
        cur2.close()
        text = ""
        if raw_html and len(raw_html) > 100:
            text = clean_text(raw_html, url) or ""
        meta = {"source": source}
        cur3 = conn.cursor()
        cur3.execute("""
        INSERT INTO articles_clean (id, source, url, title, ts, text, metadata)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING;
        """, (id_, source, url, title, ts, text, json.dumps(meta)))
        conn.commit(); cur3.close(); created += 1
    cur.close(); conn.close()
    print(f"[extract] created ~{created} clean rows")

if __name__ == "__main__":
    main()
