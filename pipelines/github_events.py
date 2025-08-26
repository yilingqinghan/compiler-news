import os, yaml, requests
from datetime import datetime, timezone
from pipelines.util import ensure_tables, pg_conn, sha1

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")

def H():
    h = {"Accept":"application/vnd.github+json"}
    if GITHUB_TOKEN: h["Authorization"] = f"Bearer {GITHUB_TOKEN}"
    return h

def ins(conn, src, link, title, ts, html=""):
    cur = conn.cursor()
    cur.execute("""
    INSERT INTO articles_raw (id, source, url, title, ts, raw_html)
    VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (id) DO NOTHING;
    """, (sha1(link), src, link, title or "(no title)", ts, html or ""))
    conn.commit(); cur.close()

def main():
    ensure_tables()
    with open("sources.yml","r",encoding="utf-8") as f:
        repos = yaml.safe_load(f).get("github_repos", [])
    conn = pg_conn()

    for r in repos:
        owner, repo = r["owner"], r["repo"]
        base = f"https://api.github.com/repos/{owner}/{repo}"
        for url in [
            f"{base}/pulls?state=all&per_page=50",
            f"{base}/issues?state=all&per_page=50",
            f"{base}/releases",
            f"{base}/commits?per_page=30",
        ]:
            try:
                resp = requests.get(url, headers=H(), timeout=20)
                if resp.status_code != 200: continue
                for e in resp.json():
                    link = e.get("html_url") or e.get("url")
                    if not link: continue
                    title = (e.get("title") or e.get("name") or e.get("commit",{}).get("message","")).split("\n")[0]
                    ts = e.get("created_at") or e.get("published_at") or e.get("commit",{}).get("author",{}).get("date")
                    ts = ts or datetime.now(timezone.utc).isoformat()
                    html = e.get("body") or ""
                    ins(conn, f"github:{owner}/{repo}", link, title, ts, html)
            except Exception as ex:
                print("github fetch error:", url, ex)

    conn.close(); print("[github_events] done")

from pipelines.util import run_cli
if __name__ == "__main__":
    run_cli(main)