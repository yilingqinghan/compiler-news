# scripts/build_archive.py
import os, re, datetime, glob

DIST = "web/dist"
def link(name): return f'<li><a href="{name}">{name}</a></li>'

def main():
    os.makedirs(DIST, exist_ok=True)
    weeklies = sorted(glob.glob(f"{DIST}/weekly-*.html"), reverse=True)
    dailies  = sorted([p for p in glob.glob(f"{DIST}/20*.html") if not os.path.basename(p).startswith("weekly-")], reverse=True)

    ul_week = "\n".join(link(os.path.basename(p)) for p in weeklies)
    ul_day  = "\n".join(link(os.path.basename(p)) for p in dailies[:60])  # 只列最近 60 天，更多请改

    html = f"""<!doctype html><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>编译器日报/周报归档</title>
<body style="font-family:-apple-system,Segoe UI,Roboto,Helvetica Neue,Arial;margin:20px;line-height:1.5">
<h1>编译器日报/周报归档</h1>
<p><a href="{os.path.basename(weeklies[0]) if weeklies else ''}">👉 打开最新周报</a></p>
<h2>周报</h2><ul>{ul_week}</ul>
<h2>最近日更</h2><ul>{ul_day}</ul>
<p style="color:#999">自动生成于 {datetime.datetime.now().isoformat()}</p>
</body>"""
    with open(f"{DIST}/index.html","w",encoding="utf-8") as f: f.write(html)

from pipelines.util import run_cli
if __name__ == "__main__":
    run_cli(main)