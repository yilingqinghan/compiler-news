# pipelines/reset_db.py
import argparse, os
from dotenv import load_dotenv
import psycopg2

load_dotenv()
POSTGRES_URL = os.getenv("POSTGRES_URL", "postgresql://intel:intelpwd@localhost:5432/intel")

SOFT_SQL = """
TRUNCATE TABLE clusters, articles_clean, articles_raw;
"""

DROP_SQL = """
DROP TABLE IF EXISTS clusters;
DROP TABLE IF EXISTS articles_clean;
DROP TABLE IF EXISTS articles_raw;
"""

def run(sql: str):
    conn = psycopg2.connect(POSTGRES_URL)
    cur = conn.cursor()
    for stmt in [s.strip() for s in sql.split(";") if s.strip()]:
        cur.execute(stmt + ";")
    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Reset intel DB quickly.")
    ap.add_argument("--hard", action="store_true", help="Drop tables instead of truncate.")
    ap.add_argument("--recreate", action="store_true", help="Drop then recreate tables via ensure_tables().")
    args = ap.parse_args()

    if args.recreate:
        # 硬重置并重建表
        from pipelines.util import ensure_tables
        run(DROP_SQL)
        ensure_tables()
        print("[reset_db] DROP -> RECREATE done.")
    elif args.hard:
        run(DROP_SQL)
        print("[reset_db] DROP done.")
    else:
        run(SOFT_SQL)
        print("[reset_db] TRUNCATE done.")