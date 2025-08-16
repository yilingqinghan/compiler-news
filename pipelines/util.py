import os, json, pathlib, hashlib
from datetime import datetime, timezone
from dotenv import load_dotenv
import psycopg2

load_dotenv()

POSTGRES_URL = os.getenv("POSTGRES_URL", "postgresql://compiler:compiler@localhost:5432/compiler_intel")

def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def pg_conn():
    return psycopg2.connect(POSTGRES_URL)

def ensure_tables():
    conn = pg_conn()
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS articles_raw (
        id TEXT PRIMARY KEY,
        source TEXT,
        url TEXT,
        title TEXT,
        ts TIMESTAMP,
        raw_html TEXT,
        fetched_at TIMESTAMP DEFAULT NOW()
    );""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS articles_clean (
        id TEXT PRIMARY KEY,
        source TEXT,
        url TEXT,
        title TEXT,
        ts TIMESTAMP,
        text TEXT,
        metadata JSONB,
        created_at TIMESTAMP DEFAULT NOW()
    );""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS clusters (
        cluster_id TEXT,
        id TEXT PRIMARY KEY,
        title TEXT,
        summary JSONB,
        created_at TIMESTAMP DEFAULT NOW()
    );""")
    conn.commit()
    cur.close(); conn.close()
