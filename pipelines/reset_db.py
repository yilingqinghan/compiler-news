# pipelines/reset_db.py
# ======================================================================
#  数据库重置工具
#  - 统一风格日志：阶段耗时、spinner、进度条、成功/失败提示
#  - 兼容原参数：--hard / --recreate
#  - 不改表结构与业务逻辑
# ======================================================================

from __future__ import annotations
import argparse
import os
from dotenv import load_dotenv
import psycopg2
from typing import List

# 日志与工具
from pipelines.logging_utils import (
    info, debug, warn, error, success,
    kv_line, status, new_progress, step
)

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


# --------------------------- 内部工具 ---------------------------

def _split_sql(sql: str) -> List[str]:
    """把多语句 SQL 拆分为条目；忽略空白。"""
    return [s.strip() for s in (sql or "").split(";") if s.strip()]


def _exec_many(conn, stmts: List[str]) -> int:
    """
    顺序执行多条 SQL，带进度条。
    返回成功执行的语句条数。
    """
    if not stmts:
        return 0

    ok = 0
    with new_progress() as progress:
        task = progress.add_task("执行 SQL 语句", total=len(stmts))
        cur = conn.cursor()
        for s in stmts:
            # 简洁预览：只显示首行与长度
            preview = s.splitlines()[0][:80]
            debug(f"[reset_db] SQL: {preview} ...")
            try:
                cur.execute(s + ";")
                ok += 1
            except Exception as ex:
                # 单条失败不隐藏，抛出到上层统一处理
                error(f"[reset_db] SQL 执行失败：{ex}")
                cur.close()
                raise
            finally:
                progress.advance(task, 1)
        conn.commit()
        cur.close()
    return ok


def _connect():
    """建立数据库连接，失败时抛出异常。"""
    with status("[reset_db] 连接数据库 …", spinner="dots"):
        conn = psycopg2.connect(POSTGRES_URL)
    return conn


# --------------------------- 核心执行 ---------------------------

def run(sql: str) -> int:
    """
    执行一段（可能包含多条语句的）SQL。
    返回成功执行的语句数。
    """
    stmts = _split_sql(sql)
    kv_line("[reset_db] 待执行", statements=len(stmts))
    if not stmts:
        warn("[reset_db] 没有可执行的 SQL，跳过")
        return 0

    conn = _connect()
    try:
        return _exec_many(conn, stmts)
    finally:
        conn.close()


# --------------------------- CLI 入口 ---------------------------

@step("Reset DB")
def main():
    ap = argparse.ArgumentParser(description="Reset intel DB quickly.")
    ap.add_argument("--hard", action="store_true", help="Drop tables instead of truncate.")
    ap.add_argument("--recreate", action="store_true", help="Drop then recreate tables via ensure_tables().")
    args = ap.parse_args()

    kv_line("[reset_db] 连接信息", url=POSTGRES_URL)

    try:
        if args.recreate:
            # 硬重置并重建表
            from pipelines.util import ensure_tables
            info("[reset_db] 模式：DROP -> RECREATE")
            n = run(DROP_SQL)
            success(f"[reset_db] DROP 完成（{n} 条语句）")
            with status("[reset_db] 重建表 ensure_tables() …", spinner="dots"):
                ensure_tables()
            success("[reset_db] RECREATE 完成")
        elif args.hard:
            info("[reset_db] 模式：DROP")
            n = run(DROP_SQL)
            success(f"[reset_db] DROP 完成（{n} 条语句）")
        else:
            info("[reset_db] 模式：TRUNCATE")
            n = run(SOFT_SQL)
            success(f"[reset_db] TRUNCATE 完成（{n} 条语句）")
    except Exception as ex:
        error(f"[reset_db] 失败：{ex}")
        raise


if __name__ == "__main__":
    main()