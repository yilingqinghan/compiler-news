# pipelines/extract.py
# =======================================================================
#  清洗原始 HTML，抽取正文与元信息 -> 写入 articles_clean
#  - 统一日志：阶段耗时、spinner、进度条、kv 表汇总
#  - 多策略内容抽取（trafilatura / readability / goose）+ 文本化兜底
#  - 可配置：批处理上限、最小文本长度
# =======================================================================

from __future__ import annotations
import os
import json
from typing import List, Tuple

from bs4 import BeautifulSoup
from trafilatura import extract as t_extract
from readability import Document
from goose3 import Goose

from pipelines.util import ensure_tables, pg_conn, run_cli
from pipelines.taxonomy import classify
from pipelines.logging_utils import (
    info, debug, warn, error, success, kv_line, kv_table,
    status, new_progress, step
)

# ------------------------- 可配置参数 -------------------------
EXTRACT_LIMIT       = int(os.getenv("EXTRACT_LIMIT", "2000"))   # 一次处理多少 rows
MIN_TEXT_LEN        = int(os.getenv("MIN_TEXT_LEN", "300"))     # 单个抽取器产物的最小长度
MAX_FALLBACK_LEN    = int(os.getenv("MAX_FALLBACK_LEN", "1200"))# 兜底文本最大长度


# ------------------------- 工具函数 -------------------------
def _textify(html: str) -> str:
    """将 HTML 粗暴文本化（保留空格分隔），并做简单去噪。"""
    if not html:
        return ""
    try:
        soup = BeautifulSoup(html, "lxml")
        return " ".join(soup.get_text(" ").split())
    except Exception:
        # 解析失败时截取一段，避免返回超长原文
        return (html or "")[:500]


def clean_text(html: str, url: str) -> str:
    """多策略抽取正文；任一成功即返回；最后兜底为文本化摘要。"""
    if not html:
        return ""

    # 候选提取器：优先高精度，然后可读性，再 Goose
    strategies = (
        lambda h: t_extract(h, include_tables=True, favor_precision=True, url=url),
        lambda h: Document(h).summary(html_partial=False),
        lambda h: (Goose().extract(raw_html=h).cleaned_text if h else None),
    )

    for fn in strategies:
        try:
            txt = fn(html)
            if txt and len(txt) > MIN_TEXT_LEN:
                return _textify(txt)
        except Exception as ex:
            debug(f"[extract] 清洗器异常：{type(fn).__name__ if hasattr(fn,'__name__') else 'fn'} -> {ex}")

    # 兜底：把 feed 的 summary/片段文本化
    return _textify(html)[:MAX_FALLBACK_LEN]


# ------------------------- 主流程 -------------------------
@step("Extract Clean Text")
def main():
    ensure_tables()
    conn = pg_conn()
    cur = conn.cursor()

    kv_line("[extract] 参数",
            limit=EXTRACT_LIMIT,
            min_text_len=MIN_TEXT_LEN,
            fallback_len=MAX_FALLBACK_LEN)

    # 读取待处理 rows
    cur.execute(
        "SELECT id, source, url, title, ts, raw_html "
        "FROM articles_raw "
        "ORDER BY ts DESC "
        "LIMIT %s;",
        (EXTRACT_LIMIT,)
    )
    rows: List[Tuple[str, str, str, str, int, str]] = cur.fetchall()
    if not rows:
        warn("[extract] 没有待清洗的原始文章（articles_raw 为空）")
        cur.close(); conn.close(); return

    info(f"[extract] 准备处理 rows={len(rows)}")

    created = 0
    skipped_exists = 0
    empty_html = 0

    # 逐条处理，带进度条
    with new_progress() as progress:
        task = progress.add_task("清洗正文与分类", total=len(rows))

        for id_, source, url, title, ts, raw_html in rows:
            progress.advance(task, 1)

            # 已存在则跳过
            cur2 = conn.cursor()
            cur2.execute("SELECT 1 FROM articles_clean WHERE id=%s", (id_,))
            if cur2.fetchone():
                skipped_exists += 1
                cur2.close()
                continue
            cur2.close()

            # 清洗与提取
            text = ""
            if raw_html and len(raw_html) > 20:
                with status(f"[{source}] 抽取正文 …", spinner="dots"):
                    try:
                        text = clean_text(raw_html, url)
                    except Exception as ex:
                        error(f"[extract] clean_text 失败：{ex}")
                        text = ""
            else:
                empty_html += 1

            # 元数据分类
            meta = {"source": source}
            try:
                meta.update(classify(title or "", text or "", url or "", source or ""))
            except Exception as ex:
                warn(f"[extract] 分类失败（回退基本元信息）：{ex}")

            # 入库
            cur3 = conn.cursor()
            cur3.execute("""
                INSERT INTO articles_clean (id, source, url, title, ts, text, metadata)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (id) DO NOTHING;
            """, (id_, source, url, title, ts, text, json.dumps(meta, ensure_ascii=False)))
            conn.commit()
            cur3.close()
            created += 1

    cur.close(); conn.close()

    # 汇总输出
    kv_table("[extract] 汇总", {
        "rows_in": len(rows),
        "inserted": created,
        "skipped_exists": skipped_exists,
        "empty_html": empty_html,
    })
    success(f"[extract] created ~{created} clean rows")


# ------------------------- CLI 入口 -------------------------
if __name__ == "__main__":
    run_cli(main)