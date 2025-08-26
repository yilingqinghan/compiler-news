# pipelines/logging_utils.py
from __future__ import annotations
import os, time, functools, contextlib
from typing import Iterator, Optional, Any, Dict

from rich.console import Console
from rich.theme import Theme
from rich.traceback import install as rich_traceback_install
from rich.progress import (
    Progress, SpinnerColumn, BarColumn, TextColumn,
    TimeElapsedColumn, TimeRemainingColumn, MofNCompleteColumn
)
from rich.panel import Panel
from rich.table import Table

# ---------- Config ----------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()   # DEBUG/INFO/WARN/ERROR
USE_RICH  = os.getenv("LOG_RICH", "1") == "1"        # 0=纯文本
NO_COLOR  = os.getenv("NO_COLOR", "0") == "1"        # 兼容 no-color

theme = Theme({
    "ts": "grey62",
    "lvl.debug": "dim",
    "lvl.info": "cyan",
    "lvl.warn": "yellow",
    "lvl.error": "bold red",
    "ok": "bold green",
    "key": "bold white",
    "val": "white",
    "muted": "grey58",
})

console = Console(theme=theme, highlight=False, color_system=None if NO_COLOR else "auto")

if USE_RICH:
    # 让异常栈也更好看（不泄露局部变量）
    rich_traceback_install(show_locals=False, width=120, word_wrap=True)

# ---------- Level gate ----------
_levels = {"DEBUG": 10, "INFO": 20, "WARN": 30, "ERROR": 40}
_cur = _levels.get(LOG_LEVEL, 20)

def _enabled(level: str) -> bool:
    return _levels[level] >= _cur

# ---------- Pretty log APIs ----------
def _tag(level: str) -> str:
    return {
        "DEBUG": "[lvl.debug]·DBG[/]",
        "INFO":  "[lvl.info]ℹ[/]",
        "WARN":  "[lvl.warn]⚠[/]",
        "ERROR": "[lvl.error]✖[/]",
    }[level] if USE_RICH else level

def _ts() -> str:
    return time.strftime("[%H:%M:%S]")

def debug(msg: str) -> None:
    if _enabled("DEBUG"):
        console.print(f"[ts]{_ts()}[/] {_tag('DEBUG')} {msg}")

def info(msg: str) -> None:
    if _enabled("INFO"):
        console.print(f"[ts]{_ts()}[/] {_tag('INFO')}  {msg}")

def warn(msg: str) -> None:
    if _enabled("WARN"):
        console.print(f"[ts]{_ts()}[/] {_tag('WARN')}  {msg}")

def error(msg: str) -> None:
    if _enabled("ERROR"):
        console.print(f"[ts]{_ts()}[/] {_tag('ERROR')} {msg}")

def success(msg: str) -> None:
    console.print(f"[ts]{_ts()}[/] [ok]✔[/] {msg}")

# k=v 样式行
def kv_line(title: str, **kv: Any) -> None:
    if not kv:
        info(title); return
    parts = []
    for k, v in kv.items():
        parts.append(f"[key]{k}[/]=[val]{v}[/]" if USE_RICH else f"{k}={v}")
    console.print(f"[ts]{_ts()}[/] {_tag('INFO')}  {title}  " + "  ".join(parts))

# 表格
def kv_table(title: str, rows: Dict[str, Any]) -> None:
    if not USE_RICH:
        info(title + " " + " ".join(f"{k}={v}" for k, v in rows.items()))
        return
    table = Table(show_header=False, box=None, pad_edge=False)
    table.add_column("k", style="key")
    table.add_column("v", style="val")
    for k, v in rows.items():
        table.add_row(str(k), str(v))
    console.print(Panel(table, title=title, border_style="muted"))

# ---------- Spinners / Progress ----------
@contextlib.contextmanager
def status(text: str, spinner: str = "dots") -> Iterator[None]:
    """npm风格转圈：with status('Fetching …'): ..."""
    if not USE_RICH:
        info(text)
        yield
        return
    with console.status(text, spinner=spinner):
        yield

def new_progress(transient: bool = True) -> Progress:
    """创建统一风格进度条（总进度/子任务通用）"""
    return Progress(
        SpinnerColumn(style="muted"),
        TextColumn("[bold]{task.description}[/]"),
        BarColumn(bar_width=None),
        MofNCompleteColumn(),  # 已完成/总数
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        console=console,
        transient=transient,
        expand=True,
    )

# ---------- Decorators ----------
def step(title: str):
    """@step('Fetch RSS'): 自动开始/结束+耗时统计"""
    def deco(fn):
        @functools.wraps(fn)
        def wrapper(*a, **kw):
            info(f"[{title}] 开始")
            t0 = time.perf_counter()
            try:
                r = fn(*a, **kw)
                dt = time.perf_counter() - t0
                success(f"[{title}] 完成，用时 {dt:.2f}s")
                return r
            except Exception as ex:
                dt = time.perf_counter() - t0
                error(f"[{title}] 失败（{dt:.2f}s）：{ex}")
                raise
        return wrapper
    return deco