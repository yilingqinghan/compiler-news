import logging, os
from rich.console import Console
from rich.panel import Panel
from rich.logging import RichHandler

console = Console()

def get_logger(name="intel"):
    logging.basicConfig(
        level=os.getenv("LOGLEVEL","INFO"),
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(rich_tracebacks=True)]
    )
    return logging.getLogger(name)

def banner(title: str, sub: str = "", style="cyan"):
    console.print(Panel.fit(f"[b]{title}[/b]\n[dim]{sub}[/dim]" if sub else f"[b]{title}[/b]",
                            border_style=style))