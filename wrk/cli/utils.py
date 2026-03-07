from __future__ import annotations

import re
import importlib

from datetime import datetime
from datetime import timezone
from datetime import timedelta

import click


ALL_STATUSES = ["scheduled", "queued", "active", "waiting", "complete", "failed", "aborted", "aborting"]

STATUS_STYLES = {
    "scheduled": "cyan",
    "queued": "bright_blue",
    "active": "bright_green",
    "waiting": "yellow",
    "complete": "green",
    "failed": "bright_red",
    "aborted": "orange1",
    "aborting": "dark_orange",
}


def load_app(app_string: str):
    """Load a Wrk instance from a ``module:attribute`` string."""
    try:
        module_path, attr = app_string.rsplit(":", 1)
    except ValueError:
        raise click.BadParameter(f"Expected 'module:attribute', got {app_string!r}", param_hint="APP")
    try:
        module = importlib.import_module(module_path)
    except ImportError as exc:
        raise click.ClickException(f"Cannot import {module_path!r}: {exc}")
    try:
        return getattr(module, attr)
    except AttributeError:
        raise click.ClickException(f"{module_path!r} has no attribute {attr!r}")


def require_rich():
    try:
        from rich.console import Console

        return Console()
    except ImportError:
        raise click.ClickException("This command requires 'rich'. Install with: pip install 'wrk[analytics]'")


def bar(value: int, total: int, width: int = 28) -> str:
    if total == 0:
        return "░" * width
    filled = max(0, round((value / total) * width))
    return "█" * filled + "░" * (width - filled)


def fmt_duration(secs: float | None) -> str:
    if secs is None:
        return "—"
    if secs < 0.001:
        return "<1ms"
    if secs < 1:
        return f"{secs * 1000:.0f}ms"
    if secs < 60:
        return f"{secs:.1f}s"
    if secs < 3600:
        return f"{secs / 60:.1f}m"
    return f"{secs / 3600:.1f}h"


def fmt_age(dt: datetime | None) -> str:
    if dt is None:
        return "—"
    now = datetime.now(timezone.utc)
    aware = dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt
    secs = (now - aware).total_seconds()
    if secs < 0:
        return "just now"
    if secs < 60:
        return f"{secs:.0f}s ago"
    if secs < 3600:
        return f"{secs / 60:.0f}m ago"
    if secs < 86400:
        return f"{secs / 3600:.1f}h ago"
    return f"{secs / 86400:.0f}d ago"


def parse_since(since: str) -> datetime:
    m = re.fullmatch(r"(\d+)([hdm])", since.strip().lower())
    if not m:
        raise click.BadParameter(f"Invalid duration {since!r}, expected e.g. 24h, 7d, 30m", param_hint="--since")
    n, unit = int(m.group(1)), m.group(2)
    delta = {"h": timedelta(hours=n), "d": timedelta(days=n), "m": timedelta(minutes=n)}[unit]
    return datetime.now(timezone.utc) - delta


def short_func(func: str | None) -> str:
    if not func:
        return "?"
    name = func.rsplit(".", 1)[-1] if "." in func else func
    return name[:27] + "…" if len(name) > 28 else name
