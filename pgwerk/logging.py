"""Logging configuration for wrk."""

from __future__ import annotations

import sys
import json
import logging
import contextvars

from typing import Any

from tests import utils


logging.getLogger(__name__).addHandler(logging.NullHandler())


_RESET = "\x1b[0m"
_GREY = "\x1b[90m"

_LEVEL_COLORS = {
    logging.DEBUG: "\x1b[37m",  # white/gray
    logging.INFO: "\x1b[34m",  # blue
    logging.WARNING: "\x1b[33m",  # yellow
    logging.ERROR: "\x1b[31m",  # red
    logging.CRITICAL: "\x1b[1;31m",  # bold red
}

DEFAULT_FMT = "%(asctime)s  %(levelname)-8s  %(name)s  %(job_id)s %(message)s"
DEFAULT_DATEFMT = "%Y-%m-%d %H:%M:%S"

# Set by BaseWorker._handle_job; isolated per asyncio task via contextvars.
job_id_var: contextvars.ContextVar[str | None] = contextvars.ContextVar("wrk_job_id", default=None)


class _JobIdFilter(logging.Filter):
    """Injects the current job ID (if any) into every log record."""

    def filter(self, record: logging.LogRecord) -> bool:
        jid = job_id_var.get()
        record.job_id = f"[{jid.split('-')[0]}]" if jid else ""
        return True


class _ColoredFormatter(logging.Formatter):
    """Text formatter that applies ANSI colors to the level name and timestamp."""

    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        ts = super().formatTime(record, datefmt)
        return f"{_GREY}{ts}{_RESET}"

    def format(self, record: logging.LogRecord) -> str:
        record = logging.makeLogRecord(record.__dict__)
        color = _LEVEL_COLORS.get(record.levelno, "")
        record.levelname = f"{color}{record.levelname}{_RESET}"
        record.name = f"{_GREY}{record.name}{_RESET}"
        return super().format(record)


class _JsonFormatter(logging.Formatter):
    """Formatter that emits one JSON object per log record."""

    def format(self, record: logging.LogRecord) -> str:
        data: dict[str, Any] = {
            "time": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        jid = job_id_var.get()
        if jid:
            data["job_id"] = jid.split("-")[0]
        if record.exc_info:
            data["exc"] = self.formatException(record.exc_info)
        if record.stack_info:
            data["stack"] = self.formatStack(record.stack_info)
        return json.dumps(data)


def configure_logging(
    level: int | str = logging.INFO,
    format: str = "text",
    color: bool | None = True,
    fmt: str | None = None,
    datefmt: str | None = None,
    logger_name: str = "wrk",
) -> None:
    """Configure the ``werk`` logger.

    Parameters
    ----------
    level:
        Log level — an ``int`` (e.g. ``logging.DEBUG``) or a string
        (``"DEBUG"``, ``"INFO"``, ``"WARNING"``, ``"ERROR"``).
    format:
        Output format: ``"text"`` (default, human-friendly) or ``"json"``
        (one JSON object per line, suitable for log aggregators).
    color:
        Whether to apply ANSI colors to level names in text mode.
        Defaults to ``True``; pass ``False`` to disable, or ``None`` to
        auto-detect based on whether stderr is a TTY. Ignored in JSON mode.
    fmt:
        Custom ``logging.Formatter`` format string (text mode only).
        Defaults to ``DEFAULT_FMT``.
    datefmt:
        ``strftime``-style date format. Defaults to ``DEFAULT_DATEFMT``.
    """
    if isinstance(level, str):
        level = getattr(logging, level.upper(), logging.INFO)

    if color is None:
        use_color = utils.tty_supports_color(sys.stderr)
    else:
        use_color = color
    _datefmt = datefmt or DEFAULT_DATEFMT

    if format == "json":
        formatter: logging.Formatter = _JsonFormatter(datefmt=_datefmt)
    elif use_color:
        formatter = _ColoredFormatter(fmt=fmt or DEFAULT_FMT, datefmt=_datefmt)
    else:
        formatter = logging.Formatter(fmt=fmt or DEFAULT_FMT, datefmt=_datefmt)

    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(formatter)
    handler.addFilter(_JobIdFilter())

    wrk_logger = logging.getLogger(logger_name)
    wrk_logger.setLevel(level)
    wrk_logger.handlers.clear()
    wrk_logger.addHandler(handler)
    wrk_logger.propagate = False
