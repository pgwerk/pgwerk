"""Utility functions shared across app, workers, and scheduler."""

from __future__ import annotations

import sys
import asyncio
import hashlib
import inspect
import logging
import importlib

from typing import TYPE_CHECKING
from typing import Any
from typing import Callable
from datetime import datetime
from datetime import timezone

from wrk.schemas import CronJob


if TYPE_CHECKING:
    from .schemas import Job
    from .schemas import Retry
    from .schemas import Context
    from .schemas import Callback
    from .schemas import Dependency

logger = logging.getLogger(__name__)


def tty_supports_color(stream: Any = None) -> bool:
    stream = stream or sys.stderr
    return hasattr(stream, "isatty") and stream.isatty()


def tick_dedupe_key(cjob: CronJob) -> str:
    if cjob.interval:
        bucket = int(datetime.now(timezone.utc).timestamp() // cjob.interval)
        return f"_wrk_cron:{cjob.name}:interval:{bucket}"

    if cjob.next_run_at is not None:
        nxt = cjob.next_run_at
        if nxt.tzinfo is None:
            nxt = nxt.replace(tzinfo=timezone.utc)
        return f"_wrk_cron:{cjob.name}:cron:{nxt.isoformat()}"

    return f"_wrk_cron:{cjob.name}:fallback:{int(datetime.now(timezone.utc).timestamp())}"


def advisory_key(s: str) -> int:
    """Derive a stable PostgreSQL advisory lock key from a string.

    Uses blake2b so the result is identical across processes regardless of
    PYTHONHASHSEED. Returns a non-negative int64 (fits pg's bigint).
    """
    digest = hashlib.blake2b(s.encode(), digest_size=8).digest()
    return int.from_bytes(digest, "big", signed=False) & 0x7FFFFFFFFFFFFFFF


# ---------------------------------------------------------------------------
# Worker helpers
# ---------------------------------------------------------------------------


def wants_context(fn: Callable) -> bool:
    """Return True if *fn*'s first positional parameter is named ``ctx`` or annotated as Context."""
    try:
        params = [
            p
            for p in inspect.signature(fn).parameters.values()
            if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)
        ]
        if not params:
            return False
        first = params[0]
        if first.name == "ctx":
            return True
        ann = first.annotation
        if ann is inspect.Parameter.empty:
            return False
        from .schemas import Context

        return ann is Context or (isinstance(ann, str) and ann == "Context")
    except (ValueError, TypeError):
        return False


async def call_hook(hook: Callable, ctx: "Context") -> None:
    result = hook(ctx)
    if asyncio.iscoroutine(result):
        await result


async def invoke_callback(dotted: str, job: "Job", timeout: int | None = None) -> None:
    """Import and call a callback by dotted path. Never raises."""
    try:
        module_path, name = dotted.rsplit(".", 1)
        fn = getattr(importlib.import_module(module_path), name)
        coro = fn(job)
        if asyncio.iscoroutine(coro):
            if timeout:
                await asyncio.wait_for(coro, timeout=timeout)
            else:
                await coro
    except asyncio.TimeoutError:
        logger.warning("Callback %s timed out for job %s", dotted, job.id)
    except Exception as exc:
        logger.exception("Callback %s failed for job %s: %s", dotted, job.id, exc)


# ---------------------------------------------------------------------------
# Enqueue helpers
# ---------------------------------------------------------------------------


def import_fn(dotted: str) -> Callable:
    """Import a callable from its dotted module path."""
    module_path, name = dotted.rsplit(".", 1)
    return getattr(importlib.import_module(module_path), name)


def fn_path(func: Callable) -> str:
    qualname = func.__qualname__
    if "<locals>" in qualname or "<lambda>" in qualname:
        raise ValueError(
            f"Cannot enqueue {func!r}: only module-level functions are supported (got qualname={qualname!r})."
        )
    return f"{func.__module__}.{qualname}"


def normalize_retry(retry: "Retry | int | None") -> tuple[int, list[int] | None]:
    if retry is None:
        return 3, None  # default: 3 attempts, saner than 1 for transient failures
    if retry == 0:
        return 1, None
    if isinstance(retry, int):
        return retry, None
    if isinstance(retry.intervals, list):
        return retry.max, retry.intervals if retry.intervals else None
    if retry.intervals:  # uniform int > 0
        return retry.max, [retry.intervals]
    return retry.max, None


def normalize_callback(
    cb: "Callback | Callable | str | None",
) -> tuple[str | None, int | None]:
    if cb is None:
        return None, None
    from .schemas import Callback as _Callback

    if isinstance(cb, _Callback):
        return cb.path(), cb.timeout
    if callable(cb):
        return fn_path(cb), None
    return cb, None


def normalize_depends_on(
    depends_on: "list[Dependency | str | Job] | Dependency | str | Job | None",
) -> list[tuple[str, bool]]:
    if depends_on is None:
        return []
    if not isinstance(depends_on, list):
        depends_on = [depends_on]
    result = []
    from .schemas import Job as _Job
    from .schemas import Dependency as _Dependency

    for d in depends_on:
        if isinstance(d, _Dependency):
            result.append((d.job_id, d.allow_failure))
        elif isinstance(d, _Job):
            result.append((d.id, False))
        else:
            result.append((str(d), False))
    return result
