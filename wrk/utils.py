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
    """Return True if *stream* is a TTY that likely supports ANSI colour codes.

    Args:
        stream: File-like object to test; defaults to ``sys.stderr``.

    Returns:
        ``True`` if the stream has ``isatty()`` and it returns a truthy value.
    """
    stream = stream or sys.stderr
    return hasattr(stream, "isatty") and stream.isatty()


def tick_dedupe_key(cjob: CronJob) -> str:
    """Return a deduplication key for a single cron tick.

    The key is stable within the current scheduling bucket so that concurrent
    scheduler instances produce the same key and the database deduplication
    logic silently drops the duplicate enqueue.

    Args:
        cjob: The CronJob being enqueued.

    Returns:
        A unique string key scoped to this cron job and scheduling bucket.
    """
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
    """Call *hook* with *ctx*, awaiting it if it returns a coroutine.

    Args:
        hook: A sync or async callable that accepts a single Context argument.
        ctx: The execution context to pass to the hook.
    """
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
    """Return the dotted module path for *func*.

    Args:
        func: A module-level callable.

    Returns:
        ``module.qualname`` string suitable for dynamic import.

    Raises:
        ValueError: If *func* is a local function or lambda that cannot be
            imported by dotted path.
    """
    qualname = func.__qualname__
    if "<locals>" in qualname or "<lambda>" in qualname:
        raise ValueError(
            f"Cannot enqueue {func!r}: only module-level functions are supported (got qualname={qualname!r})."
        )
    return f"{func.__module__}.{qualname}"


def normalize_retry(retry: "Retry | int | None") -> tuple[int, list[int] | None]:
    """Normalise a ``retry`` argument to ``(max_attempts, intervals_or_None)``.

    Args:
        retry: A :class:`~wrk.schemas.Retry` object, a plain integer (max
            attempts with no delay), or ``None`` (use library defaults).

    Returns:
        A tuple of ``(max_attempts, retry_intervals)`` where ``retry_intervals``
        is a list of per-attempt delays in seconds, or ``None`` for immediate retry.
    """
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
    """Normalise a callback argument to ``(dotted_path, timeout_or_None)``.

    Args:
        cb: A :class:`~wrk.schemas.Callback` object, a callable, a dotted
            import path string, or ``None``.

    Returns:
        A tuple of ``(dotted_path, timeout_secs)`` or ``(None, None)`` if *cb*
        is ``None``.
    """
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
    """Normalise a ``depends_on`` argument to a list of ``(job_id, allow_failure)`` tuples.

    Args:
        depends_on: A single dependency or a list of dependencies expressed as
            :class:`~wrk.schemas.Dependency` objects, :class:`~wrk.schemas.Job`
            objects, or raw UUID strings.

    Returns:
        A list of ``(job_id_str, allow_failure)`` tuples ready for DB insertion.
    """
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
