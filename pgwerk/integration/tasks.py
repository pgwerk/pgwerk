"""Job functions used by integration tests.

Workers inject a Context as the first argument, so all functions accept ctx.
Callbacks receive a Job object.
"""

from __future__ import annotations

import time
import asyncio
import threading
import dataclasses


@dataclasses.dataclass
class Box:
    value: int


# ---------------------------------------------------------------------------
# Job functions — with ctx (AsyncWorker / ThreadWorker only)
# ---------------------------------------------------------------------------


def add(ctx, x, y):
    return x + y


def fail_always(ctx):
    raise ValueError("intentional failure")


def fail_once(ctx):
    """Fails on the first attempt, succeeds on subsequent attempts."""
    from pgwerk.schemas import Job

    job: Job = ctx.job
    if job.attempts <= 1:
        raise ValueError("first attempt failure")
    return "recovered"


async def async_add(ctx, x, y):
    await asyncio.sleep(0)
    return x + y


def noop(ctx):
    return None


def echo(ctx, value):
    return value


def slow(ctx, seconds: float = 0.05):
    import time

    time.sleep(seconds)
    return "done"


async def async_slow(ctx, seconds: float = 0.05):
    await asyncio.sleep(seconds)
    return "done"


# ---------------------------------------------------------------------------
# Pure functions — no ctx, picklable (ProcessWorker / ForkWorker compatible)
# ---------------------------------------------------------------------------


def add_pure(x, y):
    return x + y


async def async_add_pure(x, y):
    await asyncio.sleep(0)
    return x + y


def fail_pure():
    raise ValueError("intentional failure")


def slow_pure(seconds: float = 0.1):
    import time

    time.sleep(seconds)
    return "done"


async def async_slow_pure(seconds: float = 0.05):
    await asyncio.sleep(seconds)
    return "done"


def crash_process():
    """Aborts the process — for ForkWorker crash-isolation tests."""
    import os

    os.abort()


# ---------------------------------------------------------------------------
# Concurrency tracking — async (same event loop; AsyncWorker async jobs)
# ---------------------------------------------------------------------------

_conc_state: dict = {"current": 0, "max": 0}


def reset_conc_tracker() -> None:
    _conc_state["current"] = 0
    _conc_state["max"] = 0


async def track_conc(seconds: float = 0.05) -> None:
    _conc_state["current"] += 1
    _conc_state["max"] = max(_conc_state["max"], _conc_state["current"])
    await asyncio.sleep(seconds)
    _conc_state["current"] -= 1


# ---------------------------------------------------------------------------
# Concurrency tracking — sync with threading.Lock (ThreadWorker / executor)
# ---------------------------------------------------------------------------

_sync_conc_lock = threading.Lock()
_sync_conc_state: dict = {"current": 0, "max": 0}


def reset_sync_conc_tracker() -> None:
    with _sync_conc_lock:
        _sync_conc_state["current"] = 0
        _sync_conc_state["max"] = 0


def track_sync_conc(seconds: float = 0.1) -> None:
    import time

    with _sync_conc_lock:
        _sync_conc_state["current"] += 1
        _sync_conc_state["max"] = max(_sync_conc_state["max"], _sync_conc_state["current"])
    time.sleep(seconds)
    with _sync_conc_lock:
        _sync_conc_state["current"] -= 1


# ---------------------------------------------------------------------------
# Group-key concurrency tracking — async (AsyncWorker, same event loop)
# ---------------------------------------------------------------------------

_group_state: dict = {"current": 0, "max": 0}


def reset_group_tracker() -> None:
    _group_state["current"] = 0
    _group_state["max"] = 0


async def track_group_concurrent(seconds: float = 0.05) -> None:
    _group_state["current"] += 1
    _group_state["max"] = max(_group_state["max"], _group_state["current"])
    await asyncio.sleep(seconds)
    _group_state["current"] -= 1


# ---------------------------------------------------------------------------
# Callback functions — called with (job,), no ctx
# ---------------------------------------------------------------------------

_callback_log: list[tuple[str, str]] = []


def clear_callback_log() -> None:
    _callback_log.clear()


def on_success(job) -> None:
    _callback_log.append(("success", job.id))


def on_failure(job) -> None:
    _callback_log.append(("failure", job.id))


def on_stopped(job) -> None:
    _callback_log.append(("stopped", job.id))


async def slow_async(ctx, seconds: float = 1.0):
    await asyncio.sleep(seconds)
    return "done"


def raise_if_second_attempt(ctx):
    job = ctx.job
    if job.attempts > 1:
        raise ValueError("second attempt failure")
    raise ValueError("first attempt failure")


# ---------------------------------------------------------------------------
# Deterministic worker-coordination helpers
# ---------------------------------------------------------------------------

_blocking_started = threading.Event()
_blocking_release = threading.Event()
_execution_log: list[tuple[str, int, str]] = []
_side_effect_log: list[tuple[str, int, str]] = []


def reset_blocking_state() -> None:
    _blocking_started.clear()
    _blocking_release.clear()
    _execution_log.clear()
    _side_effect_log.clear()


def wait_for_blocking_start(timeout: float = 5.0) -> bool:
    return _blocking_started.wait(timeout)


def release_blocking_job() -> None:
    _blocking_release.set()


def record_execution(ctx, label: str = "exec"):
    _execution_log.append((label, ctx.job.attempts, ctx.worker.id))
    _side_effect_log.append((label, ctx.job.attempts, ctx.worker.id))
    return {"label": label, "attempt": ctx.job.attempts, "worker_id": ctx.worker.id}


def block_and_record(ctx, label: str = "block", seconds_after_release: float = 0.0):
    _execution_log.append((label, ctx.job.attempts, ctx.worker.id))
    _blocking_started.set()
    _blocking_release.wait(timeout=10.0)
    if seconds_after_release:
        time.sleep(seconds_after_release)
    _side_effect_log.append((label, ctx.job.attempts, ctx.worker.id))
    return {"label": label, "attempt": ctx.job.attempts, "worker_id": ctx.worker.id}
