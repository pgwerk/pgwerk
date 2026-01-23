from __future__ import annotations

import asyncio

from typing import Any
from functools import partial
from concurrent.futures import ProcessPoolExecutor

from ..schemas import Job
from .base import BaseWorker
from ..utils import import_fn
from ..utils import wants_context
from ..schemas import Context


def _run_in_subprocess(dotted: str, args: list, kwargs: dict) -> Any:
    """Entry point for pool workers. Must be top-level to be picklable."""
    import asyncio as _aio

    from wrk.utils import import_fn

    fn = import_fn(dotted)
    if _aio.iscoroutinefunction(fn):
        return _aio.run(fn(*args, **kwargs))
    return fn(*args, **kwargs)


class ProcessWorker(BaseWorker):
    """Worker that runs all functions in a ``ProcessPoolExecutor``.

    Best for CPU-bound tasks. The pool is shared across concurrent jobs,
    so a crashing worker process may cause pending futures to fail. For
    true per-job crash isolation use ``ForkWorker`` instead.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._process_pool: ProcessPoolExecutor | None = None

    async def _setup_executor(self) -> None:
        self._process_pool = ProcessPoolExecutor(max_workers=self.concurrency)

    async def _teardown_executor(self) -> None:
        if self._process_pool:
            self._process_pool.shutdown(wait=False)

    async def _execute(self, job: Job, ctx: Context) -> Any:
        _fn = import_fn(job.function)
        if wants_context(_fn):
            raise RuntimeError(
                f"{job.function} expects a Context argument, but ProcessWorker cannot inject it "
                "(Context is not picklable). Use AsyncWorker or ThreadWorker instead."
            )
        args: list = (job.payload or {}).get("args", [])
        kwargs: dict = (job.payload or {}).get("kwargs", {})
        loop = asyncio.get_running_loop()
        fut = loop.run_in_executor(
            self._process_pool,
            partial(_run_in_subprocess, job.function, args, kwargs),
        )
        return await self._with_timeout(fut, job.timeout_secs)
