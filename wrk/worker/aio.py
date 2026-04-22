"""AsyncWorker — runs async coroutines natively; sync functions via the default executor.

Note: this file is named ``aio.py`` because ``async`` is a reserved keyword in Python
and cannot be used as a module name.
"""

from __future__ import annotations

import asyncio
import inspect

from typing import Any
from functools import partial
from concurrent.futures import ThreadPoolExecutor

from ..schemas import Job
from .base import BaseWorker
from ..utils import import_fn
from ..utils import wants_context
from ..schemas import Context


class AsyncWorker(BaseWorker):
    """Worker that runs async coroutines natively.

    Synchronous functions are dispatched to a dedicated ``ThreadPoolExecutor``
    sized to ``concurrency`` so they don't block the event loop and can't
    exceed the worker's concurrency limit. For workloads where you need
    strict thread-count control, use ``ThreadWorker`` instead.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialise the worker and allocate a placeholder for the thread pool.

        Args:
            *args: Forwarded to :class:`~wrk.worker.base.BaseWorker`.
            **kwargs: Forwarded to :class:`~wrk.worker.base.BaseWorker`.
        """
        super().__init__(*args, **kwargs)
        self._thread_pool: ThreadPoolExecutor | None = None

    async def _setup_executor(self) -> None:
        self._thread_pool = ThreadPoolExecutor(max_workers=self.concurrency)

    async def _teardown_executor(self) -> None:
        if self._thread_pool:
            self._thread_pool.shutdown(wait=True)

    async def _execute(self, job: Job, ctx: Context) -> Any:
        """Execute a job, dispatching coroutines natively and sync functions via the thread pool.

        Args:
            job: The job to execute.
            ctx: Execution context injected when the handler accepts it.

        Returns:
            The return value of the job handler.

        Raises:
            Exception: Any exception raised by the handler propagates to the caller.
        """
        fn = import_fn(job.function)
        args: list = (job.payload or {}).get("args", [])
        kwargs: dict = (job.payload or {}).get("kwargs", {})
        call_args = [ctx, *args] if wants_context(fn) else args

        if inspect.iscoroutinefunction(fn):
            return await self._with_timeout(fn(*call_args, **kwargs), job.timeout_secs)

        loop = asyncio.get_running_loop()
        fut = loop.run_in_executor(self._thread_pool, partial(fn, *call_args, **kwargs))
        return await self._with_timeout(fut, job.timeout_secs)
