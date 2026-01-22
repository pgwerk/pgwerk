from __future__ import annotations

import asyncio

from typing import Any
from functools import partial
from concurrent.futures import ThreadPoolExecutor

from ..schemas import Job
from .base import BaseWorker
from ..utils import import_fn
from ..utils import wants_context
from ..schemas import Context


class ThreadWorker(BaseWorker):
    """Worker that runs all functions in a dedicated ``ThreadPoolExecutor``.

    Unlike ``AsyncWorker``, coroutines are also run inside the thread pool
    (each in its own ``asyncio.run`` call). This is useful when you need
    strict thread-count control or want to isolate coroutines from the main
    event loop.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._thread_pool: ThreadPoolExecutor | None = None

    async def _setup_executor(self) -> None:
        self._thread_pool = ThreadPoolExecutor(max_workers=self.concurrency)

    async def _teardown_executor(self) -> None:
        if self._thread_pool:
            self._thread_pool.shutdown(wait=True)

    async def _execute(self, job: Job, ctx: Context) -> Any:
        import inspect

        args: list = (job.payload or {}).get("args", [])
        kwargs: dict = (job.payload or {}).get("kwargs", {})

        fn = import_fn(job.function)

        call_args = [ctx, *args] if wants_context(fn) else args

        if inspect.iscoroutinefunction(fn):

            def call() -> Any:
                return asyncio.run(fn(*call_args, **kwargs))
        else:
            call = partial(fn, *call_args, **kwargs)

        loop = asyncio.get_running_loop()
        fut = loop.run_in_executor(self._thread_pool, call)
        return await self._with_timeout(fut, job.timeout_secs)
