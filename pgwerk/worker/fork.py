from __future__ import annotations

import asyncio
import multiprocessing

from typing import Any

from .base import BaseWorker
from ..utils import import_fn
from ..utils import wants_context
from ..schemas import Job
from ..schemas import Context


def _fork_target(
    dotted: str,
    args: list,
    kwargs: dict,
    result_queue: "multiprocessing.Queue[tuple[str, Any]]",
) -> None:
    """Entry point for the forked process. Puts (ok|err, value) into result_queue."""
    import asyncio as _aio

    try:
        from tests.utils import import_fn

        fn = import_fn(dotted)
        result = _aio.run(fn(*args, **kwargs)) if _aio.iscoroutinefunction(fn) else fn(*args, **kwargs)
        result_queue.put(("ok", result))
    except Exception as exc:
        result_queue.put(("err", str(exc)))


class ForkWorker(BaseWorker):
    """Worker that spawns a fresh subprocess per job for true crash isolation.

    Each job gets its own short-lived process. If the process crashes, the
    parent event loop is unaffected and the job is nacked normally. Timeout
    is enforced with SIGTERM → grace period → SIGKILL.
    """

    def __init__(self, *args: Any, sigterm_grace: int | None = None, **kwargs: Any) -> None:
        """Initialise the worker with an optional SIGTERM grace period.

        Args:
            *args: Forwarded to :class:`~wrk.worker.base.BaseWorker`.
            sigterm_grace: Seconds between SIGTERM and SIGKILL when terminating
                a timed-out subprocess; falls back to ``app.config.sigterm_grace``.
            **kwargs: Forwarded to :class:`~wrk.worker.base.BaseWorker`.
        """
        super().__init__(*args, **kwargs)
        self.sigterm_grace = sigterm_grace if sigterm_grace is not None else self.app.config.sigterm_grace
        self._mp_ctx = multiprocessing.get_context("spawn")

    async def _execute(self, job: Job, ctx: Context) -> Any:  # noqa: ARG002
        """Execute a job in a freshly spawned subprocess.

        Args:
            job: The job to execute.
            ctx: Unused — ``Context`` is not picklable and cannot cross the
                process boundary.

        Returns:
            The return value of the job handler passed back via a result queue.

        Raises:
            RuntimeError: If the handler requests a Context argument, the
                subprocess exits non-zero, or the result queue is empty.
            TimeoutError: If the job exceeds ``timeout_secs``.
        """
        _fn = import_fn(job.function)
        if wants_context(_fn):
            raise RuntimeError(
                f"{job.function} expects a Context argument, but ForkWorker cannot inject it "
                "(Context is not picklable). Use AsyncWorker or ThreadWorker instead."
            )
        args: list = (job.payload or {}).get("args", [])
        kwargs: dict = (job.payload or {}).get("kwargs", {})
        loop = asyncio.get_running_loop()

        result_queue: multiprocessing.Queue = self._mp_ctx.Queue()
        proc = self._mp_ctx.Process(
            target=_fork_target,
            args=(job.function, args, kwargs, result_queue),
            daemon=True,
        )
        proc.start()

        try:
            elapsed = 0.0
            while proc.is_alive():
                if job.timeout_secs and elapsed >= job.timeout_secs:
                    await self._terminate(proc, loop)
                    raise TimeoutError(f"Job {job.id} timed out after {job.timeout_secs}s")
                await asyncio.sleep(0.05)
                elapsed += 0.05
        except asyncio.CancelledError:
            await self._terminate(proc, loop)
            raise

        if proc.exitcode != 0:
            raise RuntimeError(f"Job process exited with code {proc.exitcode} for job {job.id}")

        if result_queue.empty():
            raise RuntimeError(f"Job {job.id} produced no result")

        try:
            kind, value = result_queue.get_nowait()
        finally:
            result_queue.close()
            result_queue.join_thread()

        if kind == "err":
            raise RuntimeError(value)
        return value

    async def _terminate(self, proc: Any, loop: asyncio.AbstractEventLoop) -> None:
        """Terminate a subprocess gracefully, escalating to SIGKILL if needed.

        Sends SIGTERM and waits up to ``sigterm_grace`` seconds. If the process
        is still alive after the grace period, sends SIGKILL.

        Args:
            proc: The ``multiprocessing.Process`` to terminate.
            loop: The running event loop used to join the process off-thread.
        """
        if not proc.is_alive():
            return
        proc.terminate()
        try:
            await asyncio.wait_for(
                loop.run_in_executor(None, proc.join),
                timeout=self.sigterm_grace,
            )
        except asyncio.TimeoutError:
            if proc.is_alive():
                proc.kill()
                await loop.run_in_executor(None, proc.join)
