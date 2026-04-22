from __future__ import annotations

import os
import abc
import json
import uuid
import random
import signal
import socket
import asyncio
import logging
import traceback

from typing import TYPE_CHECKING
from typing import Any
from typing import Callable
from datetime import datetime
from datetime import timezone
from datetime import timedelta

import psycopg

from psycopg.sql import SQL
from psycopg.sql import Identifier

from ..utils import call_hook
from ..utils import invoke_callback
from ..commons import DequeueStrategy
from ..logging import job_id_var
from ..schemas import Job
from ..schemas import Context
from ..serializers import encode


if TYPE_CHECKING:
    from ..app import Werk


_TRANSIENT_DB_ERRORS = (psycopg.OperationalError, psycopg.InterfaceError)

logger = logging.getLogger(__name__)


class BaseWorker(abc.ABC):
    """Abstract base worker. Subclasses implement ``_execute``."""

    def __init__(
        self,
        app: "Werk",
        queues: list[str] | None = None,
        concurrency: int = 10,
        heartbeat_interval: int | None = None,
        poll_interval: float | None = None,
        dequeue_strategy: DequeueStrategy = DequeueStrategy.Priority,
        burst: bool = False,
        before_process: list[Callable] | None = None,
        after_process: list[Callable] | None = None,
        sweep_interval: float | None = None,
        abort_interval: float | None = None,
        shutdown_timeout: float | None = None,
    ) -> None:
        """Initialise the worker with scheduling and concurrency settings.

        Args:
            app: Connected :class:`~wrk.app.Werk` application instance.
            queues: Queue names to consume; defaults to ``["default"]``.
            concurrency: Maximum number of jobs executed simultaneously.
            heartbeat_interval: Seconds between worker heartbeat updates;
                falls back to ``app.config.heartbeat_interval``.
            poll_interval: Seconds between dequeue polls when idle;
                falls back to ``app.config.poll_interval``.
            dequeue_strategy: Order in which queues are polled.
            burst: When ``True``, shut down once all queues are empty.
            before_process: Hooks called before each job is executed.
            after_process: Hooks called after each job finishes (success or failure).
            sweep_interval: Seconds between stuck-job sweep runs;
                falls back to ``app.config.sweep_interval``.
            abort_interval: Seconds between abort-request polls;
                falls back to ``app.config.abort_interval``.
            shutdown_timeout: Seconds to wait for active jobs to drain on shutdown;
                falls back to ``app.config.shutdown_timeout``.
        """
        self.app = app
        self.queues = queues or ["default"]
        self.concurrency = concurrency
        self.heartbeat_interval = (
            heartbeat_interval if heartbeat_interval is not None else app.config.heartbeat_interval
        )
        self.poll_interval = poll_interval if poll_interval is not None else app.config.poll_interval
        self.dequeue_strategy = dequeue_strategy
        self.burst = burst
        self._before_process: list[Callable] = list(before_process or [])
        self._after_process: list[Callable] = list(after_process or [])
        self._sweep_interval = sweep_interval if sweep_interval is not None else app.config.sweep_interval
        self._abort_interval = abort_interval if abort_interval is not None else app.config.abort_interval
        self.shutdown_timeout = shutdown_timeout if shutdown_timeout is not None else app.config.shutdown_timeout

        self.id = uuid.uuid4().hex
        self.name = f"{socket.gethostname()}.{os.getpid()}"
        self._running = False
        self._active: set[asyncio.Task] = set()
        self._active_jobs: dict[str, asyncio.Task] = {}
        self._abort_requested: set[str] = set()
        self._wakeup = asyncio.Event()
        self._exception_handlers: list[Callable] = []
        self._rr_offset = 0

    async def _job_heartbeat_loop(self, job_id: str, heartbeat_secs: int) -> None:
        interval = max(1.0, heartbeat_secs / 2)
        while True:
            try:
                await self.app.touch_job(job_id)
            except Exception as exc:
                logger.warning("Worker %s: job heartbeat error for %s: %s", self.name, job_id, exc)
            await asyncio.sleep(interval)

    # ------------------------------------------------------------------
    # Hook registration
    # ------------------------------------------------------------------

    def add_before_process(self, hook: Callable) -> None:
        """Register a hook to be called before each job is executed.

        Args:
            hook: Sync or async callable that accepts a single
                :class:`~wrk.schemas.Context` argument.
        """
        self._before_process.append(hook)

    def add_after_process(self, hook: Callable) -> None:
        """Register a hook to be called after each job finishes.

        The hook is called regardless of whether the job succeeded or failed.

        Args:
            hook: Sync or async callable that accepts a single
                :class:`~wrk.schemas.Context` argument.
        """
        self._after_process.append(hook)

    # ------------------------------------------------------------------
    # Exception handler stack
    # ------------------------------------------------------------------

    def push_exception_handler(self, handler: Callable) -> None:
        """Push an exception handler onto the handler stack.

        Handlers are called in LIFO order when a job raises an unhandled
        exception. Each handler receives the ``(job, exception)`` pair.

        Args:
            handler: Sync or async callable with signature
                ``(job: Job, exc: Exception) -> None``.
        """
        self._exception_handlers.append(handler)

    def pop_exception_handler(self) -> Callable:
        """Remove and return the most recently pushed exception handler.

        Returns:
            The callable that was on top of the exception handler stack.

        Raises:
            IndexError: If the exception handler stack is empty.
        """
        if not self._exception_handlers:
            raise IndexError("No exception handlers on stack")
        return self._exception_handlers.pop()

    async def _invoke_exception_handlers(self, job: Job, exc: Exception) -> None:
        for handler in reversed(self._exception_handlers):
            try:
                result = handler(job, exc)
                if asyncio.iscoroutine(result):
                    await result
            except Exception as e:
                logger.warning("Exception handler %s raised: %s", handler, e)

    # ------------------------------------------------------------------
    # Entry point
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Start the worker and block until shutdown.

        Connects to the database if not already connected, registers the worker,
        and runs the main polling loop alongside heartbeat, LISTEN/NOTIFY, abort,
        and sweep side-loops. Installs SIGTERM/SIGINT handlers to trigger a
        graceful drain before exit.
        """
        loop = asyncio.get_running_loop()
        self._running = True
        self._wakeup.clear()

        await self._setup_executor()

        try:
            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.add_signal_handler(sig, self._request_shutdown)
        except (NotImplementedError, ValueError):
            logger.info("Worker %s: signal handlers not installed (non-main thread or non-Unix)", self.name)

        await self.app.connect()

        try:
            await self._register()
            side_tasks = [
                asyncio.ensure_future(self._heartbeat_loop()),
                asyncio.ensure_future(self._listen_loop()),
                asyncio.ensure_future(self._abort_loop()),
                asyncio.ensure_future(self._sweep_loop()),
            ]
            try:
                await self._main_loop()
            finally:
                for t in side_tasks:
                    t.cancel()
                await asyncio.gather(*side_tasks, return_exceptions=True)
        finally:
            await self._deregister()
            await self._teardown_executor()

    async def _setup_executor(self) -> None:
        """Override in subclasses that need executor setup."""

    async def _teardown_executor(self) -> None:
        """Override in subclasses that need executor teardown."""

    def _request_shutdown(self) -> None:
        logger.info("Worker %s: shutdown requested", self.name)
        self._running = False
        self._wakeup.set()

    # ------------------------------------------------------------------
    # Registration & heartbeat
    # ------------------------------------------------------------------

    async def _register(self) -> None:
        worker_repo = self.app._worker_repo
        await worker_repo.register(
            self.id,
            self.name,
            self.queues,
            json.dumps(
                {
                    "pid": os.getpid(),
                    "concurrency": self.concurrency,
                    "queues": self.queues,
                    "strategy": self.dequeue_strategy.value,
                    "worker_type": type(self).__name__,
                    "burst": self.burst,
                }
            ),
        )
        logger.info("Worker %s registered (%s)", self.name, self.id)

    async def _deregister(self) -> None:
        if not self.app._connected:
            return
        try:
            worker_repo = self.app._worker_repo
            await worker_repo.deregister(self.id)
        except Exception as exc:
            logger.warning("Worker %s: deregister failed: %s", self.name, exc)

    async def _heartbeat_loop(self) -> None:
        while self._running:
            try:
                worker_repo = self.app._worker_repo
                await worker_repo.update_heartbeat(self.id)
            except Exception as exc:
                logger.warning("Worker %s: heartbeat error: %s", self.name, exc)
            await asyncio.sleep(self.heartbeat_interval)

    # ------------------------------------------------------------------
    # LISTEN / NOTIFY
    # ------------------------------------------------------------------

    async def _listen_loop(self) -> None:
        pool = self.app._pool_or_raise()
        backoff = 1.0
        while self._running:
            try:
                async with pool.connection() as conn:
                    for queue in self.queues:
                        await conn.execute(SQL("LISTEN {ch}").format(ch=Identifier(f"{self.app.prefix}:{queue}")))
                    backoff = 1.0
                    self._wakeup.set()
                    notifies_gen = conn.notifies()
                    try:
                        async for _ in notifies_gen:
                            if not self._running:
                                return
                            self._wakeup.set()
                    finally:
                        try:
                            await notifies_gen.aclose()
                        except Exception:
                            pass
            except Exception as exc:
                if not self._running:
                    return
                logger.warning("Worker %s: listen loop error, reconnecting in %.1fs: %s", self.name, backoff, exc)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)

    # ------------------------------------------------------------------
    # Abort loop
    # ------------------------------------------------------------------

    async def _abort_loop(self) -> None:
        while self._running:
            await asyncio.sleep(self._abort_interval)
            if not self._active_jobs:
                continue
            try:
                worker_repo = self.app._worker_repo
                aborting = await worker_repo.get_aborting(list(self._active_jobs.keys()))
                for job_id in aborting:
                    task = self._active_jobs.get(job_id)
                    if task and not task.done():
                        logger.info("Worker %s: aborting job %s", self.name, job_id)
                        self._abort_requested.add(job_id)
                        task.cancel()
            except Exception as exc:
                logger.warning("Worker %s: abort loop error: %s", self.name, exc)

    # ------------------------------------------------------------------
    # Sweep loop
    # ------------------------------------------------------------------

    async def _sweep_loop(self) -> None:
        while self._running:
            await asyncio.sleep(self._sweep_interval)
            try:
                swept = await self.app.sweep()
                if swept:
                    logger.info("Worker %s: swept %d job(s)", self.name, len(swept))
            except Exception as exc:
                logger.warning("Worker %s: sweep error: %s", self.name, exc)

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    async def _main_loop(self) -> None:
        try:
            while self._running:
                available = self.concurrency - len(self._active)
                if available > 0:
                    jobs = await self._dequeue(limit=available)

                    if self.burst and not jobs and not self._active and not self._wakeup.is_set():
                        logger.info("Worker %s: burst complete, shutting down", self.name)
                        self._running = False
                        return

                    for job in jobs:
                        task = asyncio.create_task(self._handle_job(job))
                        self._active.add(task)
                        task.add_done_callback(self._active.discard)
                        task.add_done_callback(lambda _: self._wakeup.set())
                        self._active_jobs[job.id] = task
                        task.add_done_callback(lambda _, jid=job.id: self._active_jobs.pop(jid, None))  # type: ignore[misc]

                try:
                    jittered = self.poll_interval * (1 + random.uniform(-0.2, 0.2))
                    await asyncio.wait_for(self._wakeup.wait(), timeout=jittered)
                except asyncio.TimeoutError:
                    pass
                self._wakeup.clear()
        except asyncio.CancelledError:
            self._running = False

        if self._active:
            logger.info(
                "Worker %s: draining %d active job(s) (timeout=%ds)",
                self.name,
                len(self._active),
                self.shutdown_timeout,
            )
            try:
                await asyncio.shield(
                    asyncio.wait_for(
                        asyncio.gather(*list(self._active), return_exceptions=True),
                        timeout=self.shutdown_timeout,
                    )
                )
            except (asyncio.TimeoutError, asyncio.CancelledError):
                logger.warning("Worker %s: drain interrupted, cancelling %d job(s)", self.name, len(self._active))
                for task in list(self._active):
                    task.cancel()
                await asyncio.gather(*list(self._active), return_exceptions=True)

    # ------------------------------------------------------------------
    # Dequeue
    # ------------------------------------------------------------------

    def _ordered_queues(self) -> list[str]:
        if self.dequeue_strategy == DequeueStrategy.RoundRobin:
            q = self.queues[self._rr_offset :] + self.queues[: self._rr_offset]
            self._rr_offset = (self._rr_offset + 1) % len(self.queues)
            return q
        if self.dequeue_strategy == DequeueStrategy.Random:
            return random.sample(self.queues, len(self.queues))
        return self.queues

    async def _dequeue(self, limit: int = 1) -> list[Job]:
        ordered = self._ordered_queues()
        try:
            worker_repo = self.app._worker_repo
            jobs = await worker_repo.dequeue(
                worker_id=self.id,
                ordered_queues=ordered,
                limit=limit,
                strategy=self.dequeue_strategy,
            )
        except Exception as exc:
            logger.error("Worker %s: dequeue error: %s", self.name, exc)
            return []

        for job in jobs:
            logger.info(
                "Worker %s: dequeued %s [%s] (attempt %d/%d, queue=%s)",
                self.name,
                job.function,
                job.id,
                job.attempts,
                job.max_attempts,
                job.queue,
            )
        return jobs

    # ------------------------------------------------------------------
    # Job execution lifecycle
    # ------------------------------------------------------------------

    async def _handle_job(self, job: Job) -> None:
        token = job_id_var.set(job.id)
        logger.info(
            "Worker %s: starting %s [%s] (attempt %d/%d)",
            self.name,
            job.function,
            job.id,
            job.attempts,
            job.max_attempts,
        )
        ctx = Context(app=self.app, worker=self, job=job)
        heartbeat_task: asyncio.Task | None = None

        for hook in self._before_process:
            try:
                await call_hook(hook, ctx)
            except Exception:
                logger.exception("Worker %s: before_process hook error", self.name)

        try:
            if job.heartbeat_secs:
                heartbeat_task = asyncio.create_task(self._job_heartbeat_loop(job.id, job.heartbeat_secs))
            try:
                result = await self._execute(job, ctx)
            except asyncio.CancelledError:
                if job.id in self._abort_requested:
                    self._abort_requested.discard(job.id)
                    await self._nack(job, "aborted", aborted=True)
                else:
                    await self._requeue_cancelled(job)
                return
            except Exception as exc:
                ctx.exception = exc
                logger.error(
                    "Worker %s: failed %s [%s] (attempt %d/%d): %s",
                    self.name,
                    job.function,
                    job.id,
                    job.attempts,
                    job.max_attempts,
                    exc,
                    exc_info=True,
                )
                await self._invoke_exception_handlers(job, exc)
                await self._nack(job, traceback.format_exc())
                return

            await self._ack_with_retry(job, result)
            logger.info("Worker %s: completed %s [%s]", self.name, job.function, job.id)
        except asyncio.CancelledError:
            await self._requeue_cancelled(job)
        finally:
            job_id_var.reset(token)
            if heartbeat_task is not None:
                heartbeat_task.cancel()
                await asyncio.gather(heartbeat_task, return_exceptions=True)
            for hook in self._after_process:
                try:
                    await call_hook(hook, ctx)
                except Exception:
                    logger.exception("Worker %s: after_process hook error", self.name)

    @abc.abstractmethod
    async def _execute(self, job: Job, ctx: Context) -> Any:
        """Run the job and return its result. Raise on failure."""

    @staticmethod
    async def _with_timeout(awt: Any, timeout: int | None) -> Any:
        return await (asyncio.wait_for(awt, timeout=timeout) if timeout else awt)

    # ------------------------------------------------------------------
    # Ack / Nack / Requeue
    # ------------------------------------------------------------------

    async def _ack_with_retry(self, job: Job, result: Any, *, max_attempts: int = 5) -> None:
        """Retry _ack on transient DB errors with exponential backoff. Correctness-critical."""
        delay = 1.0
        for attempt in range(1, max_attempts + 1):
            try:
                await self._ack(job, result)
                return
            except _TRANSIENT_DB_ERRORS as exc:
                if attempt == max_attempts:
                    logger.critical(
                        "Worker %s: ack failed after %d attempts for job %s [%s], sweep will recover: %s",
                        self.name,
                        max_attempts,
                        job.function,
                        job.id,
                        exc,
                    )
                    return
                logger.warning(
                    "Worker %s: transient ack error for job %s (attempt %d/%d), retrying in %.1fs: %s",
                    self.name,
                    job.id,
                    attempt,
                    max_attempts,
                    delay,
                    exc,
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30.0)

    async def _ack(self, job: Job, result: Any = None) -> None:
        result_json = encode(self.app.serializer, result)
        expires_at = (
            datetime.now(timezone.utc) + timedelta(seconds=job.result_ttl) if job.result_ttl is not None else None
        )

        worker_repo = self.app._worker_repo
        if not await worker_repo.ack(self.id, job, result_json, expires_at):
            return  # race detected; logged by repo

        if job.repeat_remaining and job.repeat_remaining > 0:
            await self.app._reenqueue_repeat(job)

        if job.on_success:
            await invoke_callback(job.on_success, job, timeout=job.on_success_timeout)

    async def _nack(self, job: Job, error: str, *, aborted: bool = False) -> None:
        worker_repo = self.app._worker_repo
        is_terminal_delete = (not aborted and job.attempts >= job.max_attempts and job.failure_mode == "delete") or (
            aborted and job.failure_mode == "delete"
        )

        if is_terminal_delete:
            await worker_repo.delete_job(job.id)
            logger.info(
                "Worker %s: deleted %s [%s] on terminal failure (failure_mode=delete)", self.name, job.function, job.id
            )
            if aborted and job.on_stopped:
                await invoke_callback(job.on_stopped, job, timeout=job.on_stopped_timeout)
            elif job.on_failure:
                await invoke_callback(job.on_failure, job, timeout=job.on_failure_timeout)
            return

        will_retry = not aborted and job.attempts < job.max_attempts

        scheduled_at: datetime | None = None
        if will_retry and job.retry_intervals:
            idx = min(job.attempts - 1, len(job.retry_intervals) - 1)
            delay = job.retry_intervals[idx]
            if delay:
                scheduled_at = datetime.now(timezone.utc) + timedelta(seconds=delay)

        if aborted:
            new_status = "aborted"
        elif will_retry and scheduled_at:
            new_status = "scheduled"
        elif will_retry:
            new_status = "queued"
        else:
            new_status = "failed"
        is_terminal = new_status in ("failed", "aborted")

        expires_at: datetime | None = None
        if is_terminal and job.failure_ttl is not None:
            expires_at = datetime.now(timezone.utc) + timedelta(seconds=job.failure_ttl)

        if not await worker_repo.nack(self.id, job, error, new_status, scheduled_at, expires_at):
            return  # race detected; logged by repo

        if will_retry:
            if scheduled_at:
                delay_secs = (scheduled_at - datetime.now(timezone.utc)).total_seconds()
                logger.info(
                    "Worker %s: retrying %s [%s] in %.0fs (attempt %d/%d)",
                    self.name,
                    job.function,
                    job.id,
                    delay_secs,
                    job.attempts,
                    job.max_attempts,
                )
            else:
                logger.info(
                    "Worker %s: retrying %s [%s] immediately (attempt %d/%d)",
                    self.name,
                    job.function,
                    job.id,
                    job.attempts,
                    job.max_attempts,
                )
        else:
            logger.warning(
                "Worker %s: giving up on %s [%s] after %d/%d attempts",
                self.name,
                job.function,
                job.id,
                job.attempts,
                job.max_attempts,
            )

        if aborted and job.on_stopped:
            await invoke_callback(job.on_stopped, job, timeout=job.on_stopped_timeout)
        elif is_terminal and not aborted and job.on_failure:
            await invoke_callback(job.on_failure, job, timeout=job.on_failure_timeout)

        if will_retry and not scheduled_at:
            await worker_repo.notify(job.queue)

    async def _requeue_cancelled(self, job: Job) -> None:
        worker_repo = self.app._worker_repo
        await worker_repo.requeue_cancelled(self.id, job)
        logger.info("Worker %s: re-queued cancelled job %s", self.name, job.id)
