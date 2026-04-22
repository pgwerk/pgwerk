from __future__ import annotations

import asyncio
import logging

from typing import Any
from typing import Callable
from datetime import datetime
from datetime import timezone
from datetime import timedelta

from psycopg import AsyncConnection
from psycopg.sql import SQL
from psycopg.sql import Identifier
from psycopg_pool import AsyncConnectionPool

from .repos import JobInsert
from .repos import JobRepository
from .repos import StatsRepository
from .repos import WorkerRepository
from .utils import fn_path
from .utils import normalize_retry
from .utils import normalize_callback
from .utils import normalize_depends_on
from .config import WrkConfig
from .commons import JobStatus
from .logging import configure_logging
from .schemas import Job
from .schemas import Retry
from .schemas import Repeat
from .schemas import Callback
from .schemas import Dependency
from .schemas import JobExecution
from .schemas import EnqueueParams
from .database import DatabaseManager
from .exceptions import JobError
from .serializers import Serializer
from .serializers import encode
from .serializers import get_default


logger = logging.getLogger(__name__)


class Wrk:
    def __init__(
        self,
        dsn: str,
        *,
        config: WrkConfig | dict | None = None,
        schema: str | None = None,
        prefix: str | None = None,
        min_pool_size: int | None = None,
        max_pool_size: int | None = None,
        serializer: Serializer | None = None,
        max_active_secs: int | None = None,
        log_level: int | str | None = None,
        log_format: str | None = None,
        log_color: bool | None = None,
        log_fmt: str | None = None,
    ) -> None:
        """Initialize Wrk with a Postgres DSN and optional configuration.

        Args:
            dsn: Postgres connection string.
            config: A WrkConfig instance or dict; keyword overrides take
                precedence over values in the config object.
            schema: Postgres schema to place wrk tables in.
            prefix: Table-name prefix (default ``_wrk_``).
            min_pool_size: Minimum number of pooled connections.
            max_pool_size: Maximum number of pooled connections.
            serializer: Payload serializer; defaults to JSON.
            max_active_secs: Seconds before an active job is considered stuck
                by :meth:`sweep`.
            log_level: Logging level passed to :func:`configure_logging`.
            log_format: Log output format (``"text"`` or ``"json"``).
            log_color: Enable ANSI colour in text log output.
            log_fmt: Custom log format string.
        """
        if isinstance(config, dict):
            config = WrkConfig(**config)
        self.config: WrkConfig = config or WrkConfig()

        self.dsn = dsn
        self.schema = schema if schema is not None else self.config.schema
        self.prefix = prefix if prefix is not None else self.config.prefix
        self.serializer: Serializer = serializer or get_default()
        self._min_pool_size = min_pool_size if min_pool_size is not None else self.config.min_pool_size
        self._max_pool_size = max_pool_size if max_pool_size is not None else self.config.max_pool_size
        self.max_active_secs = max_active_secs if max_active_secs is not None else self.config.max_active_secs
        self._pool: AsyncConnectionPool | None = None
        self._connected = False
        self._db = DatabaseManager(self.schema, self.prefix, ephemeral_tables=self.config.ephemeral_tables)
        self._before_enqueues: dict[int, Callable] = {}
        self._on_startup: list[Callable] = []
        self._on_shutdown: list[Callable] = []

        self._t = {
            "worker": self._db.table("worker"),
            "jobs": self._db.table("jobs"),
            "worker_jobs": self._db.table("worker_jobs"),
            "executions": self._db.table("jobs_executions"),
            "deps": self._db.table("job_deps"),
        }

        self.__job_repo: JobRepository | None = None
        self.__worker_repo: WorkerRepository | None = None
        self.__stats_repo: StatsRepository | None = None

        if any(v is not None for v in (log_level, log_format, log_color, log_fmt)):
            configure_logging(
                level=log_level or "INFO",
                format=log_format or "text",
                color=log_color,
                fmt=log_fmt,
            )

    @property
    def _job_repo(self) -> JobRepository:
        if self.__job_repo is None:
            raise RuntimeError("Not connected. Await app.connect() or use `async with app`.")
        return self.__job_repo

    @property
    def _worker_repo(self) -> WorkerRepository:
        if self.__worker_repo is None:
            raise RuntimeError("Not connected. Await app.connect() or use `async with app`.")
        return self.__worker_repo

    @property
    def _stats_repo(self) -> StatsRepository:
        if self.__stats_repo is None:
            raise RuntimeError("Not connected. Await app.connect() or use `async with app`.")
        return self.__stats_repo

    # ------------------------------------------------------------------
    # Lifecycle hooks
    # ------------------------------------------------------------------

    def on_startup(self, func: Callable) -> Callable:
        """Register a callable to run after the pool is open.

        Can be used as a decorator.

        Args:
            func: Sync or async callable with no arguments.

        Returns:
            The original callable, unchanged.
        """
        self._on_startup.append(func)
        return func

    def on_shutdown(self, func: Callable) -> Callable:
        """Register a callable to run before the pool is closed.

        Can be used as a decorator.

        Args:
            func: Sync or async callable with no arguments.

        Returns:
            The original callable, unchanged.
        """
        self._on_shutdown.append(func)
        return func

    async def _run_hooks(self, hooks: list[Callable]) -> None:
        """Execute a list of hooks in order, awaiting any coroutines.

        Args:
            hooks: Callables to invoke sequentially.
        """
        for hook in hooks:
            result = hook()
            if asyncio.iscoroutine(result):
                await result

    # ------------------------------------------------------------------
    # Before-enqueue hooks
    # ------------------------------------------------------------------

    def register_before_enqueue(self, callback: Callable) -> None:
        """Register a callback invoked with each Job before it is inserted.

        Args:
            callback: Sync or async callable that receives a :class:`Job`.
        """
        self._before_enqueues[id(callback)] = callback

    def unregister_before_enqueue(self, callback: Callable) -> None:
        """Remove a previously registered before-enqueue callback.

        Args:
            callback: The callable originally passed to
                :meth:`register_before_enqueue`. No-op if not registered.
        """
        self._before_enqueues.pop(id(callback), None)

    async def _run_before_enqueue(self, job: Job) -> None:
        """Invoke all registered before-enqueue callbacks for *job*.

        Args:
            job: The Job that was just inserted.
        """
        for cb in self._before_enqueues.values():
            result = cb(job)
            if asyncio.iscoroutine(result):
                await result

    # ------------------------------------------------------------------
    # Enqueue
    # ------------------------------------------------------------------

    async def enqueue(
        self,
        func: Callable | str,
        *args: Any,
        _queue: str = "default",
        _priority: int = 0,
        _delay: int | None = None,
        _at: datetime | None = None,
        _retry: Retry | int = 1,
        _timeout: int | None = None,
        _heartbeat: int | None = None,
        _key: str | None = None,
        _group: str | None = None,
        _conn: AsyncConnection | None = None,
        _meta: dict[str, Any] | None = None,
        _result_ttl: int | None = None,
        _failure_ttl: int | None = None,
        _ttl: int | None = None,
        _on_success: Callback | Callable | str | None = None,
        _on_failure: Callback | Callable | str | None = None,
        _on_stopped: Callback | Callable | str | None = None,
        _repeat: Repeat | None = None,
        _depends_on: list[Dependency | str | Job] | Dependency | str | Job | None = None,
        _cron_name: str | None = None,
        _failure_mode: str = "hold",
        **kwargs: Any,
    ) -> Job | None:
        """Enqueue *func* for async execution.

        All keyword arguments prefixed with ``_`` are routing/control options;
        the rest are forwarded as the job payload.

        Args:
            func: The callable to execute.
            *args: Positional arguments forwarded to *func*.
            _queue: Queue name (default ``"default"``).
            _priority: Higher values run first within the same queue.
            _delay: Seconds from now before the job becomes eligible.
            _at: Absolute UTC datetime at which the job becomes eligible.
            _retry: Maximum attempt count or a :class:`Retry` instance with
                custom back-off intervals.
            _timeout: Seconds before a running job is considered timed out.
            _heartbeat: Heartbeat interval in seconds for long-running jobs.
            _key: Idempotency key; duplicate keys are silently dropped.
            _group: Concurrency group name.
            _conn: Existing psycopg connection; enqueues inside that
                transaction.
            _meta: Arbitrary metadata dict stored alongside the job.
            _result_ttl: Seconds to retain a completed job's result row.
            _failure_ttl: Seconds to retain a failed job's row.
            _ttl: Seconds until an unstarted job expires.
            _on_success: Callback invoked on successful completion.
            _on_failure: Callback invoked on failure.
            _on_stopped: Callback invoked when the job is stopped/cancelled.
            _repeat: Repeat policy for recurring jobs.
            _depends_on: Job(s) that must complete before this one runs.
            _cron_name: Cron schedule name this job was created from.
            _failure_mode: What to do with dependents when this job fails
                (``"hold"`` or ``"cancel"``).
            **kwargs: Keyword arguments forwarded to *func*.

        Returns:
            The inserted :class:`Job`, or ``None`` if a duplicate key was
            detected.
        """
        payload: dict[str, Any] | None = {"args": list(args), "kwargs": kwargs} if (args or kwargs) else None

        scheduled_at: datetime | None = None
        if _at is not None:
            scheduled_at = _at
        elif _delay is not None:
            scheduled_at = datetime.now(timezone.utc) + timedelta(seconds=_delay)

        max_attempts, retry_intervals = normalize_retry(_retry)
        on_success_path, on_success_timeout = normalize_callback(_on_success)
        on_failure_path, on_failure_timeout = normalize_callback(_on_failure)
        on_stopped_path, on_stopped_timeout = normalize_callback(_on_stopped)
        dep_ids = normalize_depends_on(_depends_on)

        if dep_ids:
            status = JobStatus.Waiting.value
        elif scheduled_at is not None:
            status = JobStatus.Scheduled.value
        else:
            status = JobStatus.Queued.value

        expires_at: datetime | None = None
        if _ttl is not None and not dep_ids:
            expires_at = datetime.now(timezone.utc) + timedelta(seconds=_ttl)

        repeat_remaining: int | None = _repeat.times if _repeat is not None else None
        repeat_interval_secs: int | None = _repeat.interval if _repeat is not None and not _repeat.intervals else None
        repeat_intervals: list[int] | None = _repeat.intervals if _repeat is not None else None

        data = JobInsert(
            key=_key,
            function=fn_path(func) if callable(func) else func,
            queue=_queue,
            status=status,
            priority=_priority,
            group_key=_group,
            payload=encode(self.serializer, payload),
            max_attempts=max_attempts,
            timeout_secs=_timeout,
            heartbeat_secs=_heartbeat,
            scheduled_at=scheduled_at,
            expires_at=expires_at,
            meta=encode(self.serializer, _meta),
            result_ttl=_result_ttl,
            failure_ttl=_failure_ttl,
            failure_mode=_failure_mode,
            ttl=_ttl,
            on_success=on_success_path,
            on_failure=on_failure_path,
            on_stopped=on_stopped_path,
            on_success_timeout=on_success_timeout,
            on_failure_timeout=on_failure_timeout,
            on_stopped_timeout=on_stopped_timeout,
            retry_intervals=encode(self.serializer, retry_intervals),
            repeat_remaining=repeat_remaining,
            repeat_interval_secs=repeat_interval_secs,
            repeat_intervals=encode(self.serializer, repeat_intervals),
            cron_name=_cron_name,
            dep_ids=dep_ids,
        )

        job = await self._job_repo.insert(data, conn=_conn)

        if job is not None:
            await self._run_before_enqueue(job)

        return job

    async def enqueue_many(
        self, specs: list[EnqueueParams], *, _conn: AsyncConnection | None = None
    ) -> list[Job | None]:
        """Enqueue multiple jobs in a single batch insert.

        Args:
            specs: List of :class:`EnqueueSpec` objects describing each job.
            _conn: Existing psycopg connection; enqueues inside that
                transaction.

        Returns:
            List of inserted :class:`Job` objects in the same order as
            *specs*. An entry is ``None`` if its idempotency key was a
            duplicate.
        """
        inserts: list[JobInsert] = []

        for spec in specs:
            payload: dict[str, Any] | None = (
                {"args": list(spec.args), "kwargs": spec.kwargs} if (spec.args or spec.kwargs) else None
            )

            scheduled_at: datetime | None = None
            if spec.at is not None:
                scheduled_at = spec.at
            elif spec.delay is not None:
                scheduled_at = datetime.now(timezone.utc) + timedelta(seconds=spec.delay)

            max_attempts, retry_intervals = normalize_retry(spec.retry)
            on_success_path, on_success_timeout = normalize_callback(spec.on_success)
            on_failure_path, on_failure_timeout = normalize_callback(spec.on_failure)
            on_stopped_path, on_stopped_timeout = normalize_callback(spec.on_stopped)
            dep_ids = normalize_depends_on(spec.depends_on)

            if dep_ids:
                status = JobStatus.Waiting.value
            elif scheduled_at is not None:
                status = JobStatus.Scheduled.value
            else:
                status = JobStatus.Queued.value

            expires_at: datetime | None = None
            if spec.ttl is not None and not dep_ids:
                expires_at = datetime.now(timezone.utc) + timedelta(seconds=spec.ttl)

            inserts.append(
                JobInsert(
                    key=spec.key,
                    function=fn_path(spec.func),
                    queue=spec.queue,
                    status=status,
                    priority=spec.priority,
                    group_key=spec.group,
                    payload=encode(self.serializer, payload),
                    max_attempts=max_attempts,
                    timeout_secs=spec.timeout,
                    heartbeat_secs=spec.heartbeat,
                    scheduled_at=scheduled_at,
                    expires_at=expires_at,
                    meta=encode(self.serializer, spec.meta),
                    result_ttl=spec.result_ttl,
                    failure_ttl=spec.failure_ttl,
                    failure_mode=spec.failure_mode,
                    ttl=spec.ttl,
                    on_success=on_success_path,
                    on_failure=on_failure_path,
                    on_stopped=on_stopped_path,
                    on_success_timeout=on_success_timeout,
                    on_failure_timeout=on_failure_timeout,
                    on_stopped_timeout=on_stopped_timeout,
                    retry_intervals=encode(self.serializer, retry_intervals),
                    repeat_remaining=spec.repeat.times if spec.repeat else None,
                    repeat_interval_secs=spec.repeat.interval if spec.repeat and not spec.repeat.intervals else None,
                    repeat_intervals=encode(self.serializer, spec.repeat.intervals if spec.repeat else None),
                    cron_name=spec.cron_name,
                    dep_ids=dep_ids,
                )
            )

        results = await self._job_repo.insert_many(inserts, conn=_conn)

        for job in results:
            if job is not None:
                await self._run_before_enqueue(job)

        return results

    async def wait_for(self, job_id: str, *, timeout: float | None = None, poll_interval: float = 2.0) -> Job:
        """Block until a job reaches a terminal state.

        Uses ``LISTEN/NOTIFY`` for instant wake-up and falls back to polling
        when the notification channel drops.

        Args:
            job_id: ID of the job to wait for.
            timeout: Maximum seconds to wait before raising
                :class:`asyncio.TimeoutError`.
            poll_interval: Fallback poll cadence in seconds.

        Returns:
            The refreshed :class:`Job` in its terminal state.

        Raises:
            asyncio.TimeoutError: If *timeout* elapses before the job
                finishes.
        """
        from .commons import JobStatus

        terminal = {JobStatus.Complete, JobStatus.Failed, JobStatus.Aborted}
        pool = self._pool_or_raise()

        job = await self.get_job(job_id)
        if job.status in terminal:
            return job

        channel = f"{self.prefix}:{job.queue}"
        wakeup = asyncio.Event()

        async def _listener() -> None:
            backoff = 1.0
            while True:
                try:
                    async with pool.connection() as conn:
                        await conn.execute(SQL("LISTEN {ch}").format(ch=Identifier(channel)))
                        backoff = 1.0
                        async for _ in conn.notifies():
                            wakeup.set()
                except Exception:
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 10.0)

        listener = asyncio.create_task(_listener())
        start = asyncio.get_event_loop().time()
        try:
            while True:
                elapsed = asyncio.get_event_loop().time() - start
                if timeout is not None and elapsed >= timeout:
                    raise asyncio.TimeoutError(f"job {job_id} did not complete within {timeout}s")

                remaining = (timeout - elapsed) if timeout is not None else None
                wait_secs = min(poll_interval, remaining) if remaining is not None else poll_interval

                try:
                    await asyncio.wait_for(wakeup.wait(), timeout=wait_secs)
                except asyncio.TimeoutError:
                    pass
                wakeup.clear()

                job = await self.get_job(job_id)
                if job.status in terminal:
                    return job
        finally:
            listener.cancel()
            await asyncio.gather(listener, return_exceptions=True)

    async def requeue_job(self, job_id: str) -> bool:
        """Reset a completed or failed job back to the queued state.

        Args:
            job_id: ID of the job to requeue.

        Returns:
            ``True`` if the job was found and requeued, ``False`` otherwise.
        """
        return await self._job_repo.requeue(job_id)

    async def apply(
        self,
        func: Callable,
        *args: Any,
        timeout: float | None = None,
        poll_interval: float = 0.5,
        **enqueue_kwargs: Any,
    ) -> Any:
        """Enqueue *func* and block until its result is available.

        Args:
            func: The callable to execute remotely.
            *args: Positional arguments forwarded to *func*.
            timeout: Maximum seconds to wait; ``None`` waits forever.
            poll_interval: Seconds between result polls.
            **enqueue_kwargs: Additional keyword arguments forwarded to
                :meth:`enqueue`.

        Returns:
            The value returned by *func* on success.

        Raises:
            RuntimeError: If the job could not be enqueued (duplicate key).
            JobError: If the job failed or was aborted.
            asyncio.TimeoutError: If *timeout* elapses.
        """
        job = await self.enqueue(func, *args, **enqueue_kwargs)
        if job is None:
            raise RuntimeError("Job could not be enqueued (duplicate key)")
        return await self._wait_for_job(job.id, timeout=timeout, poll_interval=poll_interval)

    async def map(
        self,
        func: Callable,
        iter_kwargs: list[dict[str, Any]],
        *,
        timeout: float | None = None,
        poll_interval: float = 0.5,
        return_exceptions: bool = False,
        **shared_enqueue_kwargs: Any,
    ) -> list[Any]:
        """Enqueue *func* once per item in *iter_kwargs* and collect all results.

        Args:
            func: The callable to execute for each item.
            iter_kwargs: List of keyword-argument dicts; one job is created
                per entry.
            timeout: Maximum seconds to wait for all jobs; ``None`` waits
                forever.
            poll_interval: Seconds between polls while jobs are running.
            return_exceptions: If ``True``, failed jobs produce a
                :class:`JobError` in the results list instead of raising.
            **shared_enqueue_kwargs: Extra keyword arguments forwarded to
                :meth:`enqueue` for every job.

        Returns:
            List of results in the same order as *iter_kwargs*. Entries for
            duplicate-key jobs that were not inserted are ``None``.

        Raises:
            JobError: If any job fails and *return_exceptions* is ``False``.
            asyncio.TimeoutError: If *timeout* elapses.
        """
        jobs: list[Job | None] = await asyncio.gather(
            *[self.enqueue(func, **{**shared_enqueue_kwargs, **kw}) for kw in iter_kwargs]
        )

        indexed = [(i, job) for i, job in enumerate(jobs) if job is not None]
        terminal = {JobStatus.Complete, JobStatus.Failed, JobStatus.Aborted}
        _sentinel = object()
        results: list[Any] = [_sentinel] * len(jobs)

        for i, job in enumerate(jobs):
            if job is None:
                results[i] = None

        async def _poll_all() -> list[Any]:
            pending = list(indexed)
            while pending:
                still_pending = []
                for i, job in pending:
                    refreshed = await self.get_job(job.id)
                    if refreshed.status in terminal:
                        if refreshed.status == JobStatus.Complete:
                            results[i] = refreshed.result
                        else:
                            exc = JobError(refreshed)
                            if not return_exceptions:
                                raise exc
                            results[i] = exc
                    else:
                        still_pending.append((i, job))
                pending = still_pending
                if pending:
                    await asyncio.sleep(poll_interval)
            return results

        if timeout is not None:
            return await asyncio.wait_for(_poll_all(), timeout=timeout)
        return await _poll_all()

    async def _wait_for_job(
        self,
        job_id: str,
        *,
        timeout: float | None = None,
        poll_interval: float = 0.5,
    ) -> Any:
        """Poll until *job_id* reaches a terminal state and return its result.

        Args:
            job_id: ID of the job to wait for.
            timeout: Maximum seconds to wait; ``None`` waits forever.
            poll_interval: Seconds between database polls.

        Returns:
            The value returned by the job handler.

        Raises:
            JobError: If the job failed or was aborted.
            asyncio.TimeoutError: If *timeout* elapses.
        """
        terminal = {JobStatus.Complete, JobStatus.Failed, JobStatus.Aborted}

        async def _poll() -> Any:
            while True:
                job = await self.get_job(job_id)
                if job.status in terminal:
                    if job.status == JobStatus.Complete:
                        return job.result
                    raise JobError(job)
                await asyncio.sleep(poll_interval)

        if timeout is not None:
            return await asyncio.wait_for(_poll(), timeout=timeout)
        return await _poll()

    async def _reenqueue_repeat(self, job: Job) -> None:
        """Re-enqueue a repeating job after it completes one iteration.

        Args:
            job: The completed :class:`Job` that has a repeat policy.
        """
        await self._job_repo.reenqueue_repeat(job)

    # ------------------------------------------------------------------
    # Jobs
    # ------------------------------------------------------------------

    async def get_job(self, job_id: str) -> Job:
        """Fetch a single job by its ID.

        Args:
            job_id: UUID of the job to retrieve.

        Returns:
            The matching :class:`Job`.

        Raises:
            JobNotFoundError: If no job with that ID exists.
        """
        return await self._job_repo.get(job_id)

    async def list_jobs(
        self,
        queue: str | None = None,
        status: str | None = None,
        worker_id: str | None = None,
        search: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[Job]:
        """List jobs with optional filters.

        Args:
            queue: Restrict to this queue name.
            status: Restrict to this status string (e.g. ``"queued"``).
            worker_id: Restrict to jobs claimed by this worker.
            search: Full-text search term matched against job function/key.
            limit: Maximum number of rows to return.
            offset: Number of rows to skip for pagination.

        Returns:
            List of matching :class:`Job` objects.
        """
        return await self._job_repo.list(
            queue=queue, status=status, worker_id=worker_id, search=search, limit=limit, offset=offset
        )

    async def cancel_job(self, job_id: str) -> bool:
        """Cancel a queued or scheduled job.

        Args:
            job_id: ID of the job to cancel.

        Returns:
            ``True`` if the job was found and cancelled, ``False`` otherwise.
        """
        return await self._job_repo.cancel(job_id)

    async def get_executions(self, job_id: str) -> list[JobExecution]:
        """Fetch the per-attempt execution history for a job.

        Args:
            job_id: ID of the job to inspect.

        Returns:
            List of :class:`JobExecution` records, oldest first.
        """
        return await self._job_repo.get_executions(job_id)

    async def get_job_dependencies(self, job_id: str) -> list[str]:
        """Return the IDs of jobs that must complete before *job_id* can run.

        Args:
            job_id: ID of the dependent job.

        Returns:
            List of upstream job IDs.
        """
        return await self._job_repo.get_dependencies(job_id)

    async def abort_job(self, job_id: str) -> bool:
        """Request an active job to stop and mark it as aborted.

        Args:
            job_id: ID of the running job to abort.

        Returns:
            ``True`` if the abort signal was recorded, ``False`` if the job
            was not found or not in an abortable state.
        """
        return await self._job_repo.abort(job_id)

    async def touch_job(self, job_id: str) -> None:
        """Extend the heartbeat deadline for a long-running job.

        Args:
            job_id: ID of the active job to touch.
        """
        await self._job_repo.touch(job_id)

    async def sweep(self) -> list[str]:
        """Detect stuck active jobs and requeue them.

        A job is considered stuck when it has not updated its heartbeat within
        ``max_active_secs`` seconds.

        Returns:
            List of job IDs that were requeued.
        """
        swept = await self._job_repo.sweep(self.max_active_secs)
        if swept:
            logger.info("wrk: sweep removed %d stuck job(s): %s", len(swept), swept)
        return swept

    async def delete_job(self, job_id: str) -> None:
        """Permanently delete a single job row by ID.

        Args:
            job_id: UUID of the job to delete.
        """
        await self._job_repo.delete(job_id)

    async def bulk_requeue_jobs(self, queue: str | None = None, function_name: str | None = None) -> int:
        """Requeue all matching failed/aborted jobs back to ``queued`` status.

        Args:
            queue: Limit requeue to this queue; ``None`` targets all queues.
            function_name: Limit requeue to jobs with this function name.

        Returns:
            Number of jobs requeued.
        """
        return await self._job_repo.bulk_requeue(queue=queue, function_name=function_name)

    async def bulk_cancel_jobs(self, queue: str | None = None) -> int:
        """Cancel all pending jobs, optionally filtered by queue.

        Args:
            queue: Limit cancellation to this queue; ``None`` targets all queues.

        Returns:
            Number of jobs cancelled.
        """
        return await self._job_repo.bulk_cancel(queue=queue)

    async def purge_jobs(self, statuses: list[str], older_than_days: int) -> int:
        """Permanently delete terminal jobs matching the given criteria.

        Args:
            statuses: Job status values to delete (e.g. ``["failed", "complete"]``).
            older_than_days: Only delete jobs completed more than this many days ago.

        Returns:
            Number of job rows deleted.
        """
        return await self._job_repo.purge(statuses=statuses, older_than_days=older_than_days)

    # ------------------------------------------------------------------
    # Cron
    # ------------------------------------------------------------------

    async def list_cron_stats(self) -> list[dict[str, Any]]:
        """Return per-cron-job statistics from the database.

        Returns:
            List of dicts with keys ``name``, ``last_enqueued_at``, and
            ``total_enqueued``.
        """
        return await self._job_repo.list_cron_stats()

    async def trigger_cron_job(self, name: str) -> Job | None:
        """Immediately enqueue a cron job, bypassing its schedule.

        Args:
            name: The registered cron-job name (``module.qualname`` by default).

        Returns:
            The newly created Job, or ``None`` if deduplication prevented enqueue.
        """
        return await self._job_repo.trigger_cron(name)

    async def reschedule_stuck(self) -> int:
        """Move scheduled jobs whose ``scheduled_at`` has passed back to ``queued``.

        Returns:
            Number of jobs rescheduled.
        """
        return await self._job_repo.reschedule_stuck()

    # ------------------------------------------------------------------
    # Workers
    # ------------------------------------------------------------------

    async def list_workers(self) -> list[dict[str, Any]]:
        """Return all registered worker rows.

        Returns:
            List of worker dicts with id, name, status, queues, and heartbeat info.
        """
        return await self._worker_repo.list()

    async def get_worker(self, worker_id: str) -> dict[str, Any] | None:
        """Return a single worker row by ID.

        Args:
            worker_id: UUID of the worker to look up.

        Returns:
            Worker dict, or ``None`` if not found.
        """
        return await self._worker_repo.get(worker_id)

    async def list_worker_jobs(self, worker_id: str, limit: int = 50, offset: int = 0) -> list[Job]:
        """Return jobs currently claimed by a worker.

        Args:
            worker_id: UUID of the worker.
            limit: Maximum number of jobs to return.
            offset: Number of jobs to skip for pagination.

        Returns:
            List of Job objects claimed by the worker.
        """
        return await self._worker_repo.list_jobs(worker_id=worker_id, limit=limit, offset=offset)

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------

    async def get_queue_stats(self) -> tuple[list[dict[str, Any]], int, int]:
        """Return per-queue job counts and aggregate totals.

        Returns:
            A tuple of ``(queue_rows, total_jobs, active_workers)`` where
            ``queue_rows`` is a list of dicts keyed by queue name and status counts.
        """
        return await self._stats_repo.get_queue_stats()

    async def get_throughput_history(self, minutes: int) -> list[dict[str, Any]]:
        """Return per-minute job throughput over a sliding window.

        Args:
            minutes: How many minutes of history to return.

        Returns:
            List of dicts with ``minute`` (timestamp) and ``completed`` count.
        """
        return await self._stats_repo.get_throughput_history(minutes)

    async def get_queue_depth_history(self, minutes: int) -> list[dict[str, Any]]:
        """Return per-minute queue depth snapshots over a sliding window.

        Args:
            minutes: How many minutes of history to return.

        Returns:
            List of dicts with ``minute`` (timestamp) and ``depth`` count.
        """
        return await self._stats_repo.get_queue_depth_history(minutes)

    async def get_server_info(self) -> tuple[str, int, list[dict[str, Any]]]:
        """Return Postgres version, connection pool size, and table metadata.

        Returns:
            A tuple of ``(pg_version, pool_size, table_rows)`` where
            ``table_rows`` contains size and row-count info for each wrk table.
        """
        return await self._stats_repo.get_server_info(self.prefix)

    # ------------------------------------------------------------------
    # Maintenance
    # ------------------------------------------------------------------

    async def vacuum(self) -> None:
        """Run ``VACUUM ANALYZE`` on all wrk tables outside of a transaction."""
        pool = self._pool_or_raise()
        conn = await pool.getconn()
        try:
            await conn.set_autocommit(True)
            for table in self._t.values():
                await conn.execute(SQL("VACUUM ANALYZE {t}").format(t=table))
        finally:
            await pool.putconn(conn)

    async def truncate(self) -> None:
        """Truncate all wrk tables and restart their identity sequences.

        Warning:
            This is a destructive, irreversible operation. All jobs, workers,
            executions, and dependency records will be permanently deleted.
        """
        pool = self._pool_or_raise()
        async with pool.connection() as conn:
            await conn.execute(
                SQL("""
                    TRUNCATE {jobs}, {executions}, {worker}, {worker_jobs}, {deps}
                    RESTART IDENTITY CASCADE
                """).format(
                    jobs=self._t["jobs"],
                    executions=self._t["executions"],
                    worker=self._t["worker"],
                    worker_jobs=self._t["worker_jobs"],
                    deps=self._t["deps"],
                )
            )

    # ------------------------------------------------------------------
    # Run worker (convenience)
    # ------------------------------------------------------------------

    async def run(
        self,
        queues: list[str] | None = None,
        concurrency: int = 10,
        worker_type: str = "async",
    ) -> None:
        """Connect and run a worker until the process is interrupted.

        Args:
            queues: Queue names to consume; defaults to ``["default"]``.
            concurrency: Maximum number of jobs processed simultaneously.
            worker_type: One of ``"async"``, ``"thread"``, ``"process"``, or
                ``"fork"``.
        """
        from .worker import ForkWorker
        from .worker import AsyncWorker
        from .worker import ThreadWorker
        from .worker import ProcessWorker

        _types = {
            "async": AsyncWorker,
            "thread": ThreadWorker,
            "process": ProcessWorker,
            "fork": ForkWorker,
        }
        cls = _types.get(worker_type, AsyncWorker)

        await self.connect()
        w = cls(app=self, queues=queues or ["default"], concurrency=concurrency)
        await w.run()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """Open the connection pool and run schema migrations.

        Idempotent — safe to call multiple times; subsequent calls are no-ops.
        After returning, the instance is ready to enqueue and inspect jobs.
        """
        if self._connected:
            return
        pool = AsyncConnectionPool(
            self.dsn,
            min_size=self._min_pool_size,
            max_size=self._max_pool_size,
            open=False,
            kwargs={"autocommit": True},
        )
        await pool.open()
        self._pool = pool  # type: ignore[assignment]

        async with pool.connection() as conn:
            await self._db.migrate(conn)
            if self.config.ephemeral_tables:
                await self._db.alter_ephemeral_tables(conn)

        get_serializer = lambda: self.serializer  # noqa: E731
        self.__job_repo = JobRepository(pool, self._t, self.prefix, get_serializer)  # type: ignore[arg-type]
        self.__worker_repo = WorkerRepository(pool, self._t, self.prefix, get_serializer, self.__job_repo)  # type: ignore[arg-type]
        self.__stats_repo = StatsRepository(pool, self._t)  # type: ignore[arg-type]

        self._connected = True
        logger.info("wrk: connected")
        await self._run_hooks(self._on_startup)

    async def disconnect(self) -> None:
        """Run shutdown hooks and close the connection pool.

        Idempotent — safe to call when already disconnected.
        """
        if not self._connected or not self._pool:
            return
        await self._run_hooks(self._on_shutdown)
        await self._pool.close()
        self._connected = False
        self.__job_repo = None
        self.__worker_repo = None
        self.__stats_repo = None

    async def __aenter__(self) -> Wrk:
        """Connect on entering the async context manager.

        Returns:
            This :class:`Wrk` instance.
        """
        await self.connect()
        return self

    async def __aexit__(self, *_: Any) -> None:
        """Disconnect on exiting the async context manager."""
        await self.disconnect()

    def _pool_or_raise(self) -> AsyncConnectionPool:
        """Return the connection pool or raise if not connected.

        Returns:
            The active :class:`AsyncConnectionPool`.

        Raises:
            RuntimeError: If :meth:`connect` has not been called yet.
        """
        if not self._connected or self._pool is None:
            raise RuntimeError("Not connected. Await app.connect() or use `async with app`.")
        return self._pool
