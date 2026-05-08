from __future__ import annotations

import os
import json
import uuid
import socket
import asyncio
import logging

from typing import TYPE_CHECKING
from typing import Any
from typing import Callable

from pgwerk.schemas import CronJob

from . import utils


if TYPE_CHECKING:
    from .app import Werk


logger = logging.getLogger(__name__)


class CronScheduler:
    """Async cron scheduler with distributed locking via PostgreSQL advisory locks.

    When multiple instances start (e.g. several worker processes), only one
    becomes the *primary* scheduler. The others run in *standby* mode and
    automatically promote if the primary's connection drops.

    Jobs can be added, removed, paused, and resumed at any time — even while
    the scheduler is running.

    Example:
        scheduler = CronScheduler(app)
        scheduler.register(send_report, queue="reports", cron="0 9 * * *")
        scheduler.register(cleanup, queue="default", interval=300)

        async with app:
            await asyncio.gather(worker.run(), scheduler.run())

    Attributes:
        id: Unique hex identifier for this scheduler instance.
        name: Human-readable name derived from hostname and PID.
    """

    def __init__(self, app: "Werk") -> None:
        self.app = app
        self._jobs: dict[str, CronJob] = {}
        self._running = False
        self._lock_key = utils.advisory_key(f"{app.prefix}:cron_scheduler")
        self.id = uuid.uuid4().hex
        self.name = f"{socket.gethostname()}.{os.getpid()}"
        self._heartbeat_task: asyncio.Task | None = None

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def register(
        self,
        func_or_cronjob: "Callable | CronJob",
        queue: str = "default",
        *,
        name: str | None = None,  # defaults to module.qualname of the function
        args: tuple | None = None,
        kwargs: dict | None = None,
        interval: int | None = None,
        cron: str | None = None,
        timeout: int | None = None,
        result_ttl: int | None = None,
        failure_ttl: int | None = None,
        meta: dict[str, Any] | None = None,
    ) -> CronJob:
        """Register a function (or a CronJob instance) to run on a schedule.

        If *name* is omitted it defaults to ``module.qualname`` of the function.
        Registering under a name that is already in use raises ``ValueError``.

        Args:
            func_or_cronjob: The function to schedule, or a pre-built
                :class:`~wrk.schemas.CronJob` instance.
            queue: Queue to enqueue the job into.
            name: Override for the job name; defaults to ``module.qualname``.
            args: Positional arguments forwarded to the function on each run.
            kwargs: Keyword arguments forwarded to the function on each run.
            interval: Seconds between runs (mutually exclusive with ``cron``).
            cron: Cron expression (e.g. ``"0 9 * * *"``); requires ``croniter``.
            timeout: Per-run timeout in seconds.
            result_ttl: Seconds to retain successful job rows.
            failure_ttl: Seconds to retain failed job rows.
            meta: Arbitrary metadata attached to each enqueued job.

        Returns:
            The registered :class:`~wrk.schemas.CronJob` instance.

        Raises:
            ValueError: If both ``interval`` and ``cron`` are set, or neither, or
                if a job with the same name is already registered.
        """
        if isinstance(func_or_cronjob, CronJob):
            key = func_or_cronjob.name or name
            if key is None:
                key = f"{func_or_cronjob.func.__module__}.{func_or_cronjob.func.__qualname__}"
                func_or_cronjob.name = key
            if key in self._jobs:
                raise ValueError(f"CronJob {key!r} is already registered; call unregister() first to replace it")
            self._jobs[key] = func_or_cronjob
            return func_or_cronjob

        func = func_or_cronjob
        cjob = CronJob(
            func=func,
            queue=queue,
            args=args or (),
            kwargs=kwargs or {},
            interval=interval,
            cron=cron,
            timeout=timeout,
            result_ttl=result_ttl,
            failure_ttl=failure_ttl,
            meta=meta,
            name=name,  # type: ignore[arg-type]
        )
        if cjob.name in self._jobs:
            raise ValueError(f"CronJob {cjob.name!r} is already registered; call unregister() first to replace it")
        self._jobs[cjob.name] = cjob
        schedule = f"every {interval}s" if interval else f"cron '{cron}'"
        logger.info(
            "CronScheduler: registered %s on %s (%s)",
            cjob.name,
            queue,
            schedule,
        )
        return cjob

    def update(
        self,
        name: str,
        *,
        queue: str | None = None,
        args: tuple | None = None,
        kwargs: dict | None = None,
        interval: int | None = None,
        cron: str | None = None,
        timeout: int | None = None,
        result_ttl: int | None = None,
        failure_ttl: int | None = None,
        meta: dict[str, Any] | None = None,
    ) -> CronJob:
        """Update fields on a registered job in place.

        Only the provided keyword arguments are applied. If either ``interval``
        or ``cron`` is supplied the other is cleared, and the schedule is
        re-validated via ``CronJob.__post_init__``.

        Args:
            name: The registered job name.
            queue: New queue to enqueue into.
            args: New positional arguments.
            kwargs: New keyword arguments.
            interval: New interval in seconds (clears ``cron``).
            cron: New cron expression (clears ``interval``).
            timeout: New per-run timeout in seconds.
            result_ttl: New seconds to retain successful job rows.
            failure_ttl: New seconds to retain failed job rows.
            meta: New metadata dict.

        Returns:
            The updated :class:`~wrk.schemas.CronJob` instance.

        Raises:
            KeyError: If no job with that name is registered.
            ValueError: If the resulting schedule configuration is invalid.
        """
        cjob = self._jobs[name]
        if queue is not None:
            cjob.queue = queue
        if args is not None:
            cjob.args = args
        if kwargs is not None:
            cjob.kwargs = kwargs
        if interval is not None:
            cjob.interval = interval
            cjob.cron = None
        if cron is not None:
            cjob.cron = cron
            cjob.interval = None
        if timeout is not None:
            cjob.timeout = timeout
        if result_ttl is not None:
            cjob.result_ttl = result_ttl
        if failure_ttl is not None:
            cjob.failure_ttl = failure_ttl
        if meta is not None:
            cjob.meta = meta
        cjob.__post_init__()
        logger.info("CronScheduler: updated %s", name)
        return cjob

    def unregister(self, name: str) -> CronJob:
        """Remove a job by name.

        Args:
            name: The registered job name.

        Returns:
            The removed :class:`~wrk.schemas.CronJob` instance.

        Raises:
            KeyError: If no job with that name is registered.
        """
        job = self._jobs.pop(name)
        logger.info("CronScheduler: unregistered %s", name)
        return job

    # ------------------------------------------------------------------
    # Dynamic control
    # ------------------------------------------------------------------

    def pause(self, name: str) -> None:
        """Pause a job — it stays registered but won't be enqueued until resumed.

        Args:
            name: The registered job name.

        Raises:
            KeyError: If no job with that name is registered.
        """
        self._jobs[name].paused = True
        logger.info("CronScheduler: paused %s", name)

    def resume(self, name: str) -> None:
        """Resume a previously paused job.

        Args:
            name: The registered job name.

        Raises:
            KeyError: If no job with that name is registered.
        """
        self._jobs[name].paused = False
        logger.info("CronScheduler: resumed %s", name)

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    @property
    def jobs(self) -> dict[str, CronJob]:
        """Read-only snapshot of registered jobs keyed by name."""
        return dict(self._jobs)

    def get(self, name: str) -> CronJob | None:
        """Return the CronJob for *name*, or ``None`` if not registered.

        Args:
            name: The registered job name.

        Returns:
            The matching :class:`~wrk.schemas.CronJob`, or ``None``.
        """
        return self._jobs.get(name)

    def __len__(self) -> int:
        return len(self._jobs)

    def __contains__(self, name: object) -> bool:
        return name in self._jobs

    # ------------------------------------------------------------------
    # Registration & heartbeat
    # ------------------------------------------------------------------

    async def _register(self) -> None:
        """Insert this scheduler into the worker table with role ``'scheduler'``."""
        await self.app._worker_repo.register(
            self.id,
            self.name,
            [],
            json.dumps({"pid": os.getpid()}),
            role="scheduler",
        )
        logger.info("CronScheduler %s registered (%s)", self.name, self.id)

    async def _deregister(self) -> None:
        """Mark this scheduler as stopped in the worker table, if still connected."""
        if not self.app._connected:
            return
        try:
            await self.app._worker_repo.deregister(self.id)
        except Exception as exc:
            logger.warning("CronScheduler %s: deregister failed: %s", self.name, exc)

    async def _heartbeat_loop(self) -> None:
        """Periodically update ``heartbeat_at`` in the worker table while running."""
        interval = self.app.config.heartbeat_interval
        while self._running:
            try:
                await self.app._worker_repo.update_heartbeat(self.id)
            except Exception as exc:
                logger.warning("CronScheduler %s: heartbeat error: %s", self.name, exc)
            await asyncio.sleep(interval)

    # ------------------------------------------------------------------
    # Internal loop helpers
    # ------------------------------------------------------------------

    def _sleep_seconds(self) -> float:
        """Return seconds to sleep until the next job is due, capped at 60."""
        jobs = list(self._jobs.values())
        if not jobs:
            return 60.0
        return min(min(j.seconds_until_next() for j in jobs), 60.0)

    async def _tick(self) -> None:
        """Enqueue all jobs whose next run time has been reached."""
        for cjob in list(self._jobs.values()):
            if not cjob.should_run():
                continue
            try:
                await self.app.enqueue(
                    cjob.func,
                    *cjob.args,
                    _queue=cjob.queue,
                    _key=utils.tick_dedupe_key(cjob),
                    _timeout=cjob.timeout,
                    _result_ttl=cjob.result_ttl,
                    _failure_ttl=cjob.failure_ttl,
                    _meta=cjob.meta,
                    _cron_name=cjob.name,
                    **cjob.kwargs,
                )
                cjob.mark_enqueued()
                logger.info("CronScheduler: enqueued %s", cjob.name)
            except Exception as exc:
                logger.exception(
                    "CronScheduler: failed to enqueue %s: %s",
                    cjob.name,
                    exc,
                )

    async def _run_as_primary(self) -> None:
        """Run the tick loop as the primary scheduler until stopped."""
        logger.info("CronScheduler: running as primary (%d job(s))", len(self._jobs))
        while self._running:
            await self._tick()
            sleep = self._sleep_seconds()
            if sleep > 0:
                await asyncio.sleep(sleep)

    async def run(self) -> None:
        """Start the scheduler and block until shutdown.

        Registers this instance in the worker table, starts a heartbeat loop,
        then competes for a PostgreSQL session-level advisory lock. The instance
        that wins the lock runs as the primary scheduler; all others sit in
        standby and automatically promote when the primary's connection drops.

        On exit (normal or cancelled), the heartbeat task is cancelled and this
        instance is deregistered from the worker table.
        """
        self._running = True
        await self._register()
        self._heartbeat_task = asyncio.ensure_future(self._heartbeat_loop())

        try:
            while self._running:
                acquired = False
                try:
                    async with await self.app._connect() as lock_conn:
                        async with lock_conn.cursor() as cur:
                            await cur.execute("SELECT pg_try_advisory_lock(%s)", (self._lock_key,))
                            row = await cur.fetchone()
                        acquired = bool(row and row[0])

                        if acquired:
                            try:
                                await self._run_as_primary()
                            finally:
                                await lock_conn.execute("SELECT pg_advisory_unlock(%s)", (self._lock_key,))
                except asyncio.CancelledError:
                    break
                except Exception as exc:
                    logger.warning("CronScheduler: error: %s", exc)

                if not acquired and self._running:
                    logger.debug(
                        "CronScheduler: standby — retrying in %.0fs", self.app.config.cron_standby_retry_interval
                    )
                    await asyncio.sleep(self.app.config.cron_standby_retry_interval)
        finally:
            if self._heartbeat_task:
                self._heartbeat_task.cancel()
                await asyncio.gather(self._heartbeat_task, return_exceptions=True)
            await self._deregister()

    def stop(self) -> None:
        """Signal the scheduler loop to stop after the current tick completes."""
        self._running = False
