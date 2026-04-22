from __future__ import annotations

import asyncio
import logging

from typing import TYPE_CHECKING
from typing import Any
from typing import Callable

from wrk import utils
from wrk.schemas import CronJob


if TYPE_CHECKING:
    from .app import Wrk


logger = logging.getLogger(__name__)


class CronScheduler:
    """
    Async cron scheduler with distributed locking via PostgreSQL advisory locks.

    When multiple instances start (e.g. several worker processes), only one
    becomes the *primary* scheduler. The others run in *standby* mode and
    automatically promote if the primary's connection drops.

    Jobs can be added, removed, paused, and resumed at any time — even while
    the scheduler is running.

    Usage::

        scheduler = CronScheduler(app)
        scheduler.register(send_report, queue="reports", cron="0 9 * * *")
        scheduler.register(cleanup, queue="default", interval=300)

        async with app:
            await asyncio.gather(worker.run(), scheduler.run())
    """

    def __init__(self, app: "Wrk") -> None:
        self.app = app
        self._jobs: dict[str, CronJob] = {}
        self._running = False
        self._lock_key = utils.advisory_key(f"{app.prefix}:cron_scheduler")

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
        Registering under an existing name replaces the previous job.

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
            ValueError: If both ``interval`` and ``cron`` are set, or neither.
        """
        if isinstance(func_or_cronjob, CronJob):
            key = func_or_cronjob.name or name
            if key is None:
                key = f"{func_or_cronjob.func.__module__}.{func_or_cronjob.func.__qualname__}"
                func_or_cronjob.name = key
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
        self._jobs[cjob.name] = cjob
        schedule = f"every {interval}s" if interval else f"cron '{cron}'"
        logger.info(
            "CronScheduler: registered %s on %s (%s)",
            cjob.name,
            queue,
            schedule,
        )
        return cjob

    def unregister(self, name: str) -> CronJob:
        """Remove a job by name. Raises ``KeyError`` if not found."""
        job = self._jobs.pop(name)
        logger.info("CronScheduler: unregistered %s", name)
        return job

    # ------------------------------------------------------------------
    # Dynamic control
    # ------------------------------------------------------------------

    def pause(self, name: str) -> None:
        """Pause a job by name — it stays registered but won't be enqueued."""
        self._jobs[name].paused = True
        logger.info("CronScheduler: paused %s", name)

    def resume(self, name: str) -> None:
        """Resume a previously paused job."""
        self._jobs[name].paused = False
        logger.info("CronScheduler: resumed %s", name)

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    @property
    def jobs(self) -> dict[str, CronJob]:
        """Read-only view of registered jobs keyed by name."""
        return dict(self._jobs)

    def get(self, name: str) -> CronJob | None:
        """Return the CronJob for *name*, or ``None`` if not registered."""
        return self._jobs.get(name)

    def __len__(self) -> int:
        return len(self._jobs)

    def __contains__(self, name: object) -> bool:
        return name in self._jobs

    # ------------------------------------------------------------------
    # Internal loop helpers
    # ------------------------------------------------------------------

    def _sleep_seconds(self) -> float:
        jobs = list(self._jobs.values())
        if not jobs:
            return 60.0
        return min(min(j.seconds_until_next() for j in jobs), 60.0)

    async def _tick(self) -> None:
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
        logger.info("CronScheduler: running as primary (%d job(s))", len(self._jobs))
        while self._running:
            await self._tick()
            sleep = self._sleep_seconds()
            if sleep > 0:
                await asyncio.sleep(sleep)

    async def run(self) -> None:
        """Run the scheduler loop.

        Acquires a PostgreSQL session-level advisory lock so that only one
        instance is active at a time. Competing instances retry every
        ``_STANDBY_RETRY_INTERVAL`` seconds and automatically promote when
        the primary's connection is released.
        """
        self._running = True
        pool = self.app._pool_or_raise()

        while self._running:
            acquired = False
            try:
                async with pool.connection() as lock_conn:
                    async with lock_conn.cursor() as cur:
                        await cur.execute("SELECT pg_try_advisory_lock(%s)", (self._lock_key,))
                        row = await cur.fetchone()
                    acquired = bool(row and row[0])

                    if acquired:
                        try:
                            await self._run_as_primary()
                        finally:
                            # Explicitly release before returning connection to pool.
                            await lock_conn.execute("SELECT pg_advisory_unlock(%s)", (self._lock_key,))
                    # lock_conn goes back to pool here; if the process died mid-run,
                    # the session-level lock is released automatically.
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.warning("CronScheduler: error: %s", exc)

            if not acquired and self._running:
                logger.debug("CronScheduler: standby — retrying in %.0fs", self.app.config.cron_standby_retry_interval)
                await asyncio.sleep(self.app.config.cron_standby_retry_interval)

    def stop(self) -> None:
        """Signal the scheduler loop to stop after the current tick completes."""
        self._running = False
