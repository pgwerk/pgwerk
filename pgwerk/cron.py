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
from datetime import datetime
from datetime import timezone
from datetime import timedelta

from pgwerk.schemas import Schedule
from pgwerk.utils import fn_path
from pgwerk.utils import schedule_tick_key
from pgwerk.repos import ScheduleAlreadyExists


if TYPE_CHECKING:
    from .app import Werk


logger = logging.getLogger(__name__)


ON_UNREGISTERED = ("keep", "pause", "delete")


class CronScheduler:
    """Postgres-backed cron scheduler.

    Schedule definitions live in ``_pgwerk_schedules``; all persistence and
    coordination happens through that table. Multiple scheduler processes share
    load via ``SELECT ... FOR UPDATE SKIP LOCKED`` — there is no primary/standby
    role.

    Typical usage::

        scheduler = CronScheduler(app, on_unregistered="pause")

        @scheduler.register(cron="0 9 * * *", queue="reports")
        async def send_report():
            ...

        async with app:
            await scheduler.run()

    Attributes:
        id: Unique hex identifier for this scheduler instance.
        name: Human-readable name derived from hostname and PID.
    """

    def __init__(self, app: "Werk", *, on_unregistered: str = "pause") -> None:
        """Initialize the scheduler.

        Args:
            app: The Werk app to enqueue jobs through.
            on_unregistered: Reconciliation policy at ``run()`` startup for
                schedules that exist in the DB but were not re-registered by
                this process:

                - ``"keep"``  — leave them as-is (DB remains source of truth;
                  removing code does not affect behaviour).
                - ``"pause"`` — set ``paused=True`` (default; recoverable, safe
                  against accidental "disappearing" schedules during rolling
                  deploys where subsets of hosts import subsets of modules).
                - ``"delete"`` — remove the row (destructive; use only when
                  this process owns the entire schedule inventory).
        """
        if on_unregistered not in ON_UNREGISTERED:
            raise ValueError(f"on_unregistered must be one of {ON_UNREGISTERED}, got {on_unregistered!r}")
        self.app = app
        self.on_unregistered = on_unregistered
        self._pending: dict[str, Schedule] = {}  # stage registrations made before run()
        self._running = False
        self.id = uuid.uuid4().hex
        self.name = f"{socket.gethostname()}.{os.getpid()}"
        self._heartbeat_task: asyncio.Task | None = None

    # ------------------------------------------------------------------
    # Registration (builds the desired-state set; DB write happens at sync)
    # ------------------------------------------------------------------

    def register(
        self,
        func: "Callable | None" = None,
        *,
        queue: str = "default",
        name: str | None = None,
        args: tuple | list | None = None,
        kwargs: dict | None = None,
        interval: int | None = None,
        cron: str | None = None,
        timeout: int | None = None,
        result_ttl: int | None = None,
        failure_ttl: int | None = None,
        meta: dict[str, Any] | None = None,
    ) -> "Callable":
        """Register *func* to run on a schedule.

        Can be used as a decorator (``@scheduler.register(interval=60)``) or as
        a call (``scheduler.register(my_fn, interval=60)``).

        The schedule is staged in-process; it is upserted into the DB once
        ``run()`` (or ``sync()``) is called. This matches how job workers are
        wired: the app instance collects the declaration at import time, and a
        single reconciliation pass hits the DB.

        Raises:
            ValueError: If both / neither of ``interval`` and ``cron`` are set,
                or if a schedule with the same name is already registered in
                this process.
        """
        def _register(f: Callable) -> Callable:
            sched_name = name or fn_path(f)
            if sched_name in self._pending:
                raise ValueError(
                    f"schedule {sched_name!r} is already registered in this process; "
                    "call unregister() first to replace it"
                )
            if (interval is None) == (cron is None):
                raise ValueError("Specify either interval or cron, not both")
            self._pending[sched_name] = Schedule(
                name=sched_name,
                function=fn_path(f),
                queue=queue,
                args=list(args) if args else [],
                kwargs=dict(kwargs) if kwargs else {},
                interval_secs=interval,
                cron=cron,
                timeout_secs=timeout,
                result_ttl=result_ttl,
                failure_ttl=failure_ttl,
                meta=meta,
            )
            return f

        return _register(func) if func is not None else _register

    def unregister(self, name: str) -> None:
        """Remove a schedule from the in-process set before ``sync()`` runs.

        After ``sync()``, use :meth:`delete` to remove the DB row.
        """
        self._pending.pop(name, None)

    # ------------------------------------------------------------------
    # Imperative registration (writes to DB immediately)
    # ------------------------------------------------------------------

    def _build_schedule(
        self,
        func: "Callable | str",
        *,
        queue: str,
        name: str | None,
        args: tuple | list | None,
        kwargs: dict | None,
        interval: int | None,
        cron: str | None,
        timeout: int | None,
        result_ttl: int | None,
        failure_ttl: int | None,
        meta: dict[str, Any] | None,
        next_run_at: datetime | None,
    ) -> Schedule:
        if (interval is None) == (cron is None):
            raise ValueError("Specify either interval or cron, not both")
        function_path = func if isinstance(func, str) else fn_path(func)
        sched_name = name or function_path
        return Schedule(
            name=sched_name,
            function=function_path,
            queue=queue,
            args=list(args) if args else [],
            kwargs=dict(kwargs) if kwargs else {},
            interval_secs=interval,
            cron=cron,
            timeout_secs=timeout,
            result_ttl=result_ttl,
            failure_ttl=failure_ttl,
            meta=meta,
            next_run_at=next_run_at,
        )

    async def schedule(
        self,
        func: "Callable | str",
        *,
        queue: str = "default",
        name: str | None = None,
        args: tuple | list | None = None,
        kwargs: dict | None = None,
        interval: int | None = None,
        cron: str | None = None,
        timeout: int | None = None,
        result_ttl: int | None = None,
        failure_ttl: int | None = None,
        meta: dict[str, Any] | None = None,
    ) -> Schedule:
        """Imperatively upsert a schedule into the database.

        Unlike :meth:`register` (which stages in-process for a later
        :meth:`sync`), this writes immediately and is safe to call after
        :meth:`run` has started. Re-calling with the same ``name`` updates
        every provided field and recomputes ``next_run_at`` from the new policy.

        Use :meth:`schedule_at` or :meth:`schedule_in` to start the first run
        at an explicit time instead of the default (policy-derived) one.
        """
        sched = self._build_schedule(
            func,
            queue=queue,
            name=name,
            args=args,
            kwargs=kwargs,
            interval=interval,
            cron=cron,
            timeout=timeout,
            result_ttl=result_ttl,
            failure_ttl=failure_ttl,
            meta=meta,
            next_run_at=None,
        )
        return await self.app._schedule_repo.upsert(sched)

    async def schedule_at(
        self,
        func: "Callable | str",
        first_run_at: datetime,
        *,
        queue: str = "default",
        name: str | None = None,
        args: tuple | list | None = None,
        kwargs: dict | None = None,
        interval: int | None = None,
        cron: str | None = None,
        timeout: int | None = None,
        result_ttl: int | None = None,
        failure_ttl: int | None = None,
        meta: dict[str, Any] | None = None,
    ) -> Schedule:
        """Like :meth:`schedule` but anchors the first run at ``first_run_at``.

        Subsequent runs follow the ``interval``/``cron`` policy normally.
        Naive datetimes are treated as UTC.
        """
        if first_run_at.tzinfo is None:
            first_run_at = first_run_at.replace(tzinfo=timezone.utc)
        sched = self._build_schedule(
            func,
            queue=queue,
            name=name,
            args=args,
            kwargs=kwargs,
            interval=interval,
            cron=cron,
            timeout=timeout,
            result_ttl=result_ttl,
            failure_ttl=failure_ttl,
            meta=meta,
            next_run_at=first_run_at,
        )
        return await self.app._schedule_repo.upsert(sched)

    async def schedule_in(
        self,
        delay_secs: float,
        func: "Callable | str",
        *,
        queue: str = "default",
        name: str | None = None,
        args: tuple | list | None = None,
        kwargs: dict | None = None,
        interval: int | None = None,
        cron: str | None = None,
        timeout: int | None = None,
        result_ttl: int | None = None,
        failure_ttl: int | None = None,
        meta: dict[str, Any] | None = None,
    ) -> Schedule:
        """Like :meth:`schedule` but starts the first run ``delay_secs`` from now."""
        first_run_at = datetime.now(timezone.utc) + timedelta(seconds=delay_secs)
        return await self.schedule_at(
            func,
            first_run_at,
            queue=queue,
            name=name,
            args=args,
            kwargs=kwargs,
            interval=interval,
            cron=cron,
            timeout=timeout,
            result_ttl=result_ttl,
            failure_ttl=failure_ttl,
            meta=meta,
        )

    # ------------------------------------------------------------------
    # Direct DB operations (available once the app is connected)
    # ------------------------------------------------------------------

    async def sync(self) -> tuple[int, int, list[str]]:
        """Push staged registrations to the DB and reconcile orphans.

        Returns:
            ``(inserted, updated, reconciled_names)``.
        """
        repo = self.app._schedule_repo
        inserted = 0
        updated = 0
        for schedule in self._pending.values():
            try:
                await repo.insert(schedule)
                inserted += 1
            except ScheduleAlreadyExists:
                await repo.update(
                    schedule.name,
                    function=schedule.function,
                    queue=schedule.queue,
                    args=schedule.args,
                    kwargs=schedule.kwargs,
                    interval_secs=schedule.interval_secs,
                    cron=schedule.cron,
                    timeout_secs=schedule.timeout_secs,
                    result_ttl=schedule.result_ttl,
                    failure_ttl=schedule.failure_ttl,
                    meta=schedule.meta,
                )
                updated += 1

        reconciled = await repo.reconcile(
            list(self._pending.keys()), on_unregistered=self.on_unregistered
        )
        if reconciled:
            logger.info(
                "CronScheduler: %s %d orphan schedule(s): %s",
                "deleted" if self.on_unregistered == "delete" else "paused",
                len(reconciled),
                reconciled,
            )
        return inserted, updated, reconciled

    async def update(self, name: str, **fields: Any) -> Schedule:
        """Update a persisted schedule in place.

        Pass any of ``queue``, ``args``, ``kwargs``, ``interval``, ``cron``,
        ``timeout``, ``result_ttl``, ``failure_ttl``, ``meta``, ``paused``.

        Setting ``interval`` clears ``cron`` (and vice versa) and recomputes
        ``next_run_at``.
        """
        # Translate friendly kwargs to DB field names.
        translated: dict[str, Any] = {}
        for k, v in fields.items():
            if k == "interval":
                translated["interval_secs"] = v
            elif k == "timeout":
                translated["timeout_secs"] = v
            else:
                translated[k] = v
        return await self.app._schedule_repo.update(name, **translated)

    async def pause(self, name: str) -> Schedule:
        """Pause a schedule; its rows remain but tick() will skip it."""
        return await self.app._schedule_repo.update(name, paused=True)

    async def resume(self, name: str) -> Schedule:
        """Unpause a schedule."""
        return await self.app._schedule_repo.update(name, paused=False)

    async def delete(self, name: str) -> bool:
        """Delete a schedule row. Returns True if a row was removed."""
        return await self.app._schedule_repo.delete(name)

    async def trigger(self, name: str) -> Schedule | None:
        """Force a schedule due NOW so the next tick enqueues it."""
        return await self.app._schedule_repo.trigger(name)

    async def list_schedules(self) -> list[Schedule]:
        """Return every schedule row, ordered by name."""
        return await self.app._schedule_repo.list_all()

    async def get(self, name: str) -> Schedule | None:
        """Return a single schedule by name, or ``None``."""
        return await self.app._schedule_repo.get(name)

    # ------------------------------------------------------------------
    # Scheduler lifecycle
    # ------------------------------------------------------------------

    async def _register_instance(self) -> None:
        await self.app._worker_repo.register(
            self.id,
            self.name,
            [],
            json.dumps({"pid": os.getpid()}),
            role="scheduler",
        )
        logger.info("CronScheduler %s registered (%s)", self.name, self.id)

    async def _deregister_instance(self) -> None:
        if not self.app._connected:
            return
        try:
            await self.app._worker_repo.deregister(self.id)
        except Exception as exc:
            logger.warning("CronScheduler %s: deregister failed: %s", self.name, exc)

    async def _heartbeat_loop(self) -> None:
        interval = self.app.config.heartbeat_interval
        while self._running:
            try:
                await self.app._worker_repo.update_heartbeat(self.id)
            except Exception as exc:
                logger.warning("CronScheduler %s: heartbeat error: %s", self.name, exc)
            await asyncio.sleep(interval)

    async def _enqueue_due(self, schedule: Schedule) -> None:
        await self.app.enqueue(
            schedule.function,
            *schedule.args,
            _queue=schedule.queue,
            _key=schedule_tick_key(schedule),
            _timeout=schedule.timeout_secs,
            _result_ttl=schedule.result_ttl,
            _failure_ttl=schedule.failure_ttl,
            _meta=schedule.meta,
            _schedule_name=schedule.name,
            **schedule.kwargs,
        )

    async def _tick(self) -> int:
        """Fire all due schedules. Returns the number fired."""
        try:
            return await self.app._schedule_repo.tick_once(self._enqueue_due)
        except Exception as exc:
            logger.exception("CronScheduler: tick error: %s", exc)
            return 0

    async def run(self) -> None:
        """Run the scheduler until :meth:`stop` is called or the task is cancelled.

        Performs a one-time sync of staged registrations, registers the
        instance in the worker table, then loops: tick, sleep until the next
        due schedule (capped at ``poll_interval`` seconds).
        """
        self._running = True
        await self.sync()
        await self._register_instance()
        self._heartbeat_task = asyncio.ensure_future(self._heartbeat_loop())

        poll_cap = max(1.0, float(self.app.config.poll_interval))
        try:
            while self._running:
                try:
                    await self._tick()
                    sleep_for = await self.app._schedule_repo.seconds_until_next_due(
                        fallback=poll_cap
                    )
                    await asyncio.sleep(min(sleep_for, poll_cap))
                except asyncio.CancelledError:
                    break
                except Exception as exc:
                    logger.warning("CronScheduler: loop error: %s", exc)
                    await asyncio.sleep(poll_cap)
        finally:
            if self._heartbeat_task:
                self._heartbeat_task.cancel()
                await asyncio.gather(self._heartbeat_task, return_exceptions=True)
            await self._deregister_instance()

    def stop(self) -> None:
        """Signal the scheduler loop to stop after the current tick completes."""
        self._running = False
