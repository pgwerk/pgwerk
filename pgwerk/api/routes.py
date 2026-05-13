from __future__ import annotations

import logging

from typing import Any
from typing import Annotated

from litestar import Router
from .auth import Guard
from litestar import Response
from litestar import Controller
from litestar import delete
from litestar import get
from litestar import post
from litestar.di import Provide
from litestar.params import Parameter
from litestar.response import File
from litestar.exceptions import ClientException
from litestar.exceptions import NotFoundException

import psycopg

from .spa import resolve_spa_file
from .exporter import get_exporter
from ..app import Werk
from ..repos import ScheduleAlreadyExists
from ..exporter import WerkExporter
from .models import TableInfo
from .models import QueueStats
from .models import ServerInfo
from .models import JobResponse
from .models import ScheduleStats
from .models import ScheduleResponse
from .models import PurgeRequest
from .models import StatsResponse
from .models import EnqueueRequest
from .models import WorkerResponse
from .models import QueueDepthPoint
from .models import BulkCancelRequest
from .models import ExecutionResponse
from .models import CreateScheduleBody
from .models import BulkRequeueRequest
from .models import WorkerThroughputPoint
from ..exceptions import JobNotFound


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Jobs
# ---------------------------------------------------------------------------


class JobController(Controller):
    path = "/jobs"

    @get("")
    async def list_jobs(
        self,
        werk: Werk,
        queue: Annotated[str | None, Parameter(query="queue")] = None,
        status: Annotated[str | None, Parameter(query="status")] = None,
        worker_id: Annotated[str | None, Parameter(query="worker_id")] = None,
        search: Annotated[str | None, Parameter(query="search")] = None,
        schedule_name: Annotated[str | None, Parameter(query="schedule_name")] = None,
        limit: Annotated[int, Parameter(query="limit", ge=1, le=500)] = 50,
        offset: Annotated[int, Parameter(query="offset", ge=0)] = 0,
    ) -> list[JobResponse]:
        """List jobs with optional filters.

        Args:
            werk: Werk application instance.
            queue: Filter by queue name.
            status: Filter by job status (e.g. pending, running, complete).
            worker_id: Filter by the worker currently holding the job.
            search: Full-text search against function name or payload.
            schedule_name: Filter to jobs enqueued by a specific schedule.
            limit: Maximum number of results to return (1–500).
            offset: Number of results to skip for pagination.

        Returns:
            List of matching jobs.
        """
        queue_filter: str | list[str] | None = queue
        if queue and "," in queue:
            queue_filter = [q for q in (s.strip() for s in queue.split(",")) if q]
        jobs = await werk.list_jobs(
            queue=queue_filter, status=status, worker_id=worker_id, search=search,
            schedule_name=schedule_name, limit=limit, offset=offset
        )
        return [JobResponse.from_job(j) for j in jobs]

    @post("", status_code=201)
    async def create_job(self, werk: Werk, data: EnqueueRequest) -> JobResponse:
        """Enqueue a new job.

        Args:
            werk: Werk application instance.
            data: Job creation parameters including function name, args, queue, and scheduling options.

        Returns:
            The newly created job.

        Raises:
            ClientException: If the job could not be created.
        """
        try:
            job = await werk.enqueue(
                data.function,
                *data.args,
                _queue=data.queue,
                _priority=data.priority,
                _key=data.key,
                _delay=data.delay,
                _at=data.scheduled_at,
                _retry=data.max_attempts,
                _timeout=data.timeout_secs,
                _meta=data.meta,
                _schedule_name=data.schedule_name,
                **data.kwargs,
            )
        except psycopg.errors.ForeignKeyViolation:
            raise NotFoundException(
                detail=f"Schedule {data.schedule_name!r} not found"
            )
        if job is None:
            raise ClientException(detail="Job could not be created")
        return JobResponse.from_job(job)

    @get("/{job_id:str}")
    async def get_job(self, werk: Werk, job_id: str) -> JobResponse:
        """Retrieve a single job by ID.

        Args:
            werk: Werk application instance.
            job_id: Unique job identifier.

        Returns:
            The requested job.

        Raises:
            NotFoundException: If no job with the given ID exists.
        """
        try:
            job = await werk.get_job(job_id)
        except JobNotFound:
            raise NotFoundException(detail=f"Job {job_id!r} not found")
        return JobResponse.from_job(job)

    @get("/{job_id:str}/executions")
    async def get_job_executions(self, werk: Werk, job_id: str) -> list[ExecutionResponse]:
        """List all execution attempts for a job.

        Args:
            werk: Werk application instance.
            job_id: Unique job identifier.

        Returns:
            Ordered list of execution records for the job.

        Raises:
            NotFoundException: If no job with the given ID exists.
        """
        try:
            await werk.get_job(job_id)
        except JobNotFound:
            raise NotFoundException(detail=f"Job {job_id!r} not found")
        executions = await werk.get_executions(job_id)
        return [ExecutionResponse.from_execution(e) for e in executions]

    @get("/{job_id:str}/dependencies")
    async def get_job_dependencies(self, werk: Werk, job_id: str) -> list[str]:
        """List the dependency job IDs for a job.

        Args:
            werk: Werk application instance.
            job_id: Unique job identifier.

        Returns:
            List of job IDs that must complete before this job can run.

        Raises:
            NotFoundException: If no job with the given ID exists.
        """
        try:
            await werk.get_job(job_id)
        except JobNotFound:
            raise NotFoundException(detail=f"Job {job_id!r} not found")
        return await werk.get_job_dependencies(job_id)

    @post("/{job_id:str}/cancel")
    async def cancel_job(self, werk: Werk, job_id: str) -> dict[str, Any]:
        """Cancel a pending or scheduled job.

        Args:
            werk: Werk application instance.
            job_id: Unique job identifier.

        Returns:
            Confirmation with ``cancelled`` flag and ``job_id``.

        Raises:
            NotFoundException: If the job does not exist or cannot be cancelled.
        """
        ok = await werk.cancel_job(job_id)
        if not ok:
            raise NotFoundException(detail=f"Job {job_id!r} not found or not cancellable")
        return {"cancelled": True, "job_id": job_id}

    @post("/{job_id:str}/abort")
    async def abort_job(self, werk: Werk, job_id: str) -> dict[str, Any]:
        """Abort an actively running job.

        Args:
            werk: Werk application instance.
            job_id: Unique job identifier.

        Returns:
            Confirmation with ``aborted`` flag and ``job_id``.

        Raises:
            NotFoundException: If the job does not exist or is not currently active.
        """
        ok = await werk.abort_job(job_id)
        if not ok:
            raise NotFoundException(detail=f"Job {job_id!r} not found or not active")
        return {"aborted": True, "job_id": job_id}

    @post("/{job_id:str}/requeue")
    async def requeue_job(self, werk: Werk, job_id: str) -> dict[str, Any]:
        """Re-queue a failed or cancelled job for another attempt.

        Args:
            werk: Werk application instance.
            job_id: Unique job identifier.

        Returns:
            Confirmation with ``requeued`` flag and ``job_id``.

        Raises:
            NotFoundException: If the job does not exist or cannot be re-queued.
        """
        ok = await werk.requeue_job(job_id)
        if not ok:
            raise NotFoundException(detail=f"Job {job_id!r} not found or not re-queueable")
        return {"requeued": True, "job_id": job_id}

    @delete("/{job_id:str}", status_code=204)
    async def delete_job(self, werk: Werk, job_id: str) -> None:
        """Permanently delete a job record.

        Args:
            werk: Werk application instance.
            job_id: Unique job identifier.
        """
        await werk.delete_job(job_id)

    @post("/requeue")
    async def requeue_jobs(self, werk: Werk, data: BulkRequeueRequest) -> dict[str, Any]:
        """Bulk re-queue failed or cancelled jobs matching the given criteria.

        Args:
            werk: Werk application instance.
            data: Filters specifying which jobs to re-queue (queue name, function name).

        Returns:
            Number of jobs re-queued.
        """
        requeued = await werk.bulk_requeue_jobs(queue=data.queue, function_name=data.function_name)
        return {"requeued": requeued}

    @post("/cancel")
    async def cancel_jobs(self, werk: Werk, data: BulkCancelRequest) -> dict[str, Any]:
        """Bulk cancel pending jobs in a queue.

        Args:
            werk: Werk application instance.
            data: Filters specifying which queue to cancel jobs from.

        Returns:
            Number of jobs cancelled.
        """
        cancelled = await werk.bulk_cancel_jobs(queue=data.queue)
        return {"cancelled": cancelled}

    @post("/purge")
    async def purge_jobs(self, werk: Werk, data: PurgeRequest) -> dict[str, Any]:
        """Delete terminal jobs older than a given age.

        Only jobs in ``complete``, ``failed``, ``aborted``, or ``cancelled`` status
        can be purged.

        Args:
            werk: Werk application instance.
            data: Purge criteria including target statuses and minimum age in days.

        Returns:
            Number of jobs purged.

        Raises:
            ClientException: If any requested status is not purgeable.
        """
        _purgeable = {"complete", "failed", "aborted", "cancelled"}
        invalid = set(data.statuses) - _purgeable
        if invalid:
            raise ClientException(detail=f"Cannot purge jobs with status: {', '.join(sorted(invalid))}")
        purged = await werk.purge_jobs(statuses=data.statuses, older_than_days=data.older_than_days)
        return {"purged": purged}


# ---------------------------------------------------------------------------
# Workers
# ---------------------------------------------------------------------------


class WorkerController(Controller):
    path = "/workers"

    @get("")
    async def list_workers(self, werk: Werk) -> list[WorkerResponse]:
        """List all registered workers.

        Args:
            werk: Werk application instance.

        Returns:
            List of worker records including last heartbeat and status.
        """
        rows = await werk.list_workers()
        return [WorkerResponse.from_row(r) for r in rows]

    @get("/{worker_id:str}")
    async def get_worker(self, werk: Werk, worker_id: str) -> WorkerResponse:
        """Retrieve a single worker by ID.

        Args:
            werk: Werk application instance.
            worker_id: Unique worker identifier.

        Returns:
            The requested worker record.

        Raises:
            NotFoundException: If no worker with the given ID exists.
        """
        row = await werk.get_worker(worker_id)
        if row is None:
            raise NotFoundException(detail=f"Worker {worker_id!r} not found")
        return WorkerResponse.from_row(row)

    @get("/{worker_id:str}/jobs")
    async def list_worker_jobs(
        self,
        werk: Werk,
        worker_id: str,
        limit: Annotated[int, Parameter(query="limit", ge=1, le=500)] = 50,
        offset: Annotated[int, Parameter(query="offset", ge=0)] = 0,
    ) -> list[JobResponse]:
        """List jobs currently claimed by a worker.

        Args:
            werk: Werk application instance.
            worker_id: Unique worker identifier.
            limit: Maximum number of results to return (1–500).
            offset: Number of results to skip for pagination.

        Returns:
            List of jobs claimed by the specified worker.
        """
        jobs = await werk.list_worker_jobs(worker_id=worker_id, limit=limit, offset=offset)
        return [JobResponse.from_job(j) for j in jobs]


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------


class StatsController(Controller):
    path = "/stats"

    @get("")
    async def get_stats(self, werk: Werk) -> StatsResponse:
        """Return aggregate queue and worker statistics.

        Args:
            werk: Werk application instance.

        Returns:
            Summary of all queues, total job count, and number of online workers.
        """
        queue_rows, total, workers_online = await werk.get_queue_stats()
        return StatsResponse(
            queues=[QueueStats.from_row(r) for r in queue_rows],
            total_jobs=total,
            workers_online=workers_online,
        )

    @get("/throughput")
    async def get_throughput_history(
        self,
        werk: Werk,
        minutes: Annotated[int, Parameter(query="minutes", ge=1, le=10080)] = 1440,
    ) -> list[WorkerThroughputPoint]:
        """Return worker throughput over a time window.

        Args:
            werk: Werk application instance.
            minutes: Time window in minutes to look back (1–10080, default 1440 = 24 h).

        Returns:
            Time-series data points of jobs completed per interval.
        """
        rows = await werk.get_throughput_history(minutes)
        return [WorkerThroughputPoint.from_row(r) for r in rows]

    @get("/queue-depth")
    async def get_queue_depth_history(
        self,
        werk: Werk,
        minutes: Annotated[int, Parameter(query="minutes", ge=1, le=10080)] = 1440,
    ) -> list[QueueDepthPoint]:
        """Return queue depth over a time window.

        Args:
            werk: Werk application instance.
            minutes: Time window in minutes to look back (1–10080, default 1440 = 24 h).

        Returns:
            Time-series data points of pending job count per interval.
        """
        rows = await werk.get_queue_depth_history(minutes)
        return [QueueDepthPoint.from_row(r) for r in rows]


# ---------------------------------------------------------------------------
# Schedules
# ---------------------------------------------------------------------------


class SchedulesController(Controller):
    path = "/schedules"

    @post("")
    async def create_schedule(self, werk: Werk, data: CreateScheduleBody) -> ScheduleResponse:
        """Create a new schedule.

        Args:
            werk: Werk application instance.
            data: Schedule definition.

        Raises:
            ClientException: If a schedule with the same name already exists, or
                neither ``cron`` nor ``interval_secs`` is provided.
        """
        from ..schemas import Schedule

        if not data.cron and not data.interval_secs:
            raise ClientException(detail="provide either 'cron' or 'interval_secs'")
        try:
            schedule = await werk.create_schedule(
                Schedule(
                    name=data.name,
                    function=data.function,
                    queue=data.queue,
                    cron=data.cron,
                    interval_secs=data.interval_secs,
                    kwargs=data.kwargs,
                )
            )
        except ScheduleAlreadyExists as exc:
            raise ClientException(detail=str(exc)) from exc
        return ScheduleResponse.from_schedule(schedule)

    @get("")
    async def list_schedules(self, werk: Werk) -> list[ScheduleResponse]:
        """List every registered schedule row.

        Includes schedules that have never fired. Use ``GET /schedules/stats``
        for per-schedule aggregate job statistics.

        Args:
            werk: Werk application instance.
        """
        rows = await werk.list_schedules()
        return [ScheduleResponse.from_schedule(s) for s in rows]

    @get("/stats")
    async def list_schedule_stats(self, werk: Werk) -> list[ScheduleStats]:
        """Per-schedule aggregate job statistics.

        Derived from ``_pgwerk_jobs`` grouped by ``schedule_name``; schedules
        that have never enqueued a job will not appear.

        Args:
            werk: Werk application instance.
        """
        rows = await werk.list_schedule_stats()
        return [ScheduleStats.from_row(r) for r in rows]

    @get("/{name:str}")
    async def get_schedule(self, werk: Werk, name: str) -> ScheduleResponse:
        """Return a single schedule by name.

        Args:
            werk: Werk application instance.
            name: Registered schedule name.

        Raises:
            NotFoundException: If no schedule with the given name exists.
        """
        schedule = await werk.get_schedule(name)
        if schedule is None:
            raise NotFoundException(detail=f"Schedule {name!r} not found")
        return ScheduleResponse.from_schedule(schedule)

    @post("/{name:str}/trigger")
    async def trigger_schedule(self, werk: Werk, name: str) -> ScheduleResponse:
        """Force the named schedule to become due on the next tick.

        Args:
            werk: Werk application instance.
            name: Registered schedule name.

        Raises:
            NotFoundException: If no schedule with the given name exists.
        """
        schedule = await werk.trigger_schedule(name)
        if schedule is None:
            raise NotFoundException(detail=f"Schedule {name!r} not found")
        return ScheduleResponse.from_schedule(schedule)

    @delete("/{name:str}", status_code=204)
    async def delete_schedule(self, werk: Werk, name: str) -> None:
        """Permanently delete a schedule by name.

        Args:
            werk: Werk application instance.
            name: Registered schedule name.

        Raises:
            NotFoundException: If no schedule with the given name exists.
        """
        deleted = await werk.delete_schedule(name)
        if not deleted:
            raise NotFoundException(detail=f"Schedule {name!r} not found")

    @post("/{name:str}/pause")
    async def pause_schedule(self, werk: Werk, name: str) -> ScheduleResponse:
        """Pause the named schedule so it is skipped on each tick.

        Args:
            werk: Werk application instance.
            name: Registered schedule name.

        Raises:
            NotFoundException: If no schedule with the given name exists.
        """
        schedule = await werk.pause_schedule(name)
        if schedule is None:
            raise NotFoundException(detail=f"Schedule {name!r} not found")
        return ScheduleResponse.from_schedule(schedule)

    @post("/{name:str}/resume")
    async def resume_schedule(self, werk: Werk, name: str) -> ScheduleResponse:
        """Resume a paused schedule.

        Args:
            werk: Werk application instance.
            name: Registered schedule name.

        Raises:
            NotFoundException: If no schedule with the given name exists.
        """
        schedule = await werk.resume_schedule(name)
        if schedule is None:
            raise NotFoundException(detail=f"Schedule {name!r} not found")
        return ScheduleResponse.from_schedule(schedule)


# ---------------------------------------------------------------------------
# Server
# ---------------------------------------------------------------------------


class ServerController(Controller):
    path = "/server"

    @get("")
    async def get_server_info(self, werk: Werk) -> ServerInfo:
        """Return Postgres server information and table sizes.

        Args:
            werk: Werk application instance.

        Returns:
            Postgres version, total database size, and per-table row counts and sizes.
        """
        pg_version, db_size_bytes, pgwerk_size_bytes, schema, table_rows = await werk.get_server_info()
        tables = [TableInfo(name=r["name"], size_bytes=r["size_bytes"], row_count=r["row_count"]) for r in table_rows]
        return ServerInfo(
            pg_version=pg_version,
            db_size_bytes=db_size_bytes,
            pgwerk_size_bytes=pgwerk_size_bytes,
            schema=schema,
            tables=tables,
        )

    @post("/sweep")
    async def run_sweep(self, werk: Werk) -> dict[str, Any]:
        """Sweep stale worker claims and release orphaned jobs.

        Args:
            werk: Werk application instance.

        Returns:
            Count and list of job IDs that were swept.
        """
        swept = await werk.sweep()
        return {"swept": len(swept), "job_ids": swept}

    @post("/reschedule-stuck")
    async def reschedule_stuck(self, werk: Werk) -> dict[str, Any]:
        """Reschedule jobs that have been stuck in running state past their timeout.

        Args:
            werk: Werk application instance.

        Returns:
            Number of jobs rescheduled.
        """
        rescheduled = await werk.reschedule_stuck()
        return {"rescheduled": rescheduled}

    @post("/vacuum")
    async def vacuum_tables(self, werk: Werk) -> dict[str, Any]:
        """Run VACUUM ANALYZE on all wrk tables.

        Args:
            werk: Werk application instance.

        Returns:
            Confirmation that vacuum completed.
        """
        await werk.vacuum()
        return {"vacuumed": True}

    @post("/truncate")
    async def truncate_tables(self, werk: Werk) -> dict[str, Any]:
        """Truncate all wrk tables, removing all jobs and worker records.

        Args:
            werk: Werk application instance.

        Returns:
            Confirmation that truncation completed.
        """
        await werk.truncate()
        return {"truncated": True}


# ---------------------------------------------------------------------------
# Core
# ---------------------------------------------------------------------------


class CoreController(Controller):
    @get("/health", tags=["Core"])
    async def health(self) -> dict[str, str]:
        return {"status": "ok"}


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------


class MetricsController(Controller):
    dependencies = {"exporter": Provide(get_exporter, use_cache=False)}

    @get("/metrics", media_type="text/plain", include_in_schema=False)
    async def get_metrics(self, exporter: WerkExporter | None) -> Response[bytes]:
        """Serve current Prometheus metrics.

        Args:
            exporter: WerkExporter instance injected via dependency.

        Returns:
            Prometheus text-format metrics payload.
        """
        if exporter is None:
            return Response(content=b"", media_type="text/plain", status_code=503)
        body, content_type = exporter.metrics_bytes()
        return Response(content=body, media_type=content_type)


# ---------------------------------------------------------------------------
# SPA
# ---------------------------------------------------------------------------


class SpaController(Controller):
    dependencies = {"served_file": Provide(resolve_spa_file, sync_to_thread=True)}

    @get("/", include_in_schema=False)
    async def spa_index(self, served_file: File) -> File:
        return served_file

    @get("/{path:path}", include_in_schema=False)
    async def spa_fallback(self, served_file: File) -> File:
        return served_file


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------


def make_router(guards: list[Guard] | None = None) -> Router:
    return Router(
        path="/api",
        route_handlers=[
            JobController,
            WorkerController,
            StatsController,
            SchedulesController,
            ServerController,
            CoreController,
        ],
        guards=guards or [],
    )
