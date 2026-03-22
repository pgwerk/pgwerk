from __future__ import annotations

import logging

from typing import Any
from typing import Annotated

from litestar import Controller
from litestar import Router
from litestar import get
from litestar import post
from litestar import delete
from litestar.params import Parameter
from litestar.exceptions import NotFoundException
from litestar.exceptions import ClientException

from ..app import Wrk
from ..exceptions import JobNotFound
from .models import TableInfo
from .models import QueueStats
from .models import ServerInfo
from .models import JobResponse
from .models import CronJobStats
from .models import PurgeRequest
from .models import StatsResponse
from .models import EnqueueRequest
from .models import WorkerResponse
from .models import QueueDepthPoint
from .models import ExecutionResponse
from .models import BulkCancelRequest
from .models import BulkRequeueRequest
from .models import WorkerThroughputPoint


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Jobs
# ---------------------------------------------------------------------------


class JobController(Controller):
    path = "/jobs"

    @get("")
    async def list_jobs(
        self,
        wrk: Wrk,
        queue: Annotated[str | None, Parameter(query="queue")] = None,
        status: Annotated[str | None, Parameter(query="status")] = None,
        worker_id: Annotated[str | None, Parameter(query="worker_id")] = None,
        search: Annotated[str | None, Parameter(query="search")] = None,
        limit: Annotated[int, Parameter(query="limit", ge=1, le=500)] = 50,
        offset: Annotated[int, Parameter(query="offset", ge=0)] = 0,
    ) -> list[JobResponse]:
        """List jobs with optional filters.

        Args:
            wrk: Wrk application instance.
            queue: Filter by queue name.
            status: Filter by job status (e.g. pending, running, complete).
            worker_id: Filter by the worker currently holding the job.
            search: Full-text search against function name or payload.
            limit: Maximum number of results to return (1–500).
            offset: Number of results to skip for pagination.

        Returns:
            List of matching jobs.
        """
        jobs = await wrk.list_jobs(
            queue=queue, status=status, worker_id=worker_id, search=search, limit=limit, offset=offset
        )
        return [JobResponse.from_job(j) for j in jobs]

    @post("", status_code=201)
    async def create_job(self, wrk: Wrk, data: EnqueueRequest) -> JobResponse:
        """Enqueue a new job.

        Args:
            wrk: Wrk application instance.
            data: Job creation parameters including function name, args, queue, and scheduling options.

        Returns:
            The newly created job.

        Raises:
            ClientException: If the job could not be created.
        """
        job = await wrk.enqueue(
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
            _cron_name=data.cron_name,
            **data.kwargs,
        )
        if job is None:
            raise ClientException(detail="Job could not be created")
        return JobResponse.from_job(job)

    @get("/{job_id:str}")
    async def get_job(self, wrk: Wrk, job_id: str) -> JobResponse:
        """Retrieve a single job by ID.

        Args:
            wrk: Wrk application instance.
            job_id: Unique job identifier.

        Returns:
            The requested job.

        Raises:
            NotFoundException: If no job with the given ID exists.
        """
        try:
            job = await wrk.get_job(job_id)
        except JobNotFound:
            raise NotFoundException(detail=f"Job {job_id!r} not found")
        return JobResponse.from_job(job)

    @get("/{job_id:str}/executions")
    async def get_job_executions(self, wrk: Wrk, job_id: str) -> list[ExecutionResponse]:
        """List all execution attempts for a job.

        Args:
            wrk: Wrk application instance.
            job_id: Unique job identifier.

        Returns:
            Ordered list of execution records for the job.

        Raises:
            NotFoundException: If no job with the given ID exists.
        """
        try:
            await wrk.get_job(job_id)
        except JobNotFound:
            raise NotFoundException(detail=f"Job {job_id!r} not found")
        executions = await wrk.get_executions(job_id)
        return [ExecutionResponse.from_execution(e) for e in executions]

    @get("/{job_id:str}/dependencies")
    async def get_job_dependencies(self, wrk: Wrk, job_id: str) -> list[str]:
        """List the dependency job IDs for a job.

        Args:
            wrk: Wrk application instance.
            job_id: Unique job identifier.

        Returns:
            List of job IDs that must complete before this job can run.

        Raises:
            NotFoundException: If no job with the given ID exists.
        """
        try:
            await wrk.get_job(job_id)
        except JobNotFound:
            raise NotFoundException(detail=f"Job {job_id!r} not found")
        return await wrk.get_job_dependencies(job_id)

    @post("/{job_id:str}/cancel")
    async def cancel_job(self, wrk: Wrk, job_id: str) -> dict[str, Any]:
        """Cancel a pending or scheduled job.

        Args:
            wrk: Wrk application instance.
            job_id: Unique job identifier.

        Returns:
            Confirmation with ``cancelled`` flag and ``job_id``.

        Raises:
            NotFoundException: If the job does not exist or cannot be cancelled.
        """
        ok = await wrk.cancel_job(job_id)
        if not ok:
            raise NotFoundException(detail=f"Job {job_id!r} not found or not cancellable")
        return {"cancelled": True, "job_id": job_id}

    @post("/{job_id:str}/abort")
    async def abort_job(self, wrk: Wrk, job_id: str) -> dict[str, Any]:
        """Abort an actively running job.

        Args:
            wrk: Wrk application instance.
            job_id: Unique job identifier.

        Returns:
            Confirmation with ``aborted`` flag and ``job_id``.

        Raises:
            NotFoundException: If the job does not exist or is not currently active.
        """
        ok = await wrk.abort_job(job_id)
        if not ok:
            raise NotFoundException(detail=f"Job {job_id!r} not found or not active")
        return {"aborted": True, "job_id": job_id}

    @post("/{job_id:str}/requeue")
    async def requeue_job(self, wrk: Wrk, job_id: str) -> dict[str, Any]:
        """Re-queue a failed or cancelled job for another attempt.

        Args:
            wrk: Wrk application instance.
            job_id: Unique job identifier.

        Returns:
            Confirmation with ``requeued`` flag and ``job_id``.

        Raises:
            NotFoundException: If the job does not exist or cannot be re-queued.
        """
        ok = await wrk.requeue_job(job_id)
        if not ok:
            raise NotFoundException(detail=f"Job {job_id!r} not found or not re-queueable")
        return {"requeued": True, "job_id": job_id}

    @delete("/{job_id:str}", status_code=204)
    async def delete_job(self, wrk: Wrk, job_id: str) -> None:
        """Permanently delete a job record.

        Args:
            wrk: Wrk application instance.
            job_id: Unique job identifier.
        """
        await wrk.delete_job(job_id)

    @post("/requeue")
    async def requeue_jobs(self, wrk: Wrk, data: BulkRequeueRequest) -> dict[str, Any]:
        """Bulk re-queue failed or cancelled jobs matching the given criteria.

        Args:
            wrk: Wrk application instance.
            data: Filters specifying which jobs to re-queue (queue name, function name).

        Returns:
            Number of jobs re-queued.
        """
        requeued = await wrk.bulk_requeue_jobs(queue=data.queue, function_name=data.function_name)
        return {"requeued": requeued}

    @post("/cancel")
    async def cancel_jobs(self, wrk: Wrk, data: BulkCancelRequest) -> dict[str, Any]:
        """Bulk cancel pending jobs in a queue.

        Args:
            wrk: Wrk application instance.
            data: Filters specifying which queue to cancel jobs from.

        Returns:
            Number of jobs cancelled.
        """
        cancelled = await wrk.bulk_cancel_jobs(queue=data.queue)
        return {"cancelled": cancelled}

    @post("/purge")
    async def purge_jobs(self, wrk: Wrk, data: PurgeRequest) -> dict[str, Any]:
        """Delete terminal jobs older than a given age.

        Only jobs in ``complete``, ``failed``, ``aborted``, or ``cancelled`` status
        can be purged.

        Args:
            wrk: Wrk application instance.
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
        purged = await wrk.purge_jobs(statuses=data.statuses, older_than_days=data.older_than_days)
        return {"purged": purged}


# ---------------------------------------------------------------------------
# Workers
# ---------------------------------------------------------------------------


class WorkerController(Controller):
    path = "/workers"

    @get("")
    async def list_workers(self, wrk: Wrk) -> list[WorkerResponse]:
        """List all registered workers.

        Args:
            wrk: Wrk application instance.

        Returns:
            List of worker records including last heartbeat and status.
        """
        rows = await wrk.list_workers()
        return [WorkerResponse.from_row(r) for r in rows]

    @get("/{worker_id:str}")
    async def get_worker(self, wrk: Wrk, worker_id: str) -> WorkerResponse:
        """Retrieve a single worker by ID.

        Args:
            wrk: Wrk application instance.
            worker_id: Unique worker identifier.

        Returns:
            The requested worker record.

        Raises:
            NotFoundException: If no worker with the given ID exists.
        """
        row = await wrk.get_worker(worker_id)
        if row is None:
            raise NotFoundException(detail=f"Worker {worker_id!r} not found")
        return WorkerResponse.from_row(row)

    @get("/{worker_id:str}/jobs")
    async def list_worker_jobs(
        self,
        wrk: Wrk,
        worker_id: str,
        limit: Annotated[int, Parameter(query="limit", ge=1, le=500)] = 50,
        offset: Annotated[int, Parameter(query="offset", ge=0)] = 0,
    ) -> list[JobResponse]:
        """List jobs currently claimed by a worker.

        Args:
            wrk: Wrk application instance.
            worker_id: Unique worker identifier.
            limit: Maximum number of results to return (1–500).
            offset: Number of results to skip for pagination.

        Returns:
            List of jobs claimed by the specified worker.
        """
        jobs = await wrk.list_worker_jobs(worker_id=worker_id, limit=limit, offset=offset)
        return [JobResponse.from_job(j) for j in jobs]


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------


class StatsController(Controller):
    path = "/stats"

    @get("")
    async def get_stats(self, wrk: Wrk) -> StatsResponse:
        """Return aggregate queue and worker statistics.

        Args:
            wrk: Wrk application instance.

        Returns:
            Summary of all queues, total job count, and number of online workers.
        """
        queue_rows, total, workers_online = await wrk.get_queue_stats()
        return StatsResponse(
            queues=[QueueStats.from_row(r) for r in queue_rows],
            total_jobs=total,
            workers_online=workers_online,
        )

    @get("/throughput")
    async def get_throughput_history(
        self,
        wrk: Wrk,
        minutes: Annotated[int, Parameter(query="minutes", ge=1, le=10080)] = 1440,
    ) -> list[WorkerThroughputPoint]:
        """Return worker throughput over a time window.

        Args:
            wrk: Wrk application instance.
            minutes: Time window in minutes to look back (1–10080, default 1440 = 24 h).

        Returns:
            Time-series data points of jobs completed per interval.
        """
        rows = await wrk.get_throughput_history(minutes)
        return [WorkerThroughputPoint.from_row(r) for r in rows]

    @get("/queue-depth")
    async def get_queue_depth_history(
        self,
        wrk: Wrk,
        minutes: Annotated[int, Parameter(query="minutes", ge=1, le=10080)] = 1440,
    ) -> list[QueueDepthPoint]:
        """Return queue depth over a time window.

        Args:
            wrk: Wrk application instance.
            minutes: Time window in minutes to look back (1–10080, default 1440 = 24 h).

        Returns:
            Time-series data points of pending job count per interval.
        """
        rows = await wrk.get_queue_depth_history(minutes)
        return [QueueDepthPoint.from_row(r) for r in rows]


# ---------------------------------------------------------------------------
# Cron
# ---------------------------------------------------------------------------


class CronController(Controller):
    path = "/cron"

    @get("")
    async def list_cron_jobs(self, wrk: Wrk) -> list[CronJobStats]:
        """List all registered cron jobs and their last-run statistics.

        Args:
            wrk: Wrk application instance.

        Returns:
            List of cron job entries with name, schedule, and execution stats.
        """
        rows = await wrk.list_cron_stats()
        return [CronJobStats.from_row(r) for r in rows]

    @post("/{name:str}/trigger", status_code=201)
    async def trigger_cron_job(self, wrk: Wrk, name: str) -> JobResponse:
        """Manually trigger a cron job by name, bypassing its schedule.

        Args:
            wrk: Wrk application instance.
            name: Registered cron job name.

        Returns:
            The enqueued job created for this trigger.

        Raises:
            NotFoundException: If no cron job with the given name is registered.
        """
        job = await wrk.trigger_cron_job(name)
        if job is None:
            raise NotFoundException(detail=f"Cron job {name!r} not found")
        return JobResponse.from_job(job)


# ---------------------------------------------------------------------------
# Server
# ---------------------------------------------------------------------------


class ServerController(Controller):
    path = "/server"

    @get("")
    async def get_server_info(self, wrk: Wrk) -> ServerInfo:
        """Return Postgres server information and table sizes.

        Args:
            wrk: Wrk application instance.

        Returns:
            Postgres version, total database size, and per-table row counts and sizes.
        """
        pg_version, db_size_bytes, table_rows = await wrk.get_server_info()
        tables = [TableInfo(name=r["name"], size_bytes=r["size_bytes"], row_count=r["row_count"]) for r in table_rows]
        return ServerInfo(pg_version=pg_version, db_size_bytes=db_size_bytes, tables=tables)

    @post("/sweep")
    async def run_sweep(self, wrk: Wrk) -> dict[str, Any]:
        """Sweep stale worker claims and release orphaned jobs.

        Args:
            wrk: Wrk application instance.

        Returns:
            Count and list of job IDs that were swept.
        """
        swept = await wrk.sweep()
        return {"swept": len(swept), "job_ids": swept}

    @post("/reschedule-stuck")
    async def reschedule_stuck(self, wrk: Wrk) -> dict[str, Any]:
        """Reschedule jobs that have been stuck in running state past their timeout.

        Args:
            wrk: Wrk application instance.

        Returns:
            Number of jobs rescheduled.
        """
        rescheduled = await wrk.reschedule_stuck()
        return {"rescheduled": rescheduled}

    @post("/vacuum")
    async def vacuum_tables(self, wrk: Wrk) -> dict[str, Any]:
        """Run VACUUM ANALYZE on all wrk tables.

        Args:
            wrk: Wrk application instance.

        Returns:
            Confirmation that vacuum completed.
        """
        await wrk.vacuum()
        return {"vacuumed": True}

    @post("/truncate")
    async def truncate_tables(self, wrk: Wrk) -> dict[str, Any]:
        """Truncate all wrk tables, removing all jobs and worker records.

        Args:
            wrk: Wrk application instance.

        Returns:
            Confirmation that truncation completed.
        """
        await wrk.truncate()
        return {"truncated": True}


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = Router(
    path="/api",
    route_handlers=[
        JobController,
        WorkerController,
        StatsController,
        CronController,
        ServerController,
    ],
)
