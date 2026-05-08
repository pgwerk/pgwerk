"""Response models and request bodies for the wrk REST API."""

from __future__ import annotations

import dataclasses

from typing import TYPE_CHECKING
from typing import Any
from datetime import datetime


if TYPE_CHECKING:
    from ..schemas import Job
    from ..schemas import JobExecution
    from ..schemas import Schedule


@dataclasses.dataclass
class JobResponse:
    """Serialisable view of a job row returned by the API.

    Attributes:
        id: UUID of the job.
        function: Dotted import path of the job handler.
        queue: Queue the job belongs to.
        status: Current lifecycle state as a plain string.
        priority: Dequeue priority.
        attempts: Number of execution attempts so far.
        max_attempts: Maximum total attempts.
        scheduled_at: Earliest time the job may be dequeued.
        enqueued_at: Wall-clock insertion time.
        key: Optional deduplication key.
        group_key: Optional group concurrency key.
        error: Error message or traceback from the last failed attempt.
        timeout_secs: Hard per-attempt timeout in seconds.
        heartbeat_secs: Maximum heartbeat gap before the job is considered lost.
        started_at: When the most recent attempt began.
        completed_at: When the job reached a terminal state.
        worker_id: UUID of the worker currently or last executing this job.
        meta: User-supplied metadata.
    """

    id: str
    function: str
    queue: str
    status: str
    priority: int
    attempts: int
    max_attempts: int
    scheduled_at: datetime
    enqueued_at: datetime
    key: str | None = None
    group_key: str | None = None
    error: str | None = None
    timeout_secs: int | None = None
    heartbeat_secs: int | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    worker_id: str | None = None
    meta: dict[str, Any] | None = None

    @classmethod
    def from_job(cls, job: "Job") -> "JobResponse":
        """Construct a JobResponse from a :class:`~wrk.schemas.Job` instance.

        Args:
            job: The source Job domain object.

        Returns:
            A JobResponse suitable for JSON serialisation.
        """
        return cls(
            id=job.id,
            function=job.function,
            queue=job.queue,
            status=str(job.status.value),
            priority=job.priority,
            attempts=job.attempts,
            max_attempts=job.max_attempts,
            scheduled_at=job.scheduled_at,
            enqueued_at=job.enqueued_at,
            key=job.key,
            group_key=job.group_key,
            error=job.error,
            timeout_secs=job.timeout_secs,
            heartbeat_secs=job.heartbeat_secs,
            started_at=job.started_at,
            completed_at=job.completed_at,
            worker_id=job.worker_id,
            meta=job.meta,
        )


@dataclasses.dataclass
class ExecutionResponse:
    """Serialisable view of a single job execution attempt.

    Attributes:
        id: UUID of the execution row.
        job_id: UUID of the parent job.
        attempt: 1-based attempt number.
        status: Current or final state of this execution.
        worker_id: UUID of the worker that ran this attempt.
        error: Error message or traceback if the attempt failed.
        result: Decoded return value if the execution succeeded.
        started_at: When this attempt began.
        completed_at: When this attempt reached a terminal state.
    """

    id: str
    job_id: str
    attempt: int
    status: str
    worker_id: str | None = None
    error: str | None = None
    result: Any = None
    started_at: datetime | None = None
    completed_at: datetime | None = None

    @classmethod
    def from_execution(cls, e: "JobExecution") -> "ExecutionResponse":
        """Construct an ExecutionResponse from a :class:`~wrk.schemas.JobExecution`.

        Args:
            e: The source JobExecution domain object.

        Returns:
            An ExecutionResponse suitable for JSON serialisation.
        """
        return cls(
            id=e.id,
            job_id=e.job_id,
            attempt=e.attempt,
            status=str(e.status.value),
            worker_id=e.worker_id,
            error=e.error,
            result=e.result,
            started_at=e.started_at,
            completed_at=e.completed_at,
        )


@dataclasses.dataclass
class WorkerResponse:
    """Serialisable view of a registered worker row.

    Attributes:
        id: UUID of the worker.
        name: Human-readable ``hostname.pid`` identifier.
        queue: Primary queue the worker is consuming.
        status: Current worker status (e.g. ``"idle"``, ``"busy"``).
        role: Process role — ``"worker"`` or ``"scheduler"``.
        metadata: JSON blob with runtime info (PID, concurrency, strategy, etc.).
        heartbeat_at: Timestamp of the last heartbeat.
        started_at: When the worker registered.
        expires_at: Time after which the worker row may be cleaned up.
    """

    id: str
    name: str
    queue: str
    status: str
    role: str = "worker"
    metadata: dict[str, Any] | None = None
    heartbeat_at: datetime | None = None
    started_at: datetime | None = None
    expires_at: datetime | None = None

    @classmethod
    def from_row(cls, r: dict[str, Any]) -> "WorkerResponse":
        """Construct a WorkerResponse from a raw database row dict.

        Args:
            r: Database row as a mapping of column names to values.

        Returns:
            A WorkerResponse suitable for JSON serialisation.
        """
        return cls(
            id=r["id"],
            name=r["name"],
            queue=r["queue"],
            status=r["status"],
            role=r.get("role", "worker"),
            metadata=r["metadata"],
            heartbeat_at=r["heartbeat_at"],
            started_at=r["started_at"],
            expires_at=r["expires_at"],
        )


@dataclasses.dataclass
class QueueStats:
    """Per-queue job counts broken down by status.

    Attributes:
        queue: Name of the queue.
        scheduled: Jobs waiting for their ``scheduled_at`` to arrive.
        queued: Jobs ready for immediate dequeue.
        active: Jobs currently being executed.
        waiting: Jobs blocked on dependencies.
        failed: Jobs that exhausted all retry attempts.
        complete: Successfully completed jobs.
        aborted: Jobs that were manually aborted.
    """

    queue: str
    scheduled: int
    queued: int
    active: int
    waiting: int
    failed: int
    complete: int
    aborted: int

    @classmethod
    def from_row(cls, r: dict[str, Any]) -> "QueueStats":
        """Construct QueueStats from a raw database row dict.

        Args:
            r: Database row as a mapping of column names to integer counts.

        Returns:
            A QueueStats instance.
        """
        return cls(
            queue=r["queue"],
            scheduled=r["scheduled"],
            queued=r["queued"],
            active=r["active"],
            waiting=r["waiting"],
            failed=r["failed"],
            complete=r["complete"],
            aborted=r["aborted"],
        )


@dataclasses.dataclass
class StatsResponse:
    """Aggregate statistics response returned by the stats endpoint.

    Attributes:
        queues: Per-queue job counts.
        total_jobs: Total number of job rows across all queues.
        workers_online: Number of workers with a recent heartbeat.
    """

    queues: list[QueueStats]
    total_jobs: int
    workers_online: int


@dataclasses.dataclass
class WorkerThroughputPoint:
    """A single data point in a worker throughput time series.

    Attributes:
        time: Truncated timestamp for this bucket (e.g. per-minute).
        worker_id: UUID of the worker, or ``None`` for an aggregate row.
        worker_name: Human-readable worker name, or ``None`` for an aggregate row.
        count: Number of jobs completed in this bucket.
    """

    time: datetime
    worker_id: str | None
    worker_name: str | None
    count: int

    @classmethod
    def from_row(cls, r: dict[str, Any]) -> "WorkerThroughputPoint":
        """Construct a WorkerThroughputPoint from a raw database row dict.

        Args:
            r: Database row with keys ``time``, ``worker_id``, ``worker_name``,
                and ``count``.

        Returns:
            A WorkerThroughputPoint instance.
        """
        return cls(
            time=r["time"],
            worker_id=r["worker_id"],
            worker_name=r["worker_name"],
            count=r["count"],
        )


@dataclasses.dataclass
class QueueDepthPoint:
    """A single data point in a queue depth time series.

    Attributes:
        time: Truncated timestamp for this bucket (e.g. per-minute).
        queued: Number of jobs in ``queued`` status at this point in time.
        active: Number of jobs in ``active`` status at this point in time.
    """

    time: datetime
    queued: int
    active: int

    @classmethod
    def from_row(cls, r: dict[str, Any]) -> "QueueDepthPoint":
        """Construct a QueueDepthPoint from a raw database row dict.

        Args:
            r: Database row with keys ``time``, ``queued``, and ``active``.

        Returns:
            A QueueDepthPoint instance.
        """
        return cls(
            time=r["time"],
            queued=r["queued"],
            active=r["active"],
        )


@dataclasses.dataclass
class TableInfo:
    """Size and row-count metadata for a single wrk database table.

    Attributes:
        name: Fully-qualified table name.
        size_bytes: Total on-disk size of the table in bytes.
        row_count: Estimated number of rows (from ``pg_class``).
    """

    name: str
    size_bytes: int
    row_count: int


@dataclasses.dataclass
class ServerInfo:
    """Server-level information returned by the server info endpoint.

    Attributes:
        pg_version: Postgres server version string.
        db_size_bytes: Total size of the entire Postgres database in bytes.
        pgwerk_size_bytes: Combined size of all wrk tables in bytes.
        schema: Postgres schema that qualifies wrk table names, or ``None`` for the default search path.
        tables: Per-table size and row-count metadata.
    """

    pg_version: str
    db_size_bytes: int
    pgwerk_size_bytes: int
    schema: str | None
    tables: list[TableInfo]


@dataclasses.dataclass
class PurgeRequest:
    """Request body for the bulk job purge endpoint.

    Attributes:
        statuses: Job status values to purge (defaults to ``complete``,
            ``failed``, and ``aborted``).
        older_than_days: Only delete jobs that completed more than this many
            days ago.
    """

    statuses: list[str] = dataclasses.field(default_factory=lambda: ["complete", "failed", "aborted"])
    older_than_days: int = 30


@dataclasses.dataclass
class BulkRequeueRequest:
    """Request body for the bulk requeue endpoint.

    Attributes:
        queue: Limit requeue to this queue; ``None`` targets all queues.
        function_name: Limit requeue to jobs with this function name.
    """

    queue: str | None = None
    function_name: str | None = None


@dataclasses.dataclass
class BulkCancelRequest:
    """Request body for the bulk cancel endpoint.

    Attributes:
        queue: Limit cancellation to this queue; ``None`` targets all queues.
    """

    queue: str | None = None


@dataclasses.dataclass
class EnqueueRequest:
    """Request body for the single-job enqueue endpoint.

    Attributes:
        function: Dotted import path of the handler to invoke.
        queue: Target queue name.
        priority: Dequeue priority — higher values are processed first.
        args: Positional arguments forwarded to the handler.
        kwargs: Keyword arguments forwarded to the handler.
        key: Optional deduplication key; duplicate enqueues are silently dropped.
        delay: Seconds to wait before the job becomes eligible for dequeue.
        scheduled_at: Absolute time at which the job becomes eligible; overrides
            ``delay`` when both are set.
        max_attempts: Maximum total execution attempts.
        timeout_secs: Hard per-attempt wall-clock timeout in seconds.
        meta: Arbitrary user-supplied metadata stored with the job.
        schedule_name: Name of the Schedule this enqueue is associated with.
    """

    function: str
    queue: str = "default"
    priority: int = 0
    args: list[Any] = dataclasses.field(default_factory=list)
    kwargs: dict[str, Any] = dataclasses.field(default_factory=dict)
    key: str | None = None
    delay: int | None = None
    scheduled_at: datetime | None = None
    max_attempts: int = 1
    timeout_secs: int | None = None
    meta: dict[str, Any] | None = None
    schedule_name: str | None = None


@dataclasses.dataclass
class ScheduleResponse:
    """A registered schedule row from ``_pgwerk_schedules``.

    Attributes:
        name: Schedule name (primary key).
        function: Dotted import path of the handler.
        queue: Queue the schedule enqueues into.
        args: Positional arguments forwarded to the handler on each run.
        kwargs: Keyword arguments forwarded to the handler on each run.
        interval_secs: Seconds between runs (mutually exclusive with ``cron``).
        cron: Cron expression (mutually exclusive with ``interval_secs``).
        timeout_secs: Per-run wall-clock timeout in seconds.
        result_ttl: Seconds to retain successful job rows.
        failure_ttl: Seconds to retain failed job rows.
        meta: Arbitrary metadata attached to each enqueued job.
        paused: Whether the schedule is currently paused.
        next_run_at: Next wall-clock time this schedule is due.
        last_run_at: Most recent wall-clock time an enqueue fired.
        last_registered_at: When this row was last bumped by ``register()``.
        created_at: Row creation time.
    """

    name: str
    function: str
    queue: str
    args: list[Any]
    kwargs: dict[str, Any]
    interval_secs: int | None
    cron: str | None
    timeout_secs: int | None
    result_ttl: int | None
    failure_ttl: int | None
    meta: dict[str, Any] | None
    paused: bool
    next_run_at: datetime | None
    last_run_at: datetime | None
    last_registered_at: datetime | None
    created_at: datetime | None

    @classmethod
    def from_schedule(cls, s: "Schedule") -> "ScheduleResponse":
        return cls(
            name=s.name,
            function=s.function,
            queue=s.queue,
            args=list(s.args) if s.args else [],
            kwargs=dict(s.kwargs) if s.kwargs else {},
            interval_secs=s.interval_secs,
            cron=s.cron,
            timeout_secs=s.timeout_secs,
            result_ttl=s.result_ttl,
            failure_ttl=s.failure_ttl,
            meta=s.meta,
            paused=s.paused,
            next_run_at=s.next_run_at,
            last_run_at=s.last_run_at,
            last_registered_at=s.last_registered_at,
            created_at=s.created_at,
        )


@dataclasses.dataclass
class ScheduleStats:
    """Aggregated job statistics for a single schedule.

    Attributes:
        name: Schedule name (``module.qualname`` by default).
        function: Dotted import path of the handler.
        queue: Queue the schedule enqueues into.
        total_runs: Total number of times the schedule has been enqueued.
        failed_runs: Number of runs that ended in a failed or aborted state.
        last_status: Status of the most recent execution.
        last_enqueued_at: Timestamp of the most recent enqueue.
        last_completed_at: Timestamp of the most recent completion.
    """

    name: str
    function: str
    queue: str
    total_runs: int
    failed_runs: int
    last_status: str | None = None
    last_enqueued_at: datetime | None = None
    last_completed_at: datetime | None = None

    @classmethod
    def from_row(cls, r: dict[str, Any]) -> "ScheduleStats":
        """Construct ScheduleStats from a raw database row dict.

        Args:
            r: Database row with aggregated schedule statistics columns.

        Returns:
            A ScheduleStats instance.
        """
        return cls(
            name=r["schedule_name"],
            function=r["function"],
            queue=r["queue"],
            total_runs=r["total_runs"],
            failed_runs=r["failed_runs"],
            last_status=r["last_status"],
            last_enqueued_at=r["last_enqueued_at"],
            last_completed_at=r["last_completed_at"],
        )


@dataclasses.dataclass
class CreateScheduleBody:
    """Request body for POST /schedules.

    Attributes:
        name: Unique schedule name (primary key).
        function: Dotted import path of the handler to run on each tick.
        queue: Queue the enqueued jobs land on.
        cron: Cron expression (mutually exclusive with ``interval_secs``).
        interval_secs: Seconds between runs (mutually exclusive with ``cron``).
        kwargs: Keyword arguments forwarded to the handler on each run.
    """

    name: str
    function: str
    queue: str = "default"
    cron: str | None = None
    interval_secs: int | None = None
    kwargs: dict[str, Any] = dataclasses.field(default_factory=dict)
