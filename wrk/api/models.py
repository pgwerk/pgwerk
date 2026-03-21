from __future__ import annotations

import dataclasses

from typing import TYPE_CHECKING
from typing import Any
from datetime import datetime


if TYPE_CHECKING:
    from ..schemas import Job
    from ..schemas import JobExecution


@dataclasses.dataclass
class JobResponse:
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
    id: str
    job_id: str
    attempt: int
    status: str
    worker_id: str | None = None
    error: str | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None

    @classmethod
    def from_execution(cls, e: "JobExecution") -> "ExecutionResponse":
        return cls(
            id=e.id,
            job_id=e.job_id,
            attempt=e.attempt,
            status=str(e.status.value),
            worker_id=e.worker_id,
            error=e.error,
            started_at=e.started_at,
            completed_at=e.completed_at,
        )


@dataclasses.dataclass
class WorkerResponse:
    id: str
    name: str
    queue: str
    status: str
    metadata: dict[str, Any] | None = None
    heartbeat_at: datetime | None = None
    started_at: datetime | None = None
    expires_at: datetime | None = None

    @classmethod
    def from_row(cls, r: dict[str, Any]) -> "WorkerResponse":
        return cls(
            id=r["id"],
            name=r["name"],
            queue=r["queue"],
            status=r["status"],
            metadata=r["metadata"],
            heartbeat_at=r["heartbeat_at"],
            started_at=r["started_at"],
            expires_at=r["expires_at"],
        )


@dataclasses.dataclass
class QueueStats:
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
    queues: list[QueueStats]
    total_jobs: int
    workers_online: int


@dataclasses.dataclass
class WorkerThroughputPoint:
    time: datetime
    worker_id: str | None
    worker_name: str | None
    count: int

    @classmethod
    def from_row(cls, r: dict[str, Any]) -> "WorkerThroughputPoint":
        return cls(
            time=r["time"],
            worker_id=r["worker_id"],
            worker_name=r["worker_name"],
            count=r["count"],
        )


@dataclasses.dataclass
class QueueDepthPoint:
    time: datetime
    queued: int
    active: int

    @classmethod
    def from_row(cls, r: dict[str, Any]) -> "QueueDepthPoint":
        return cls(
            time=r["time"],
            queued=r["queued"],
            active=r["active"],
        )


@dataclasses.dataclass
class TableInfo:
    name: str
    size_bytes: int
    row_count: int


@dataclasses.dataclass
class ServerInfo:
    pg_version: str
    db_size_bytes: int
    tables: list[TableInfo]


@dataclasses.dataclass
class PurgeRequest:
    statuses: list[str] = dataclasses.field(default_factory=lambda: ["complete", "failed", "aborted"])
    older_than_days: int = 30


@dataclasses.dataclass
class BulkRequeueRequest:
    queue: str | None = None
    function_name: str | None = None


@dataclasses.dataclass
class BulkCancelRequest:
    queue: str | None = None


@dataclasses.dataclass
class EnqueueRequest:
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
    cron_name: str | None = None


@dataclasses.dataclass
class CronJobStats:
    name: str
    function: str
    queue: str
    total_runs: int
    failed_runs: int
    last_status: str | None = None
    last_enqueued_at: datetime | None = None
    last_completed_at: datetime | None = None

    @classmethod
    def from_row(cls, r: dict[str, Any]) -> "CronJobStats":
        return cls(
            name=r["cron_name"],
            function=r["function"],
            queue=r["queue"],
            total_runs=r["total_runs"],
            failed_runs=r["failed_runs"],
            last_status=r["last_status"],
            last_enqueued_at=r["last_enqueued_at"],
            last_completed_at=r["last_completed_at"],
        )
