"""Job specification and execution context types."""

from __future__ import annotations

import dataclasses

from typing import TYPE_CHECKING
from typing import Any
from typing import Callable
from typing import Optional
from typing import LiteralString
from typing import cast
from datetime import datetime
from datetime import timezone
from datetime import timedelta
from dataclasses import field
from dataclasses import dataclass

from .commons import JobStatus
from .commons import ExecutionStatus
from .serializers import Serializer
from .serializers import decode


if TYPE_CHECKING:
    from .app import Werk
    from .worker.base import BaseWorker


@dataclasses.dataclass
class Job:
    """A single job row as returned from the database.

    Attributes:
        id: UUID primary key.
        function: Dotted import path of the handler (e.g. ``myapp.tasks.send_email``).
        queue: Name of the queue this job belongs to.
        status: Current lifecycle state.
        priority: Dequeue priority — higher values are processed first.
        attempts: Number of execution attempts made so far.
        max_attempts: Maximum total attempts before the job is failed.
        scheduled_at: Earliest time the job may be dequeued.
        enqueued_at: Wall-clock time the job was inserted.
        key: Optional deduplication key; duplicate enqueues are silently dropped.
        group_key: Optional group key for concurrency limits within a group.
        payload: Decoded ``{"args": [...], "kwargs": {...}}`` dict.
        result: Decoded return value from the last successful execution.
        error: Traceback or error message from the last failed execution.
        timeout_secs: Hard wall-clock timeout per attempt in seconds.
        heartbeat_secs: Maximum gap between heartbeats before the job is considered lost.
        started_at: Time the most recent execution began.
        completed_at: Time the job reached a terminal state.
        touched_at: Time of the most recent heartbeat.
        expires_at: Time after which the job row may be garbage-collected.
        worker_id: UUID of the worker currently executing this job.
        meta: Arbitrary user-supplied metadata stored alongside the job.
        result_ttl: Seconds to retain the job row after success.
        failure_ttl: Seconds to retain the job row after failure.
        ttl: Maximum seconds the job may wait in the queue before being dropped.
        on_success: Dotted path of the success callback.
        on_failure: Dotted path of the failure callback.
        on_stopped: Dotted path of the abort/stop callback.
        retry_intervals: Per-attempt retry delays in seconds.
        repeat_remaining: Number of additional repeats still pending.
        repeat_interval_secs: Uniform delay between repeats in seconds.
        repeat_intervals: Per-run repeat delays in seconds.
        cron_name: Name of the CronJob that created this job, if any.
        failure_mode: What to do on terminal failure — ``"hold"`` or ``"delete"``.
    """

    id: str
    function: str
    queue: str
    status: JobStatus
    priority: int
    attempts: int
    max_attempts: int
    scheduled_at: datetime
    enqueued_at: datetime
    key: str | None = None
    group_key: str | None = None
    payload: dict[str, Any] | None = None
    result: Any = None
    error: str | None = None
    timeout_secs: int | None = None
    heartbeat_secs: int | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    touched_at: datetime | None = None
    expires_at: datetime | None = None
    worker_id: str | None = None
    meta: dict[str, Any] | None = None
    result_ttl: int | None = None
    failure_ttl: int | None = None
    ttl: int | None = None
    on_success: str | None = None
    on_failure: str | None = None
    on_stopped: str | None = None
    on_success_timeout: int | None = None
    on_failure_timeout: int | None = None
    on_stopped_timeout: int | None = None
    retry_intervals: list[int] | None = None
    repeat_remaining: int | None = None
    repeat_interval_secs: int | None = None
    repeat_intervals: list[int] | None = None
    cron_name: str | None = None
    failure_mode: str = "hold"

    def __post_init__(self) -> None:
        """Coerce a raw status string to a ``JobStatus`` enum value."""
        if isinstance(self.status, str):
            self.status = JobStatus(self.status)

    @classmethod
    def from_row(cls, row: dict, serializer: Serializer) -> Job:
        """Construct a Job from a raw database row dict.

        Args:
            row: Database row as a mapping of column names to raw values.
            serializer: Serializer used to decode JSONB payloads.

        Returns:
            A fully decoded Job instance.
        """
        d = dict(row)
        d["id"] = str(d["id"])
        if d.get("worker_id") is not None:
            d["worker_id"] = str(d["worker_id"])
        d["payload"] = decode(serializer, d.get("payload"))
        d["result"] = decode(serializer, d.get("result"))
        d["meta"] = decode(serializer, d.get("meta"))
        d["retry_intervals"] = decode(serializer, d.get("retry_intervals"))
        d["repeat_intervals"] = decode(serializer, d.get("repeat_intervals"))
        return cls(**d)


@dataclasses.dataclass
class JobInsert:
    """Pre-processed values ready for INSERT into the jobs table."""

    function: str
    queue: str
    status: str
    priority: int
    max_attempts: int
    failure_mode: str
    dep_ids: list[tuple[str, bool]] = dataclasses.field(default_factory=list)
    key: str | None = None
    group_key: str | None = None
    payload: str | None = None
    timeout_secs: int | None = None
    heartbeat_secs: int | None = None
    scheduled_at: datetime | None = None
    expires_at: datetime | None = None
    meta: str | None = None
    result_ttl: int | None = None
    failure_ttl: int | None = None
    ttl: int | None = None
    on_success: str | None = None
    on_failure: str | None = None
    on_stopped: str | None = None
    on_success_timeout: int | None = None
    on_failure_timeout: int | None = None
    on_stopped_timeout: int | None = None
    retry_intervals: str | None = None
    repeat_remaining: int | None = None
    repeat_interval_secs: int | None = None
    repeat_intervals: str | None = None
    cron_name: str | None = None

    def as_params(self) -> dict[str, Any]:
        """Return a dict of all fields except ``dep_ids`` for use as DB query parameters.

        Returns:
            Mapping of field name to value, omitting the ``dep_ids`` list.
        """
        return {f.name: getattr(self, f.name) for f in dataclasses.fields(self) if f.name != "dep_ids"}


@dataclasses.dataclass
class JobExecution:
    """A single execution attempt recorded for a job.

    Attributes:
        id: UUID primary key for this execution row.
        job_id: UUID of the parent job.
        attempt: 1-based attempt number.
        status: Current or final state of this execution.
        worker_id: UUID of the worker that ran this attempt.
        error: Traceback or error message if the execution failed.
        result: Decoded return value if the execution succeeded.
        started_at: When this attempt began.
        completed_at: When this attempt reached a terminal state.
    """

    id: str
    job_id: str
    attempt: int
    status: ExecutionStatus
    worker_id: str | None = None
    error: str | None = None
    result: Any = None
    started_at: datetime | None = None
    completed_at: datetime | None = None

    def __post_init__(self) -> None:
        """Coerce a raw status string to an ``ExecutionStatus`` enum value."""
        if isinstance(self.status, str):
            self.status = ExecutionStatus(self.status)

    @classmethod
    def from_row(cls, row: dict, serializer: Serializer) -> JobExecution:
        """Construct a JobExecution from a raw database row dict.

        Args:
            row: Database row as a mapping of column names to raw values.
            serializer: Serializer used to decode the JSONB result field.

        Returns:
            A fully decoded JobExecution instance.
        """
        worker_id = row["worker_id"]
        return cls(
            **{
                **row,
                "id": str(row["id"]),
                "job_id": str(row["job_id"]),
                "worker_id": str(worker_id) if worker_id else None,
                "result": decode(serializer, row["result"]),
            }
        )


@dataclass
class CronJob:
    """A function registered to run on a fixed interval or cron schedule."""

    func: Callable
    queue: str = "default"
    args: tuple = field(default_factory=tuple)
    kwargs: dict = field(default_factory=dict)
    interval: int | None = None  # seconds between runs
    cron: str | None = None  # cron expression, e.g. "*/5 * * * *"
    timeout: int | None = None
    result_ttl: int | None = None
    failure_ttl: int | None = None
    meta: dict[str, Any] | None = None
    name: str = field(default=None)  # type: ignore[assignment]  # always set by __post_init__
    paused: bool = False

    next_run_at: datetime | None = field(default=None, init=False, repr=False)
    last_run_at: datetime | None = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        """Validate configuration and derive the initial ``next_run_at`` for cron jobs.

        Raises:
            ValueError: If both ``interval`` and ``cron`` are set, or neither is set.
            ImportError: If ``cron`` is set but ``croniter`` is not installed.
        """
        if self.interval and self.cron:
            raise ValueError("Specify either interval or cron, not both")
        if not self.interval and not self.cron:
            raise ValueError("Must specify either interval or cron")
        if self.name is None:
            self.name = f"{self.func.__module__}.{self.func.__qualname__}"
        if self.cron:
            self._advance_cron()

    def _advance_cron(self) -> None:
        try:
            from croniter import croniter
        except ImportError:
            raise ImportError("croniter is required for cron expressions: pip install croniter")
        assert self.cron is not None
        base = self.last_run_at or datetime.now(timezone.utc)
        self.next_run_at = croniter(self.cron, base).get_next(datetime)

    def should_run(self) -> bool:
        """Return True if this job is due to run now.

        Returns:
            ``False`` if the job is paused or its next run time has not arrived.
        """
        if self.paused:
            return False
        now = datetime.now(timezone.utc)
        if self.interval:
            if self.last_run_at is None:
                return True
            return now >= self.last_run_at + timedelta(seconds=self.interval)
        if self.next_run_at:
            nxt = self.next_run_at
            if nxt.tzinfo is None:
                nxt = nxt.replace(tzinfo=timezone.utc)
            return now >= nxt
        return False

    def mark_enqueued(self) -> None:
        """Record that the job was just enqueued and advance the schedule."""
        self.last_run_at = datetime.now(timezone.utc)
        if self.cron:
            self._advance_cron()

    def seconds_until_next(self) -> float:
        """Return the number of seconds until this job should next run.

        Returns:
            Seconds until the next scheduled run, or ``0.0`` if overdue.
            Falls back to ``60.0`` when the schedule cannot be determined.
        """
        now = datetime.now(timezone.utc)
        if self.interval:
            if self.last_run_at is None:
                return 0.0
            nxt = self.last_run_at + timedelta(seconds=self.interval)
            return max(0.0, (nxt - now).total_seconds())
        if self.next_run_at:
            nxt = self.next_run_at
            if nxt.tzinfo is None:
                nxt = nxt.replace(tzinfo=timezone.utc)
            return max(0.0, (nxt - now).total_seconds())
        return 60.0


@dataclasses.dataclass
class Context:
    """Execution context injected as the first argument to job handlers."""

    app: "Werk"
    worker: "BaseWorker"
    job: "Job"
    exception: Optional[Exception] = None


@dataclasses.dataclass
class Dependency:
    """Declare that a job depends on another job.

    When ``allow_failure`` is True the dependent job will still be enqueued
    even if this dependency fails or is aborted.
    """

    job: "str | Job"
    allow_failure: bool = False

    @property
    def job_id(self) -> str:
        """Return the job UUID string for this dependency.

        Returns:
            The ``.id`` attribute if ``job`` is a Job object, else ``str(job)``.
        """
        return self.job.id if hasattr(self.job, "id") else str(self.job)


@dataclasses.dataclass
class Callback:
    """A callback function with an optional per-callback timeout."""

    func: Callable | str
    timeout: int | None = None

    def path(self) -> str:
        """Return the dotted import path for this callback.

        Returns:
            ``module.qualname`` if ``func`` is a callable, or the string as-is.
        """
        if callable(self.func):
            return f"{self.func.__module__}.{self.func.__qualname__}"
        return str(self.func)


@dataclasses.dataclass
class Repeat:
    """Repeat a job N additional times after the first run.

    ``intervals`` (list) overrides the uniform ``interval`` and provides a
    per-run delay. When the list is shorter than ``times`` the last value is
    reused for subsequent runs.
    """

    times: int
    interval: int = 0
    intervals: list[int] | None = None

    def __post_init__(self) -> None:
        """Validate that ``times`` and ``interval`` are non-negative.

        Raises:
            ValueError: If ``times`` is less than 1 or ``interval`` is negative.
        """
        if self.times < 1:
            raise ValueError("Repeat.times must be >= 1")
        if self.interval < 0:
            raise ValueError("Repeat.interval must be >= 0")

    def get_interval(self, run: int) -> int:
        """Return the delay in seconds before the given run.

        Args:
            run: Zero-based run index (0 = first repeat after the initial execution).

        Returns:
            Delay in seconds. Uses the per-run list when provided, clamping to the
            last value when the index exceeds the list length.
        """
        if self.intervals:
            idx = min(run, len(self.intervals) - 1)
            return self.intervals[idx]
        return self.interval


@dataclasses.dataclass
class Retry:
    """Retry configuration for a job.

    ``max`` is the total number of attempts (including the first).
    ``intervals`` is either a uniform delay in seconds or a list of per-attempt
    delays. When the list is exhausted the last value is reused.
    """

    max: int
    intervals: list[int] | int = 0

    def __post_init__(self) -> None:
        """Validate that ``max`` is at least 1 and ``intervals`` is non-negative.

        Raises:
            ValueError: If ``max`` is less than 1, or a uniform interval is negative.
        """
        if self.max < 1:
            raise ValueError("Retry.max must be >= 1")
        if isinstance(self.intervals, int) and self.intervals < 0:
            raise ValueError("Retry.intervals must be >= 0")

    def get_interval(self, attempt: int) -> int:
        """Return the retry delay in seconds for the given attempt.

        Args:
            attempt: 1-based attempt number (1 = first retry after initial failure).

        Returns:
            Delay in seconds before the next attempt. Uses the per-attempt list
            when provided, clamping to the last value when the index is out of range.
        """
        if isinstance(self.intervals, list):
            idx = min(attempt - 1, len(self.intervals) - 1)
            return self.intervals[idx] if self.intervals else 0
        return self.intervals

    def to_intervals_list(self) -> list[int] | None:
        """Return ``intervals`` as a list, or ``None`` for a uniform int interval.

        Returns:
            The list of per-attempt delays, or ``None`` if a single uniform delay
            was configured (which is stored as a 1-element list in the DB).
        """
        if isinstance(self.intervals, list):
            return self.intervals
        return None


@dataclasses.dataclass
class EnqueueParams:
    """Specification for a single job in ``enqueue_many``."""

    func: Callable
    args: tuple = ()
    kwargs: dict = dataclasses.field(default_factory=dict)
    queue: str = "default"
    priority: int = 0
    delay: int | None = None
    at: datetime | None = None
    retry: Retry | int = 1
    timeout: int | None = None
    heartbeat: int | None = None
    key: str | None = None
    group: str | None = None
    meta: dict[str, Any] | None = None
    result_ttl: int | None = None
    failure_ttl: int | None = None
    ttl: int | None = None
    on_success: Callback | Callable | str | None = None
    on_failure: Callback | Callable | str | None = None
    on_stopped: Callback | Callable | str | None = None
    repeat: Repeat | None = None
    depends_on: "list[Dependency | str | Job] | Dependency | str | Job | None" = None
    cron_name: str | None = None
    failure_mode: str = "hold"


JOB_COLS: LiteralString = cast(
    LiteralString,
    "\n    " + ",\n    ".join(f.name for f in dataclasses.fields(Job)) + "\n",
)
