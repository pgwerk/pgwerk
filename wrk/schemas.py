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
    from .app import Wrk
    from .worker.base import BaseWorker


@dataclasses.dataclass
class Job:
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
        if isinstance(self.status, str):
            self.status = JobStatus(self.status)

    @classmethod
    def from_row(cls, row: dict, serializer: Serializer) -> Job:
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
        return {f.name: getattr(self, f.name) for f in dataclasses.fields(self) if f.name != "dep_ids"}


@dataclasses.dataclass
class JobExecution:
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
        if isinstance(self.status, str):
            self.status = ExecutionStatus(self.status)

    @classmethod
    def from_row(cls, row: dict, serializer: Serializer) -> JobExecution:
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
        self.last_run_at = datetime.now(timezone.utc)
        if self.cron:
            self._advance_cron()

    def seconds_until_next(self) -> float:
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

    app: "Wrk"
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
        return self.job.id if hasattr(self.job, "id") else str(self.job)


@dataclasses.dataclass
class Callback:
    """A callback function with an optional per-callback timeout."""

    func: Callable | str
    timeout: int | None = None

    def path(self) -> str:
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
        if self.times < 1:
            raise ValueError("Repeat.times must be >= 1")
        if self.interval < 0:
            raise ValueError("Repeat.interval must be >= 0")

    def get_interval(self, run: int) -> int:
        """Return delay in seconds for the given run index (0-based)."""
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
        if self.max < 1:
            raise ValueError("Retry.max must be >= 1")
        if isinstance(self.intervals, int) and self.intervals < 0:
            raise ValueError("Retry.intervals must be >= 0")

    def get_interval(self, attempt: int) -> int:
        """Return retry delay in seconds for the given attempt (1-based)."""
        if isinstance(self.intervals, list):
            idx = min(attempt - 1, len(self.intervals) - 1)
            return self.intervals[idx] if self.intervals else 0
        return self.intervals

    def to_intervals_list(self) -> list[int] | None:
        """Return intervals as a list, or None for a uniform (int) interval."""
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
