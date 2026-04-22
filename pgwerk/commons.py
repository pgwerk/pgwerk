"""Job status and dequeue strategy enums."""

from __future__ import annotations

import enum


class JobStatus(str, enum.Enum):
    """Lifecycle states a job row can occupy."""

    Scheduled = "scheduled"
    Queued = "queued"
    Waiting = "waiting"
    Active = "active"
    Aborting = "aborting"
    Complete = "complete"
    Failed = "failed"
    Aborted = "aborted"


class ExecutionStatus(str, enum.Enum):
    """Terminal or in-progress state for a single execution attempt."""

    Running = "running"
    Complete = "complete"
    Failed = "failed"
    Aborted = "aborted"


class DequeueStrategy(str, enum.Enum):
    """Controls the order in which queues are polled during dequeue."""

    Priority = "priority"
    RoundRobin = "round_robin"
    Random = "random"


class FailureMode(str, enum.Enum):
    Hold = "hold"  # keep failed job row for inspection / manual retry
    Delete = "delete"  # delete job row on terminal failure (fire-and-forget)

