"""Default configuration values for wrk workers and schedulers."""

from __future__ import annotations

from typing import ClassVar
from dataclasses import dataclass


@dataclass
class WrkConfig:
    """Configuration for a wrk worker or scheduler.

    Attributes:
        schema_version: Internal schema version; bump on each migration. Not
            user-configurable — treated as a read-only class constant.
        schema: PostgreSQL schema that qualifies all wrk table names (e.g.
            ``"public"``). ``None`` means no schema prefix.
        prefix: Prefix applied to every wrk table name (default ``"_pgwerk"``),
            producing tables like ``_pgwerk_jobs``, ``_pgwerk_worker``, etc.
        min_pool_size: Minimum number of connections kept open in the
            connection pool.
        max_pool_size: Maximum number of connections the pool will open.
        max_active_secs: How long (seconds) a job may stay in the
            ``active`` state before the sweep marks it as failed. Applies
            to jobs that never heartbeat or whose worker crashes.
        heartbeat_interval: How often (seconds) a running worker updates
            its heartbeat timestamp in ``_pgwerk_worker``.
        poll_interval: How often (seconds) the polling loop checks
            ``_pgwerk_jobs`` for new work when no LISTEN/NOTIFY wake-up
            arrives.
        abort_interval: How often (seconds) the worker checks for a
            cancellation signal on the currently running job.
        sweep_interval: How often (seconds) the maintenance sweep runs to
            requeue stalled jobs, clean up dead workers, and resolve
            dependency chains.
        shutdown_timeout: Seconds to wait for in-flight jobs to finish
            during graceful shutdown before forcibly terminating.
        sigterm_grace: Seconds ``ForkWorker`` waits between sending
            SIGTERM and SIGKILL to a timed-out subprocess.
        cron_standby_retry_interval: Seconds a standby ``CronScheduler``
            waits before retrying the advisory lock that guards the
            primary scheduler role.
        ephemeral_tables: Use ``UNLOGGED`` tables for ``_pgwerk_worker`` and
            ``_pgwerk_worker_jobs``. Faster writes; data is lost on a crash,
            which is safe because workers re-register on startup and the
            sweep re-establishes claims.
    """

    schema_version: ClassVar[int] = 4

    # Schema / table naming
    schema: str | None = None
    prefix: str = "_pgwerk"

    # Connection pool
    min_pool_size: int = 2
    max_pool_size: int = 10

    # Job lifecycle
    max_active_secs: int = 3600

    # Worker polling / maintenance intervals (seconds)
    heartbeat_interval: int = 10
    poll_interval: float = 5.0
    abort_interval: float = 1.0
    sweep_interval: float = 60.0
    shutdown_timeout: float = 30.0

    # ForkWorker
    sigterm_grace: int = 5

    # CronScheduler
    cron_standby_retry_interval: float = 30.0

    # Table storage
    ephemeral_tables: bool = False
