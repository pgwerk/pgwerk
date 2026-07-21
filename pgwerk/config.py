"""Configuration for wrk workers, schedulers, and the API server."""

from __future__ import annotations

import os

from typing import ClassVar
from dataclasses import dataclass


@dataclass
class WerkConfig:
    """Unified configuration for wrk.

    Attributes:
        schema_version: Internal schema version; bump on each migration. Not
            user-configurable — treated as a read-only class constant.
        dsn: Postgres connection string.
        schema: PostgreSQL schema that qualifies all wrk table names (e.g.
            ``"public"``). ``None`` means no schema prefix.
        prefix: Prefix applied to every wrk table name (default ``"_pgwerk"``),
            producing tables like ``_pgwerk_jobs``, ``_pgwerk_worker``, etc.
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
        ephemeral_tables: Use ``UNLOGGED`` tables for ``_pgwerk_worker`` and
            ``_pgwerk_worker_jobs``. Faster writes; data is lost on a crash,
            which is safe because workers re-register on startup and the
            sweep re-establishes claims.
        listen: When ``True`` (default), workers open a persistent
            ``LISTEN`` connection for instant ``NOTIFY``-driven wake-up.
            Set to ``False`` to fall back to pure polling — required when
            routing connections through PgBouncer in transaction-pooling
            mode, which does not support ``LISTEN``/``NOTIFY``.
        metrics: Enable Prometheus metrics at ``GET /metrics``.
        metrics_interval: Metrics scrape interval in seconds.
        ui: Serve the SPA dashboard (requires built static files).
        ui_auth: Basic Auth for the SPA as ``user:password``.
        api_token: Bearer token for all ``/api/*`` routes.
        default_retry_backoff: When ``True`` (default), passing a plain integer
            to ``_retry`` synthesizes an exponential backoff schedule rather
            than retrying immediately. An explicit :class:`Retry` always wins.
    """

    schema_version: ClassVar[int] = 7
    min_compatible_db_version: ClassVar[int] = 6
    max_compatible_db_version: ClassVar[int | None] = None

    # Connection
    dsn: str | None = None

    # Schema / table naming
    schema: str = "pgwerk"
    prefix: str = "_pgwerk"

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

    # Table storage
    ephemeral_tables: bool = False

    # API server
    metrics: bool = False
    metrics_interval: float = 15.0
    ui: bool = True
    ui_auth: str | None = None
    api_token: str | None = None

    # LISTEN/NOTIFY
    listen: bool = True

    # Retry behavior
    default_retry_backoff: bool = True

    # Dangerous operations
    allow_truncate: bool = False

    @classmethod
    def from_env(cls) -> "WerkConfig":

        def _bool(key: str, default: bool) -> bool:
            v = os.environ.get(key, "").lower()
            return default if not v else v in ("1", "true", "yes")

        def _float(key: str, default: float) -> float:
            return float(os.environ.get(key, default))

        def _int(key: str, default: int) -> int:
            return int(os.environ.get(key, default))

        return cls(
            dsn=os.environ.get("PGWERK_DSN"),
            schema=os.environ.get("PGWERK_SCHEMA", "pgwerk"),
            prefix=os.environ.get("PGWERK_PREFIX", "_pgwerk"),
            max_active_secs=_int("PGWERK_MAX_ACTIVE_SECS", 3600),
            heartbeat_interval=_int("PGWERK_HEARTBEAT_INTERVAL", 10),
            poll_interval=_float("PGWERK_POLL_INTERVAL", 5.0),
            abort_interval=_float("PGWERK_ABORT_INTERVAL", 1.0),
            sweep_interval=_float("PGWERK_SWEEP_INTERVAL", 60.0),
            shutdown_timeout=_float("PGWERK_SHUTDOWN_TIMEOUT", 30.0),
            sigterm_grace=_int("PGWERK_SIGTERM_GRACE", 5),
            ephemeral_tables=_bool("PGWERK_EPHEMERAL_TABLES", False),
            metrics=_bool("PGWERK_METRICS", False),
            metrics_interval=_float("PGWERK_METRICS_INTERVAL", 15.0),
            ui=not _bool("PGWERK_NO_UI", False),
            ui_auth=os.environ.get("PGWERK_UI_AUTH"),
            api_token=os.environ.get("PGWERK_API_TOKEN"),
            default_retry_backoff=_bool("PGWERK_DEFAULT_RETRY_BACKOFF", True),
            allow_truncate=_bool("PGWERK_ALLOW_TRUNCATE", False),
            listen=_bool("PGWERK_LISTEN", True),
        )

    def to_env(self) -> None:

        os.environ["PGWERK_DSN"] = self.dsn or ""
        os.environ["PGWERK_SCHEMA"] = self.schema
        os.environ["PGWERK_PREFIX"] = self.prefix
        os.environ["PGWERK_MAX_ACTIVE_SECS"] = str(self.max_active_secs)
        os.environ["PGWERK_HEARTBEAT_INTERVAL"] = str(self.heartbeat_interval)
        os.environ["PGWERK_POLL_INTERVAL"] = str(self.poll_interval)
        os.environ["PGWERK_ABORT_INTERVAL"] = str(self.abort_interval)
        os.environ["PGWERK_SWEEP_INTERVAL"] = str(self.sweep_interval)
        os.environ["PGWERK_SHUTDOWN_TIMEOUT"] = str(self.shutdown_timeout)
        os.environ["PGWERK_SIGTERM_GRACE"] = str(self.sigterm_grace)
        os.environ["PGWERK_EPHEMERAL_TABLES"] = "true" if self.ephemeral_tables else "false"
        os.environ["PGWERK_METRICS"] = "true" if self.metrics else "false"
        os.environ["PGWERK_METRICS_INTERVAL"] = str(self.metrics_interval)
        os.environ["PGWERK_NO_UI"] = "true" if not self.ui else "false"
        if self.ui_auth:
            os.environ["PGWERK_UI_AUTH"] = self.ui_auth
        if self.api_token:
            os.environ["PGWERK_API_TOKEN"] = self.api_token
        os.environ["PGWERK_DEFAULT_RETRY_BACKOFF"] = "true" if self.default_retry_backoff else "false"
        os.environ["PGWERK_ALLOW_TRUNCATE"] = "true" if self.allow_truncate else "false"
        os.environ["PGWERK_LISTEN"] = "true" if self.listen else "false"
