from __future__ import annotations

import logging

from psycopg import AsyncConnection
from psycopg.sql import SQL
from psycopg.sql import Identifier

from wrk import utils

from .config import WrkConfig


logger = logging.getLogger(__name__)


class DatabaseManager:
    """Owns table naming and schema lifecycle for a wrk installation."""

    def __init__(self, schema: str | None, prefix: str, ephemeral_tables: bool = False) -> None:
        self.schema = schema
        self.prefix = prefix
        self.ephemeral_tables = ephemeral_tables

    def table(self, name: str) -> Identifier:
        """Return a qualified Identifier for ``{prefix}_{name}``."""
        full = f"{self.prefix}_{name}"
        if self.schema and self.schema != "public":
            return Identifier(self.schema, full)
        return Identifier(full)

    def _ddl(self) -> list:
        t = self.table
        p = self.prefix
        unlogged = SQL("UNLOGGED ") if self.ephemeral_tables else SQL("")

        def idx(name: str) -> Identifier:
            return Identifier(f"{p}_{name}")

        return [
            # --- worker -------------------------------------------------------
            SQL(
                "CREATE {unlogged}TABLE IF NOT EXISTS {worker} ("
                "    id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),"
                "    name         TEXT        NOT NULL,"
                "    queue        TEXT        NOT NULL DEFAULT 'default',"
                "    status       TEXT        NOT NULL DEFAULT 'idle',"
                "    metadata     JSONB,"
                "    heartbeat_at TIMESTAMPTZ,"
                "    started_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),"
                "    expires_at   TIMESTAMPTZ"
                ")"
            ).format(unlogged=unlogged, worker=t("worker")),
            SQL("CREATE INDEX IF NOT EXISTS {idx} ON {worker} (queue, status)").format(
                idx=idx("worker_queue_status_idx"), worker=t("worker")
            ),
            # --- jobs ---------------------------------------------------------
            SQL("""
                CREATE TABLE IF NOT EXISTS {jobs} (
                    id                   UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
                    key                  TEXT        UNIQUE,
                    function             TEXT        NOT NULL,
                    queue                TEXT        NOT NULL DEFAULT 'default',
                    status               TEXT        NOT NULL DEFAULT 'queued',
                    priority             SMALLINT    NOT NULL DEFAULT 0,
                    group_key            TEXT,
                    payload              JSONB,
                    result               JSONB,
                    error                TEXT,
                    attempts             INT         NOT NULL DEFAULT 0,
                    max_attempts         INT         NOT NULL DEFAULT 1,
                    timeout_secs         INT,
                    heartbeat_secs       INT,
                    scheduled_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    enqueued_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    started_at           TIMESTAMPTZ,
                    completed_at         TIMESTAMPTZ,
                    touched_at           TIMESTAMPTZ,
                    expires_at           TIMESTAMPTZ,
                    worker_id            UUID,
                    meta                 JSONB,
                    result_ttl           INT,
                    failure_ttl          INT,
                    failure_mode         TEXT        NOT NULL DEFAULT 'hold',
                    ttl                  INT,
                    on_success           TEXT,
                    on_failure           TEXT,
                    on_stopped           TEXT,
                    on_success_timeout   INT,
                    on_failure_timeout   INT,
                    on_stopped_timeout   INT,
                    retry_intervals      JSONB,
                    repeat_remaining     INT,
                    repeat_interval_secs INT,
                    repeat_intervals     JSONB,
                    cron_name            TEXT
                )
            """).format(jobs=t("jobs")),
            # Full-status index — supports UI queries and filtering on any status
            SQL("CREATE INDEX IF NOT EXISTS {idx} ON {jobs} (queue, status, priority DESC, scheduled_at)").format(
                idx=idx("jobs_dequeue_idx"), jobs=t("jobs")
            ),
            # Partial index — optimises the hot dequeue path; only indexes actionable rows
            SQL(
                "CREATE INDEX IF NOT EXISTS {idx} ON {jobs} (queue, priority DESC, scheduled_at) "
                "WHERE status IN ('queued', 'scheduled')"
            ).format(idx=idx("jobs_dequeue_partial_idx"), jobs=t("jobs")),
            # Partial index — optimises stale active job recovery in dequeue
            SQL(
                "CREATE INDEX IF NOT EXISTS {idx} ON {jobs} (queue, worker_id, heartbeat_secs, touched_at, started_at) "
                "WHERE status = 'active'"
            ).format(idx=idx("jobs_active_recovery_idx"), jobs=t("jobs")),
            SQL(
                "CREATE INDEX IF NOT EXISTS {idx} ON {jobs} (queue, status, group_key) WHERE group_key IS NOT NULL"
            ).format(idx=idx("jobs_group_key_idx"), jobs=t("jobs")),
            # --- worker_jobs --------------------------------------------------
            SQL(
                "CREATE {unlogged}TABLE IF NOT EXISTS {worker_jobs} ("
                "    worker_id  UUID        NOT NULL REFERENCES {worker}(id) ON DELETE CASCADE,"
                "    job_id     UUID        NOT NULL REFERENCES {jobs}(id)   ON DELETE CASCADE,"
                "    claimed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),"
                "    PRIMARY KEY (worker_id, job_id)"
                ")"
            ).format(
                unlogged=unlogged,
                worker_jobs=t("worker_jobs"),
                worker=t("worker"),
                jobs=t("jobs"),
            ),
            # --- jobs_executions ----------------------------------------------
            SQL("""
                CREATE TABLE IF NOT EXISTS {executions} (
                    id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
                    job_id       UUID        NOT NULL REFERENCES {jobs}(id)   ON DELETE CASCADE,
                    worker_id    UUID                 REFERENCES {worker}(id) ON DELETE SET NULL,
                    attempt      INT         NOT NULL DEFAULT 1,
                    status       TEXT        NOT NULL DEFAULT 'running',
                    error        TEXT,
                    result       JSONB,
                    started_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    completed_at TIMESTAMPTZ
                )
            """).format(
                executions=t("jobs_executions"),
                jobs=t("jobs"),
                worker=t("worker"),
            ),
            SQL("CREATE INDEX IF NOT EXISTS {idx} ON {executions} (job_id)").format(
                idx=idx("executions_job_id_idx"), executions=t("jobs_executions")
            ),
            # --- job_deps -----------------------------------------------------
            SQL("""
                CREATE TABLE IF NOT EXISTS {deps} (
                    job_id        UUID    NOT NULL REFERENCES {jobs}(id) ON DELETE CASCADE,
                    depends_on    UUID    NOT NULL REFERENCES {jobs}(id) ON DELETE CASCADE,
                    allow_failure BOOLEAN NOT NULL DEFAULT FALSE,
                    PRIMARY KEY (job_id, depends_on)
                )
            """).format(deps=t("job_deps"), jobs=t("jobs")),
            SQL("CREATE INDEX IF NOT EXISTS {idx} ON {deps} (depends_on)").format(
                idx=idx("job_deps_on_idx"), deps=t("job_deps")
            ),
        ]

    def _migrations(self) -> list[tuple[int, list]]:
        """Return (target_version, [sql_stmts]) for each incremental migration."""
        t = self.table
        p = self.prefix

        def idx(name: str) -> Identifier:
            return Identifier(f"{p}_{name}")

        return [
            (
                3,
                [
                    SQL("ALTER TABLE {jobs} ADD COLUMN IF NOT EXISTS heartbeat_secs INT").format(jobs=t("jobs")),
                    SQL("ALTER TABLE {jobs} ADD COLUMN IF NOT EXISTS touched_at TIMESTAMPTZ").format(jobs=t("jobs")),
                ],
            ),
            (
                4,
                [
                    SQL("ALTER TABLE {jobs} ADD COLUMN IF NOT EXISTS failure_mode TEXT NOT NULL DEFAULT 'hold'").format(
                        jobs=t("jobs")
                    ),
                    # Partial index for the hot dequeue path
                    SQL(
                        "CREATE INDEX IF NOT EXISTS {idx} ON {jobs} (queue, priority DESC, scheduled_at) "
                        "WHERE status IN ('queued', 'scheduled')"
                    ).format(idx=idx("jobs_dequeue_partial_idx"), jobs=t("jobs")),
                    # Partial index for stale active job recovery
                    SQL(
                        "CREATE INDEX IF NOT EXISTS {idx} ON {jobs} "
                        "(queue, worker_id, heartbeat_secs, touched_at, started_at) "
                        "WHERE status = 'active'"
                    ).format(idx=idx("jobs_active_recovery_idx"), jobs=t("jobs")),
                ],
            ),
        ]

    async def migrate(self, conn: AsyncConnection) -> None:
        versions_tbl = self.table("versions")

        if self.schema and self.schema != "public":
            await conn.execute(SQL("CREATE SCHEMA IF NOT EXISTS {s}").format(s=Identifier(self.schema)))

        await conn.execute(SQL("CREATE TABLE IF NOT EXISTS {v} (version INT NOT NULL)").format(v=versions_tbl))

        lock_key = utils.advisory_key(f"{self.schema}:{self.prefix}")

        async with conn.transaction():
            async with conn.cursor() as cur:
                # Blocking form — serialises concurrent startups; the version
                # check below makes the second entrant a no-op after the first
                # migration commits.
                await cur.execute("SELECT pg_advisory_xact_lock(%s)", (lock_key,))

                await cur.execute(SQL("SELECT version FROM {v}").format(v=versions_tbl))
                row = await cur.fetchone()
                current = row[0] if row else 0

                if current >= WrkConfig.schema_version:
                    return

                logger.info("wrk: migrating schema from version %d to %d", current, WrkConfig.schema_version)

                if current == 0:
                    for stmt in self._ddl():
                        await cur.execute(stmt)
                else:
                    for target_version, stmts in self._migrations():
                        if target_version > current:
                            for stmt in stmts:
                                await cur.execute(stmt)

                if current == 0:
                    await cur.execute(
                        SQL("INSERT INTO {v} (version) VALUES (%s)").format(v=versions_tbl),
                        (WrkConfig.schema_version,),
                    )
                else:
                    await cur.execute(
                        SQL("UPDATE {v} SET version = %s").format(v=versions_tbl),
                        (WrkConfig.schema_version,),
                    )

        logger.info("wrk: schema up to date (version %d)", WrkConfig.schema_version)

    async def alter_ephemeral_tables(self, conn: AsyncConnection) -> None:
        """ALTER worker/worker_jobs tables to UNLOGGED if they are currently permanent."""
        for name in ("worker", "worker_jobs"):
            tbl = self.table(name)
            row = await (
                await conn.execute(
                    SQL("""
                        SELECT relpersistence FROM pg_class c
                        JOIN pg_namespace n ON n.oid = c.relnamespace
                        WHERE c.relname = %(relname)s
                          AND n.nspname = %(schema)s
                    """),
                    {
                        "relname": f"{self.prefix}_{name}",
                        "schema": self.schema or "public",
                    },
                )
            ).fetchone()
            if row and row[0] == "p":
                await conn.execute(SQL("ALTER TABLE {t} SET UNLOGGED").format(t=tbl))
                logger.info("wrk: altered %s_%s to UNLOGGED", self.prefix, name)
