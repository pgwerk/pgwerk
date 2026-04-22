from __future__ import annotations

import logging
import dataclasses

from typing import Any
from typing import Callable
from typing import LiteralString
from typing import cast
from datetime import datetime
from datetime import timezone
from datetime import timedelta
from contextlib import asynccontextmanager

from psycopg import AsyncConnection
from psycopg.sql import SQL
from psycopg.sql import Identifier
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from .commons import DequeueStrategy
from .schemas import JOB_COLS
from .schemas import Job
from .schemas import JobInsert
from .schemas import JobExecution
from .serializers import Serializer
from .serializers import encode


logger = logging.getLogger(__name__)

_INSERT_COLS = [f.name for f in dataclasses.fields(JobInsert) if f.name != "dep_ids"]
_INSERT_SQL: LiteralString = cast(
    LiteralString,
    "INSERT INTO {jobs} (\n    "
    + ",\n    ".join(_INSERT_COLS)
    + "\n) VALUES (\n    "
    + ",\n    ".join(f"COALESCE(%({col})s, NOW())" if col == "scheduled_at" else f"%({col})s" for col in _INSERT_COLS)
    + "\n) ON CONFLICT (key) DO NOTHING RETURNING",
)


class JobRepository:
    def __init__(
        self,
        pool: AsyncConnectionPool,
        tables: dict[str, Any],
        prefix: str,
        get_serializer: Callable[[], Serializer],
    ) -> None:
        self._pool = pool
        self._t = tables
        self._prefix = prefix
        self._get_serializer = get_serializer

    @property
    def _serializer(self) -> Serializer:
        return self._get_serializer()

    @asynccontextmanager
    async def _conn(self, conn: AsyncConnection | None, transaction: bool = False):
        if conn is not None:
            yield conn
        elif transaction:
            async with self._pool.connection() as c, c.transaction():
                yield c
        else:
            async with self._pool.connection() as c:
                yield c

    # ------------------------------------------------------------------
    # Insert
    # ------------------------------------------------------------------

    async def insert(self, data: JobInsert, conn: AsyncConnection | None = None) -> Job | None:
        async with self._conn(conn) as c, c.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                SQL(_INSERT_SQL + JOB_COLS).format(jobs=self._t["jobs"]),
                data.as_params(),
            )
            row = await cur.fetchone()
            if not row:
                return None

            job = Job.from_row(row, self._serializer)

            if data.dep_ids:
                await cur.executemany(
                    SQL("""
                        INSERT INTO {deps} (job_id, depends_on, allow_failure)
                        VALUES (%s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """).format(deps=self._t["deps"]),
                    [(str(job.id), dep_id, allow_failure) for dep_id, allow_failure in data.dep_ids],
                )
            else:
                await c.execute(SQL("NOTIFY {ch}").format(ch=Identifier(f"{self._prefix}:{data.queue}")))

            return job

    async def insert_many(self, jobs: list[JobInsert], conn: AsyncConnection | None = None) -> list[Job | None]:
        results: list[Job | None] = []
        notify_queues: set[str] = set()

        async with self._conn(conn, transaction=conn is None) as c, c.cursor(row_factory=dict_row) as cur:
            for data in jobs:
                await cur.execute(
                    SQL(_INSERT_SQL + JOB_COLS).format(jobs=self._t["jobs"]),
                    data.as_params(),
                )
                row = await cur.fetchone()
                if not row:
                    results.append(None)
                    continue

                job = Job.from_row(row, self._serializer)
                results.append(job)

                if data.dep_ids:
                    await cur.executemany(
                        SQL("""
                            INSERT INTO {deps} (job_id, depends_on, allow_failure)
                            VALUES (%s, %s, %s)
                            ON CONFLICT DO NOTHING
                        """).format(deps=self._t["deps"]),
                        [(str(job.id), dep_id, af) for dep_id, af in data.dep_ids],
                    )
                else:
                    notify_queues.add(data.queue)

            for queue in notify_queues:
                await c.execute(SQL("NOTIFY {ch}").format(ch=Identifier(f"{self._prefix}:{queue}")))

        return results

    # ------------------------------------------------------------------
    # Inspection
    # ------------------------------------------------------------------

    async def get(self, job_id: str) -> Job:
        from .exceptions import JobNotFound

        async with self._pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                SQL("SELECT" + JOB_COLS + "FROM {jobs} WHERE id = %(id)s").format(jobs=self._t["jobs"]),
                {"id": job_id},
            )
            row = await cur.fetchone()
        if row is None:
            raise JobNotFound(f"Job {job_id!r} not found")
        return Job.from_row(row, self._serializer)

    async def list_jobs(
        self,
        queue: str | None = None,
        status: str | None = None,
        worker_id: str | None = None,
        search: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[Job]:
        filters = []
        params: dict[str, Any] = {"limit": limit, "offset": offset}
        if queue:
            filters.append("queue = %(queue)s")
            params["queue"] = queue
        if status:
            filters.append("status = %(status)s")
            params["status"] = status
        if worker_id:
            filters.append("worker_id = %(worker_id)s")
            params["worker_id"] = worker_id
        if search:
            filters.append("(function ILIKE %(search)s OR id::text ILIKE %(search)s OR queue ILIKE %(search)s)")
            params["search"] = f"%{search}%"
        where = SQL("WHERE " + " AND ".join(filters)) if filters else SQL("")
        async with self._pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                SQL(
                    "SELECT" + JOB_COLS + "FROM {jobs} {where}"
                    " ORDER BY enqueued_at DESC LIMIT %(limit)s OFFSET %(offset)s"
                ).format(jobs=self._t["jobs"], where=where),
                params,
            )
            rows = await cur.fetchall()
        return [Job.from_row(r, self._serializer) for r in rows]

    async def get_executions(self, job_id: str) -> list[JobExecution]:
        async with self._pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                SQL("""
                    SELECT id, job_id, worker_id, attempt, status,
                           error, result, started_at, completed_at
                    FROM {executions}
                    WHERE job_id = %(job_id)s
                    ORDER BY attempt
                """).format(executions=self._t["executions"]),
                {"job_id": job_id},
            )
            rows = await cur.fetchall()
        return [JobExecution.from_row(r, self._serializer) for r in rows]

    async def get_dependencies(self, job_id: str) -> list[str]:
        async with self._pool.connection() as conn, conn.cursor() as cur:
            await cur.execute(
                SQL("SELECT depends_on FROM {deps} WHERE job_id = %(id)s").format(deps=self._t["deps"]),
                {"id": job_id},
            )
            rows = await cur.fetchall()
        return [str(r[0]) for r in rows]

    # ------------------------------------------------------------------
    # Mutations
    # ------------------------------------------------------------------

    async def cancel(self, job_id: str) -> bool:
        async with self._pool.connection() as conn, conn.transaction(), conn.cursor() as cur:
            await cur.execute(
                SQL("""
                    UPDATE {jobs}
                    SET status = 'aborted', completed_at = NOW()
                    WHERE id = %(id)s AND status IN ('scheduled', 'queued', 'waiting')
                    RETURNING id
                """).format(jobs=self._t["jobs"]),
                {"id": job_id},
            )
            if not await cur.fetchone():
                return False
            await self.settle_dependents(cur, job_id)
            return True

    async def abort(self, job_id: str) -> bool:
        async with self._pool.connection() as conn, conn.cursor() as cur:
            await cur.execute(
                SQL("""
                    UPDATE {jobs}
                    SET status = 'aborting'
                    WHERE id = %(id)s AND status = 'active'
                    RETURNING id
                """).format(jobs=self._t["jobs"]),
                {"id": job_id},
            )
            return await cur.fetchone() is not None

    async def touch(self, job_id: str) -> None:
        async with self._pool.connection() as conn, conn.transaction():
            await conn.execute(
                SQL("UPDATE {jobs} SET touched_at = NOW() WHERE id = %(id)s").format(jobs=self._t["jobs"]),
                {"id": job_id},
            )

    async def requeue(self, job_id: str) -> bool:
        async with self._pool.connection() as conn, conn.cursor() as cur:
            await cur.execute(
                SQL("""
                    UPDATE {jobs}
                    SET status = 'queued',
                        error = NULL,
                        result = NULL,
                        started_at = NULL,
                        completed_at = NULL,
                        expires_at = NULL,
                        scheduled_at = NOW(),
                        attempts = 0,
                        worker_id = NULL
                    WHERE id = %(id)s
                      AND status IN ('failed', 'aborted')
                    RETURNING id, queue
                """).format(jobs=self._t["jobs"]),
                {"id": job_id},
            )
            row = await cur.fetchone()
            if row is None:
                return False
            await conn.execute(SQL("NOTIFY {ch}").format(ch=Identifier(f"{self._prefix}:{row[1]}")))
            return True

    async def sweep(self, max_active_secs: int) -> list[str]:
        swept_ids: list[str] = []
        notify_queues: set[str] = set()

        async with self._pool.connection() as conn, conn.transaction(), conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                SQL("""
                    WITH stuck AS (
                        SELECT id, queue, attempts, max_attempts
                        FROM {jobs}
                        WHERE status IN ('active', 'aborting')
                          AND (
                              (timeout_secs IS NOT NULL
                               AND started_at + timeout_secs * INTERVAL '1 second' < NOW())
                              OR
                              (heartbeat_secs IS NOT NULL
                               AND COALESCE(touched_at, started_at) + heartbeat_secs * INTERVAL '1 second' < NOW())
                              OR
                              (timeout_secs IS NULL AND heartbeat_secs IS NULL
                               AND started_at + %(max_active_secs)s * INTERVAL '1 second' < NOW())
                          )
                        FOR UPDATE SKIP LOCKED
                    )
                    UPDATE {jobs} j
                    SET status       = CASE WHEN s.attempts >= s.max_attempts THEN 'failed' ELSE 'queued' END,
                        error        = CASE WHEN s.attempts >= s.max_attempts THEN 'swept'  ELSE error    END,
                        worker_id    = NULL,
                        started_at   = CASE WHEN s.attempts < s.max_attempts THEN NULL ELSE started_at END,
                        touched_at   = NULL,
                        scheduled_at = CASE WHEN s.attempts < s.max_attempts THEN NOW() ELSE scheduled_at END,
                        completed_at = CASE WHEN s.attempts >= s.max_attempts THEN NOW() ELSE NULL END
                    FROM stuck s
                    WHERE j.id = s.id
                    RETURNING j.id::text, j.queue, j.status
                """).format(jobs=self._t["jobs"]),
                {"max_active_secs": max_active_secs},
            )
            rows = await cur.fetchall()

            for row in rows:
                job_id = row["id"]
                swept_ids.append(job_id)

                if row["status"] == "failed":
                    await cur.execute(
                        SQL("""
                            UPDATE {executions}
                            SET status = 'failed', error = 'swept', completed_at = NOW()
                            WHERE job_id = %(jid)s::uuid AND status = 'running'
                        """).format(executions=self._t["executions"]),
                        {"jid": job_id},
                    )
                    notify_queues.update(await self.settle_dependents(cur, job_id))
                else:
                    notify_queues.add(row["queue"])

                await cur.execute(
                    SQL("DELETE FROM {worker_jobs} WHERE job_id = %(jid)s::uuid").format(
                        worker_jobs=self._t["worker_jobs"]
                    ),
                    {"jid": job_id},
                )

            for queue in notify_queues:
                await conn.execute(SQL("NOTIFY {ch}").format(ch=Identifier(f"{self._prefix}:{queue}")))

        return swept_ids

    async def reenqueue_repeat(self, job: Job) -> None:
        if job.repeat_intervals:
            delay = job.repeat_intervals[0]
            next_intervals: list[int] | None = (
                job.repeat_intervals[1:] if len(job.repeat_intervals) > 1 else job.repeat_intervals
            )
        else:
            delay = job.repeat_interval_secs or 0
            next_intervals = None

        scheduled_at = datetime.now(timezone.utc) + timedelta(seconds=delay)

        async with self._pool.connection() as conn:
            await conn.execute(
                SQL("""
                    INSERT INTO {jobs} (
                        function, queue, status, priority, group_key, payload,
                        max_attempts, timeout_secs, heartbeat_secs, scheduled_at,
                        meta, result_ttl, failure_ttl, ttl,
                        on_success, on_failure, on_stopped,
                        on_success_timeout, on_failure_timeout, on_stopped_timeout,
                        retry_intervals,
                        repeat_remaining, repeat_interval_secs, repeat_intervals
                    )
                    SELECT
                        function, queue, 'queued', priority, group_key, payload,
                        max_attempts, timeout_secs, heartbeat_secs, %(scheduled_at)s,
                        meta, result_ttl, failure_ttl, ttl,
                        on_success, on_failure, on_stopped,
                        on_success_timeout, on_failure_timeout, on_stopped_timeout,
                        retry_intervals,
                        %(remaining)s, repeat_interval_secs, %(repeat_intervals)s
                    FROM {jobs}
                    WHERE id = %(id)s
                """).format(jobs=self._t["jobs"]),
                {
                    "id": job.id,
                    "scheduled_at": scheduled_at,
                    "remaining": (job.repeat_remaining or 1) - 1,
                    "repeat_intervals": encode(self._serializer, next_intervals),
                },
            )
            await conn.execute(SQL("NOTIFY {ch}").format(ch=Identifier(f"{self._prefix}:{job.queue}")))

    # ------------------------------------------------------------------
    # Shared graph helper (also used by WorkerRepository)
    # ------------------------------------------------------------------

    async def delete(self, job_id: str) -> None:
        async with self._pool.connection() as conn:
            await conn.execute(
                SQL("DELETE FROM {jobs} WHERE id = %(id)s").format(jobs=self._t["jobs"]),
                {"id": job_id},
            )

    async def bulk_requeue(self, queue: str | None = None, function_name: str | None = None) -> int:
        filters = ["status = 'failed'"]
        params: dict[str, Any] = {}
        if queue:
            filters.append("queue = %(queue)s")
            params["queue"] = queue
        if function_name:
            filters.append("function = %(function_name)s")
            params["function_name"] = function_name
        where = SQL(" AND ".join(filters))

        async with self._pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                SQL("""
                    UPDATE {jobs}
                    SET status = 'queued',
                        error = NULL,
                        result = NULL,
                        started_at = NULL,
                        completed_at = NULL,
                        expires_at = NULL,
                        scheduled_at = NOW(),
                        attempts = 0,
                        worker_id = NULL
                    WHERE {where}
                    RETURNING queue
                """).format(jobs=self._t["jobs"], where=where),
                params,
            )
            rows = await cur.fetchall()
            for q in {r["queue"] for r in rows}:
                await conn.execute(SQL("NOTIFY {ch}").format(ch=Identifier(f"{self._prefix}:{q}")))
        return len(rows)

    async def bulk_cancel(self, queue: str | None = None) -> int:
        filters = ["status IN ('queued', 'scheduled', 'waiting')"]
        params: dict[str, Any] = {}
        if queue:
            filters.append("queue = %(queue)s")
            params["queue"] = queue
        where = SQL(" AND ".join(filters))

        async with self._pool.connection() as conn, conn.cursor() as cur:
            await cur.execute(
                SQL("""
                    UPDATE {jobs}
                    SET status = 'aborted', completed_at = NOW()
                    WHERE {where}
                """).format(jobs=self._t["jobs"], where=where),
                params,
            )
            return cur.rowcount

    async def purge(self, statuses: list[str], older_than_days: int) -> int:
        async with self._pool.connection() as conn, conn.cursor() as cur:
            await cur.execute(
                SQL("""
                    DELETE FROM {jobs}
                    WHERE status = ANY(%(statuses)s)
                      AND enqueued_at < NOW() - make_interval(days => %(days)s)
                """).format(jobs=self._t["jobs"]),
                {"statuses": statuses, "days": older_than_days},
            )
            return cur.rowcount

    async def list_cron_stats(self) -> list[dict[str, Any]]:
        async with self._pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                SQL("""
                    SELECT
                        cron_name,
                        MAX(function)                             AS function,
                        MAX(queue)                               AS queue,
                        COUNT(*)                                 AS total_runs,
                        COUNT(*) FILTER (WHERE status = 'failed') AS failed_runs,
                        (array_agg(status ORDER BY enqueued_at DESC))[1] AS last_status,
                        MAX(enqueued_at)                         AS last_enqueued_at,
                        MAX(completed_at)                        AS last_completed_at
                    FROM {jobs}
                    WHERE cron_name IS NOT NULL
                    GROUP BY cron_name
                    ORDER BY MAX(enqueued_at) DESC
                """).format(jobs=self._t["jobs"])
            )
            return await cur.fetchall()

    async def trigger_cron(self, name: str) -> Job | None:
        async with self._pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                SQL("""
                    SELECT function, queue FROM {jobs}
                    WHERE cron_name = %(name)s
                    ORDER BY enqueued_at DESC LIMIT 1
                """).format(jobs=self._t["jobs"]),
                {"name": name},
            )
            existing = await cur.fetchone()

        if not existing:
            return None

        channel = Identifier(f"{self._prefix}:{existing['queue']}")

        async with self._pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                SQL(
                    """
                    INSERT INTO {jobs} (function, queue, status, priority, scheduled_at, cron_name)
                    VALUES (%(fn)s, %(queue)s, 'queued', 0, NOW(), %(cron_name)s)
                    RETURNING
                """
                    + JOB_COLS
                ).format(jobs=self._t["jobs"]),
                {"fn": existing["function"], "queue": existing["queue"], "cron_name": name},
            )
            row = await cur.fetchone()
            if not row:
                return None
            job = Job.from_row(row, self._serializer)
            await conn.execute(SQL("NOTIFY {ch}").format(ch=channel))
        return job

    async def reschedule_stuck(self) -> int:
        async with self._pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                SQL("""
                    UPDATE {jobs}
                    SET status = 'queued'
                    WHERE status = 'scheduled' AND scheduled_at <= NOW()
                    RETURNING queue
                """).format(jobs=self._t["jobs"])
            )
            rows = await cur.fetchall()
            for q in {r["queue"] for r in rows}:
                await conn.execute(SQL("NOTIFY {ch}").format(ch=Identifier(f"{self._prefix}:{q}")))
        return len(rows)

    # ------------------------------------------------------------------
    # Shared graph helper (also used by WorkerRepository)
    # ------------------------------------------------------------------

    async def settle_dependents(self, cur: Any, settled_job_id: str) -> list[str]:
        notify_queues: set[str] = set()
        pending = [settled_job_id]
        while pending:
            batch = pending
            pending = []
            for jid in batch:
                await cur.execute(
                    SQL("""
                        SELECT j.id::text
                        FROM {deps} d
                        JOIN {jobs} j ON j.id = d.job_id
                        WHERE d.depends_on = %(jid)s
                          AND j.status = 'waiting'
                        FOR UPDATE OF j
                    """).format(deps=self._t["deps"], jobs=self._t["jobs"]),
                    {"jid": jid},
                )
                candidate_rows = await cur.fetchall()
                candidate_ids = [r["id"] if isinstance(r, dict) else r[0] for r in candidate_rows]
                if not candidate_ids:
                    continue

                await cur.execute(
                    SQL("""
                        WITH settled AS (
                            SELECT j.id,
                                   j.queue,
                                   EXISTS(
                                       SELECT 1
                                       FROM {deps} d3
                                       JOIN {jobs} jd3 ON jd3.id = d3.depends_on
                                       WHERE d3.job_id = j.id
                                         AND jd3.status IN ('failed', 'aborted')
                                         AND NOT d3.allow_failure
                                   ) AS must_fail
                            FROM {jobs} j
                            WHERE j.id = ANY(%(candidate_ids)s::uuid[])
                              AND NOT EXISTS (
                                  SELECT 1
                                  FROM {deps} d2
                                  JOIN {jobs} jd ON jd.id = d2.depends_on
                                  WHERE d2.job_id = j.id
                                    AND jd.status NOT IN ('complete', 'failed', 'aborted')
                              )
                        )
                        UPDATE {jobs} j
                        SET status       = CASE WHEN s.must_fail THEN 'failed'  ELSE 'queued'  END,
                            error        = CASE WHEN s.must_fail THEN 'dependency failed' ELSE NULL END,
                            completed_at = CASE WHEN s.must_fail THEN NOW()     ELSE NULL       END,
                            scheduled_at = CASE WHEN s.must_fail THEN j.scheduled_at ELSE NOW() END
                        FROM settled s
                        WHERE j.id = s.id
                        RETURNING j.id::text, j.queue, j.status
                    """).format(deps=self._t["deps"], jobs=self._t["jobs"]),
                    {"candidate_ids": candidate_ids},
                )
                rows = await cur.fetchall()
                for r in rows:
                    row_id = r["id"] if isinstance(r, dict) else r[0]
                    row_queue = r["queue"] if isinstance(r, dict) else r[1]
                    row_status = r["status"] if isinstance(r, dict) else r[2]
                    if row_status == "failed":
                        pending.append(row_id)
                    else:
                        notify_queues.add(row_queue)
        return list(notify_queues)


class WorkerRepository:
    def __init__(
        self,
        pool: AsyncConnectionPool,
        tables: dict[str, Any],
        prefix: str,
        get_serializer: Callable[[], Serializer],
        job_repo: JobRepository,
    ) -> None:
        self._pool = pool
        self._t = tables
        self._prefix = prefix
        self._get_serializer = get_serializer
        self._job_repo = job_repo

    @property
    def _serializer(self) -> Serializer:
        return self._get_serializer()

    # ------------------------------------------------------------------
    # Registration & heartbeat
    # ------------------------------------------------------------------

    async def register(self, worker_id: str, name: str, queues: list[str], metadata: str) -> None:
        async with self._pool.connection() as conn, conn.transaction():
            await conn.execute(
                SQL("""
                    INSERT INTO {worker} (id, name, queue, status, metadata, heartbeat_at)
                    VALUES (%(id)s, %(name)s, %(queue)s, 'active', %(meta)s, NOW())
                    ON CONFLICT (id) DO UPDATE
                        SET name = EXCLUDED.name,
                            queue = EXCLUDED.queue,
                            status = 'active',
                            metadata = EXCLUDED.metadata,
                            heartbeat_at = NOW()
                """).format(worker=self._t["worker"]),
                {"id": worker_id, "name": name, "queue": ",".join(queues), "meta": metadata},
            )

    async def deregister(self, worker_id: str) -> None:
        async with self._pool.connection() as conn, conn.transaction():
            await conn.execute(
                SQL("""
                    UPDATE {worker}
                    SET status = 'stopped', expires_at = NOW() + INTERVAL '1 hour'
                    WHERE id = %(id)s
                """).format(worker=self._t["worker"]),
                {"id": worker_id},
            )

    async def update_heartbeat(self, worker_id: str) -> None:
        async with self._pool.connection() as conn, conn.transaction():
            await conn.execute(
                SQL("UPDATE {worker} SET heartbeat_at = NOW() WHERE id = %(id)s").format(worker=self._t["worker"]),
                {"id": worker_id},
            )

    # ------------------------------------------------------------------
    # Dequeue
    # ------------------------------------------------------------------

    async def dequeue(
        self,
        worker_id: str,
        ordered_queues: list[str],
        limit: int,
        strategy: DequeueStrategy,
    ) -> list[Job]:
        if strategy == DequeueStrategy.Priority:
            order_sql = SQL(
                "ORDER BY CASE WHEN status IN ('queued', 'scheduled') THEN 0 ELSE 1 END, "
                "priority DESC, scheduled_at ASC"
            )
            extra_params: dict[str, Any] = {}
        else:
            order_sql = SQL(
                "ORDER BY CASE WHEN status IN ('queued', 'scheduled') THEN 0 ELSE 1 END, "
                "array_position(%(ordered_queues)s::text[], queue), priority DESC, scheduled_at ASC"
            )
            extra_params = {"ordered_queues": ordered_queues}

        async with self._pool.connection() as conn, conn.transaction(), conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                SQL(
                    """
                    WITH locked AS (
                        SELECT id AS _id,
                               status IN ('queued', 'scheduled') AS _is_new
                        FROM {jobs}
                        WHERE queue = ANY(%(queues)s)
                          AND (
                              (
                                  status IN ('queued', 'scheduled')
                                  AND scheduled_at <= NOW()
                                  AND (expires_at IS NULL OR expires_at > NOW())
                                  AND (
                                      group_key IS NULL
                                      OR group_key NOT IN (
                                          SELECT DISTINCT group_key FROM {jobs}
                                          WHERE queue = ANY(%(queues)s)
                                            AND status = 'active'
                                            AND group_key IS NOT NULL
                                      )
                                  )
                              )
                              OR
                              (
                                  status = 'active'
                                  AND worker_id IS DISTINCT FROM %(wid)s::uuid
                                  AND heartbeat_secs IS NOT NULL
                                  AND COALESCE(touched_at, started_at) + heartbeat_secs * INTERVAL '1 second' < NOW()
                              )
                          )
                        {order_sql}
                        LIMIT %(limit)s
                        FOR UPDATE SKIP LOCKED
                    )
                    UPDATE {jobs}
                    SET status     = 'active',
                        started_at = NOW(),
                        attempts   = attempts + CASE WHEN locked._is_new THEN 1 ELSE 0 END,
                        worker_id  = %(wid)s
                    FROM locked
                    WHERE {jobs}.id = locked._id
                    RETURNING
                    """
                    + JOB_COLS
                ).format(jobs=self._t["jobs"], order_sql=order_sql),
                {"queues": ordered_queues, "limit": limit, "wid": worker_id, **extra_params},
            )
            rows = await cur.fetchall()
            if not rows:
                return []

            jobs = [Job.from_row(r, self._serializer) for r in rows]

            for job in jobs:
                await cur.execute(
                    SQL("DELETE FROM {wj} WHERE job_id = %(jid)s").format(wj=self._t["worker_jobs"]),
                    {"jid": job.id},
                )
                await cur.execute(
                    SQL("""
                        INSERT INTO {worker_jobs} (worker_id, job_id)
                        VALUES (%(wid)s, %(jid)s)
                    """).format(worker_jobs=self._t["worker_jobs"]),
                    {"wid": worker_id, "jid": job.id},
                )
                await cur.execute(
                    SQL("""
                        INSERT INTO {executions} (job_id, worker_id, attempt, status)
                        VALUES (%(jid)s, %(wid)s, %(attempt)s, 'running')
                    """).format(executions=self._t["executions"]),
                    {"jid": job.id, "wid": worker_id, "attempt": job.attempts},
                )

            return jobs

    # ------------------------------------------------------------------
    # Ack / Nack / Requeue
    # ------------------------------------------------------------------

    async def ack(
        self,
        worker_id: str,
        job: Job,
        result_json: str | None,
        expires_at: datetime | None,
    ) -> bool:
        async with self._pool.connection() as conn, conn.transaction(), conn.cursor() as cur:
            await cur.execute(
                SQL("""
                    UPDATE {jobs}
                    SET status = 'complete', result = %(result)s,
                        completed_at = NOW(), expires_at = %(expires_at)s,
                        worker_id = NULL
                    WHERE id = %(id)s
                      AND worker_id = %(wid)s::uuid
                      AND status IN ('active', 'aborting')
                    RETURNING 1
                """).format(jobs=self._t["jobs"]),
                {"id": job.id, "result": result_json, "expires_at": expires_at, "wid": worker_id},
            )
            if not await cur.fetchone():
                await cur.execute(
                    SQL("SELECT status, worker_id FROM {jobs} WHERE id = %(id)s").format(jobs=self._t["jobs"]),
                    {"id": job.id},
                )
                state = await cur.fetchone()
                logger.warning(
                    "ack race detected for job %s [%s] — row no longer owned by this worker (db state: %s)",
                    job.function,
                    job.id,
                    state,
                )
                return False

            await cur.execute(
                SQL("""
                    UPDATE {executions}
                    SET status = 'complete', result = %(result)s, completed_at = NOW()
                    WHERE job_id = %(jid)s AND attempt = %(attempt)s AND worker_id = %(wid)s::uuid
                """).format(executions=self._t["executions"]),
                {"jid": job.id, "attempt": job.attempts, "result": result_json, "wid": worker_id},
            )
            await cur.execute(
                SQL("DELETE FROM {wj} WHERE worker_id = %(wid)s AND job_id = %(jid)s").format(
                    wj=self._t["worker_jobs"]
                ),
                {"wid": worker_id, "jid": job.id},
            )
            unblocked_queues = await self._job_repo.settle_dependents(cur, job.id)
            for q in unblocked_queues:
                await conn.execute(SQL("NOTIFY {ch}").format(ch=Identifier(f"{self._prefix}:{q}")))

        return True

    async def nack(
        self,
        worker_id: str,
        job: Job,
        error: str,
        new_status: str,
        scheduled_at: datetime | None,
        expires_at: datetime | None,
    ) -> bool:
        is_terminal = new_status in ("failed", "aborted")

        async with self._pool.connection() as conn, conn.transaction(), conn.cursor() as cur:
            await cur.execute(
                SQL("""
                    UPDATE {jobs}
                    SET status       = %(status)s,
                        error        = %(error)s,
                        expires_at   = %(expires_at)s,
                        worker_id    = NULL,
                        touched_at   = NULL,
                        started_at   = CASE WHEN %(status)s IN ('queued', 'scheduled') THEN NULL ELSE started_at END,
                        scheduled_at = COALESCE(%(scheduled_at)s::timestamptz, scheduled_at),
                        completed_at = CASE WHEN %(status)s IN ('failed', 'aborted')
                                            THEN NOW() ELSE NULL END
                    WHERE id = %(id)s
                      AND worker_id = %(wid)s::uuid
                      AND status IN ('active', 'aborting')
                    RETURNING 1
                """).format(jobs=self._t["jobs"]),
                {
                    "id": job.id,
                    "status": new_status,
                    "error": error,
                    "expires_at": expires_at,
                    "scheduled_at": scheduled_at,
                    "wid": worker_id,
                },
            )
            if not await cur.fetchone():
                logger.warning(
                    "nack race detected for job %s [%s] — row no longer owned by this worker, skipping",
                    job.function,
                    job.id,
                )
                return False

            await cur.execute(
                SQL("""
                    UPDATE {executions}
                    SET status = %(exec_status)s, error = %(error)s, completed_at = NOW()
                    WHERE job_id = %(jid)s AND attempt = %(attempt)s AND worker_id = %(wid)s::uuid
                """).format(executions=self._t["executions"]),
                {
                    "jid": job.id,
                    "attempt": job.attempts,
                    "error": error,
                    "exec_status": "aborted" if new_status == "aborted" else "failed",
                    "wid": worker_id,
                },
            )
            await cur.execute(
                SQL("DELETE FROM {wj} WHERE worker_id = %(wid)s AND job_id = %(jid)s").format(
                    wj=self._t["worker_jobs"]
                ),
                {"wid": worker_id, "jid": job.id},
            )
            if is_terminal:
                unblocked_queues = await self._job_repo.settle_dependents(cur, job.id)
                for q in unblocked_queues:
                    await conn.execute(SQL("NOTIFY {ch}").format(ch=Identifier(f"{self._prefix}:{q}")))

        return True

    async def delete_job(self, job_id: str) -> None:
        async with self._pool.connection() as conn, conn.transaction():
            await conn.execute(
                SQL("DELETE FROM {jobs} WHERE id = %(id)s").format(jobs=self._t["jobs"]),
                {"id": job_id},
            )

    async def requeue_cancelled(self, worker_id: str, job: Job) -> None:
        async with self._pool.connection() as conn, conn.transaction(), conn.cursor() as cur:
            await cur.execute(
                SQL("""
                    UPDATE {jobs}
                    SET status = 'queued',
                        worker_id = NULL,
                        started_at = NULL,
                        touched_at = NULL,
                        scheduled_at = NOW(),
                        error = NULL,
                        attempts = GREATEST(attempts - 1, 0)
                    WHERE id = %(id)s
                      AND worker_id = %(wid)s::uuid
                      AND status IN ('active', 'aborting')
                """).format(jobs=self._t["jobs"]),
                {"id": job.id, "wid": worker_id},
            )
            await cur.execute(
                SQL("""
                    UPDATE {executions}
                    SET status = 'failed', error = 'cancelled', completed_at = NOW()
                    WHERE job_id = %(jid)s AND attempt = %(attempt)s AND worker_id = %(wid)s::uuid
                """).format(executions=self._t["executions"]),
                {"jid": job.id, "attempt": job.attempts, "wid": worker_id},
            )
            await cur.execute(
                SQL("DELETE FROM {wj} WHERE worker_id = %(wid)s AND job_id = %(jid)s").format(
                    wj=self._t["worker_jobs"]
                ),
                {"wid": worker_id, "jid": job.id},
            )
        async with self._pool.connection() as conn:
            await conn.execute(SQL("NOTIFY {ch}").format(ch=Identifier(f"{self._prefix}:{job.queue}")))

    # ------------------------------------------------------------------
    # Abort polling
    # ------------------------------------------------------------------

    async def get_aborting(self, job_ids: list[str]) -> list[str]:
        async with self._pool.connection() as conn, conn.cursor() as cur:
            await cur.execute(
                SQL("""
                    SELECT id::text FROM {jobs}
                    WHERE id = ANY(%(ids)s::uuid[])
                      AND status = 'aborting'
                """).format(jobs=self._t["jobs"]),
                {"ids": job_ids},
            )
            rows = await cur.fetchall()
        return [r[0] for r in rows]

    async def notify(self, queue: str) -> None:
        async with self._pool.connection() as conn:
            await conn.execute(SQL("NOTIFY {ch}").format(ch=Identifier(f"{self._prefix}:{queue}")))

    async def fetch(self) -> list[dict[str, Any]]:
        async with self._pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                SQL("""
                    SELECT id::text, name, queue, status, metadata, heartbeat_at, started_at, expires_at
                    FROM {worker}
                    ORDER BY started_at DESC
                """).format(worker=self._t["worker"])
            )
            return await cur.fetchall()

    async def get(self, worker_id: str) -> dict[str, Any] | None:
        async with self._pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                SQL("""
                    SELECT id::text, name, queue, status, metadata, heartbeat_at, started_at, expires_at
                    FROM {worker}
                    WHERE id = %(id)s
                """).format(worker=self._t["worker"]),
                {"id": worker_id},
            )
            return await cur.fetchone()

    async def list_jobs(self, worker_id: str, limit: int = 50, offset: int = 0) -> list[Job]:
        async with self._pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                SQL(
                    """
                    SELECT """
                    + JOB_COLS
                    + """
                    FROM {jobs}
                    WHERE id IN (
                        SELECT DISTINCT job_id
                        FROM {executions}
                        WHERE worker_id = %(worker_id)s
                    )
                    ORDER BY enqueued_at DESC
                    LIMIT %(limit)s OFFSET %(offset)s
                """
                ).format(jobs=self._t["jobs"], executions=self._t["executions"]),
                {"worker_id": worker_id, "limit": limit, "offset": offset},
            )
            rows = await cur.fetchall()
        return [Job.from_row(r, self._serializer) for r in rows]


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------


class StatsRepository:
    _SAMPLE_GRID = """
        WITH sample_times AS (
            SELECT NOW() - make_interval(secs => g.i * %(step)s) AS ts
            FROM generate_series(0, %(n)s - 1) AS g(i)
        )
    """

    def __init__(
        self,
        pool: AsyncConnectionPool,
        tables: dict[str, Any],
    ) -> None:
        self._pool = pool
        self._t = tables

    @staticmethod
    def _step_secs(minutes: int) -> int:
        if minutes <= 30:
            return 60
        if minutes <= 60:
            return 5 * 60
        if minutes <= 360:
            return 15 * 60
        if minutes <= 1440:
            return 60 * 60
        return 6 * 60 * 60

    async def get_queue_stats(self) -> tuple[list[dict[str, Any]], int, int]:
        async with self._pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                SQL("""
                    SELECT
                        queue,
                        COUNT(*) FILTER (WHERE status = 'scheduled') AS scheduled,
                        COUNT(*) FILTER (WHERE status = 'queued')   AS queued,
                        COUNT(*) FILTER (WHERE status = 'active')   AS active,
                        COUNT(*) FILTER (WHERE status = 'waiting')  AS waiting,
                        COUNT(*) FILTER (WHERE status = 'failed')   AS failed,
                        COUNT(*) FILTER (WHERE status = 'complete') AS complete,
                        COUNT(*) FILTER (WHERE status = 'aborted')  AS aborted
                    FROM {jobs}
                    GROUP BY queue
                    ORDER BY queue
                """).format(jobs=self._t["jobs"])
            )
            queue_rows = await cur.fetchall()

            await cur.execute(SQL("SELECT COUNT(*) FROM {jobs}").format(jobs=self._t["jobs"]))
            total: int = (await cur.fetchone() or {}).get("count", 0)

            await cur.execute(
                SQL("""
                    SELECT COUNT(*)
                    FROM {worker}
                    WHERE heartbeat_at > NOW() - INTERVAL '30 seconds'
                """).format(worker=self._t["worker"])
            )
            workers_online: int = (await cur.fetchone() or {}).get("count", 0)

        return queue_rows, total, workers_online

    async def get_throughput_history(self, minutes: int) -> list[dict[str, Any]]:
        step = self._step_secs(minutes)
        n = max(1, (minutes * 60) // step)
        async with self._pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                SQL(
                    self._SAMPLE_GRID
                    + """
                    SELECT
                        s.ts                                  AS time,
                        e.worker_id::text                     AS worker_id,
                        COALESCE(w.name, e.worker_id::text)   AS worker_name,
                        COUNT(e.id)::int                      AS count
                    FROM sample_times s
                    LEFT JOIN {executions} e
                        ON e.completed_at >  s.ts - make_interval(secs => %(step)s)
                       AND e.completed_at <= s.ts
                       AND e.status = 'complete'
                       AND e.worker_id IS NOT NULL
                    LEFT JOIN {worker} w ON w.id = e.worker_id
                    GROUP BY s.ts, e.worker_id, w.name
                    ORDER BY s.ts, worker_name
                """
                ).format(executions=self._t["executions"], worker=self._t["worker"]),
                {"step": step, "n": n},
            )
            return await cur.fetchall()

    async def get_queue_depth_history(self, minutes: int) -> list[dict[str, Any]]:
        step = self._step_secs(minutes)
        n = max(1, (minutes * 60) // step)
        window_secs = n * step
        async with self._pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                SQL(
                    self._SAMPLE_GRID
                    + """
                    , relevant_jobs AS (
                        SELECT id, enqueued_at, started_at, completed_at
                        FROM {jobs}
                        WHERE enqueued_at <= NOW()
                          AND (
                              completed_at IS NULL
                           OR completed_at > NOW() - make_interval(secs => %(window)s)
                          )
                    )
                    SELECT
                        s.ts AS time,
                        COUNT(j.id) FILTER (
                            WHERE j.enqueued_at <= s.ts
                              AND (j.started_at IS NULL OR j.started_at > s.ts)
                              AND (j.completed_at IS NULL OR j.completed_at > s.ts)
                        )::int AS queued,
                        COUNT(j.id) FILTER (
                            WHERE j.started_at IS NOT NULL
                              AND j.started_at <= s.ts
                              AND (j.completed_at IS NULL OR j.completed_at > s.ts)
                        )::int AS active
                    FROM sample_times s
                    LEFT JOIN relevant_jobs j ON TRUE
                    GROUP BY s.ts
                    ORDER BY s.ts
                """
                ).format(jobs=self._t["jobs"]),
                {"step": step, "n": n, "window": window_secs},
            )
            return await cur.fetchall()

    async def get_server_info(self, prefix: str) -> tuple[str, int, list[dict[str, Any]]]:
        async with self._pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("SELECT version() AS ver")
            pg_version: str = (await cur.fetchone() or {}).get("ver", "")

            await cur.execute("SELECT pg_database_size(current_database()) AS sz")
            db_size_bytes: int = (await cur.fetchone() or {}).get("sz", 0)

            await cur.execute(
                """
                SELECT
                    s.relname                               AS name,
                    pg_total_relation_size(s.relid)::bigint AS size_bytes,
                    s.n_live_tup::bigint                    AS row_count
                FROM pg_stat_user_tables s
                WHERE s.relname LIKE %(pattern)s
                ORDER BY size_bytes DESC
                """,
                {"pattern": f"{prefix}_%"},
            )
            table_rows = await cur.fetchall()

        return pg_version, db_size_bytes, table_rows
