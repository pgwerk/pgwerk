from __future__ import annotations

import logging
import dataclasses

from typing import Any
from typing import Callable
from typing import Sequence
from typing import LiteralString
from typing import AsyncGenerator
from typing import cast
from datetime import datetime
from datetime import timezone
from datetime import timedelta
from contextlib import asynccontextmanager

import psycopg

from psycopg.sql import SQL
from psycopg.sql import Identifier
from psycopg.sql import Placeholder
from psycopg.rows import dict_row

from .utils import compute_next_run
from .commons import DequeueStrategy
from .schemas import JOB_COLS
from .schemas import Job
from .schemas import Schedule
from .schemas import JobInsert
from .schemas import JobExecution
from .connection import Connect
from .connection import AsyncConnection
from .exceptions import JobNotFound
from .serializers import Serializer
from .serializers import encode


logger = logging.getLogger(__name__)


class JobRepository:

    _INSERT_COLS: list[str] = [f.name for f in dataclasses.fields(JobInsert) if f.name != "dep_ids"]
    _INSERT_SQL: LiteralString = cast(  # type: ignore[redundant-cast]
        LiteralString,
        "INSERT INTO {jobs} (\n    "
        + ",\n    ".join(_INSERT_COLS)
        + "\n) VALUES (\n    "
        + ",\n    ".join(
            f"COALESCE(%({col})s, NOW())" if col == "scheduled_at" else f"%({col})s" for col in _INSERT_COLS
        )
        + "\n) ON CONFLICT (key) DO NOTHING RETURNING",
    )

    def __init__(
        self,
        connect: Connect,
        tables: dict[str, Any],
        prefix: str,
        serializer: Serializer,
    ) -> None:
        self._connect = connect
        self._t = tables
        self._prefix = prefix
        self._serializer = serializer

    @asynccontextmanager
    async def _conn(
        self, conn: AsyncConnection | None, transaction: bool = False
    ) -> AsyncGenerator[AsyncConnection, None]:
        if conn is not None:
            yield conn
        elif transaction:
            async with await self._connect() as c, c.transaction():
                yield c
        else:
            async with await self._connect() as c:
                yield c

    # ------------------------------------------------------------------
    # Insert
    # ------------------------------------------------------------------

    async def insert(self, data: JobInsert, conn: AsyncConnection | None = None, notify: bool = True) -> Job | None:
        """Insert a single job row and return the resulting :class:`Job`.

        Args:
            data: Pre-processed job values to insert.
            conn: Existing connection to insert within; opens its own when ``None``.
            notify: When ``True``, emit ``NOTIFY`` to wake listening workers (and
                settle dependents when the job has dependencies).

        Returns:
            The inserted :class:`Job`, or ``None`` if a key conflict suppressed the insert.
        """
        async with self._conn(conn) as c, c.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                SQL(self._INSERT_SQL + JOB_COLS).format(jobs=self._t["jobs"]),
                data.as_params(),
            )
            row = await cur.fetchone()
            if not row:
                return None

            job = Job.from_row(row, self._serializer)

            if notify:
                if data.dep_ids:
                    await cur.executemany(
                        SQL("""
                            INSERT INTO {deps} (job_id, depends_on, allow_failure)
                            VALUES (%s, %s, %s)
                            ON CONFLICT DO NOTHING
                        """).format(deps=self._t["deps"]),
                        [(str(job.id), dep_id, allow_failure) for dep_id, allow_failure in data.dep_ids],
                    )
                    if await self._check_dependencies(cur, str(job.id)):
                        for q in await self.settle_dependents(cur, data.dep_ids[0][0]):
                            await c.execute(SQL("NOTIFY {ch}").format(ch=Identifier(f"{self._prefix}:{q}")))
                else:
                    await c.execute(SQL("NOTIFY {ch}").format(ch=Identifier(f"{self._prefix}:{data.queue}")))
            elif data.dep_ids:
                await cur.executemany(
                    SQL("""
                        INSERT INTO {deps} (job_id, depends_on, allow_failure)
                        VALUES (%s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """).format(deps=self._t["deps"]),
                    [(str(job.id), dep_id, allow_failure) for dep_id, allow_failure in data.dep_ids],
                )

            return job

    async def record_execution(
        self,
        job_id: str,
        attempt: int,
        worker_id: str | None = None,
        conn: AsyncConnection | None = None,
    ) -> None:
        """Write the initial ``running`` execution row for an attempt.

        Mirrors the row the worker creates at dequeue time, for jobs claimed
        outside the dequeue path (the ``_sync=True`` inline-execution path).

        Args:
            job_id: ID of the job being executed.
            attempt: 1-based attempt number this execution row represents.
            worker_id: Worker that owns the attempt, or ``None`` to leave it unset.
            conn: Existing connection to write within; opens its own when ``None``.
        """
        async with self._conn(conn) as c:
            await c.execute(
                SQL("""
                    INSERT INTO {executions} (job_id, worker_id, attempt, status)
                    VALUES (%(jid)s, %(wid)s, %(attempt)s, 'running')
                """).format(executions=self._t["executions"]),
                {"jid": job_id, "wid": worker_id, "attempt": attempt},
            )

    async def insert_many(self, jobs: list[JobInsert], conn: AsyncConnection | None = None) -> list[Job | None]:
        results: list[Job | None] = []
        notify_queues: set[str] = set()

        async with self._conn(conn, transaction=conn is None) as c, c.cursor(row_factory=dict_row) as cur:
            for data in jobs:
                await cur.execute(
                    SQL(self._INSERT_SQL + JOB_COLS).format(jobs=self._t["jobs"]),
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
                    if await self._check_dependencies(cur, str(job.id)):
                        notify_queues.update(await self.settle_dependents(cur, data.dep_ids[0][0]))
                else:
                    notify_queues.add(data.queue)

            for queue in notify_queues:
                await c.execute(SQL("NOTIFY {ch}").format(ch=Identifier(f"{self._prefix}:{queue}")))

        return results

    # ------------------------------------------------------------------
    # Inspection
    # ------------------------------------------------------------------

    async def get(self, job_id: str) -> Job:
        async with await self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
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
        queue: str | Sequence[str] | None = None,
        status: str | None = None,
        worker_id: str | None = None,
        search: str | None = None,
        schedule_name: str | None = None,
        retried: bool = False,
        limit: int = 50,
        offset: int = 0,
    ) -> list[Job]:
        filters = []
        params: dict[str, Any] = {"limit": limit, "offset": offset}
        if queue:
            if isinstance(queue, str):
                filters.append("queue = %(queue)s")
                params["queue"] = queue
            else:
                filters.append("queue = ANY(%(queue)s)")
                params["queue"] = list(queue)
        if status:
            filters.append("status = %(status)s")
            params["status"] = status
        if worker_id:
            filters.append("worker_id = %(worker_id)s")
            params["worker_id"] = worker_id
        if search:
            filters.append("(function ILIKE %(search)s OR id::text ILIKE %(search)s OR queue ILIKE %(search)s)")
            params["search"] = f"%{search}%"
        if schedule_name:
            filters.append("schedule_name = %(schedule_name)s")
            params["schedule_name"] = schedule_name
        if retried:
            filters.append("attempts > 0")
        where = SQL("WHERE " + " AND ".join(filters)) if filters else SQL("")
        async with await self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
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
        async with await self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
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
        async with await self._connect() as conn, conn.cursor() as cur:
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
        async with await self._connect() as conn, conn.transaction(), conn.cursor() as cur:
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
        async with await self._connect() as conn, conn.cursor() as cur:
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
        async with await self._connect() as conn, conn.transaction():
            await conn.execute(
                SQL("UPDATE {jobs} SET touched_at = NOW() WHERE id = %(id)s").format(jobs=self._t["jobs"]),
                {"id": job_id},
            )

    async def requeue(self, job_id: str) -> bool:
        async with await self._connect() as conn, conn.cursor() as cur:
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
                        max_attempts = max_attempts + attempts,
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

        async with await self._connect() as conn, conn.transaction(), conn.cursor(row_factory=dict_row) as cur:
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

        async with await self._connect() as conn:
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
    # Shared graph helpers (also used by WorkerRepository)
    # ------------------------------------------------------------------

    async def _check_dependencies(self, cur: Any, job_id: str) -> bool:
        """Check whether all dependencies of a job are in a terminal state.

        Used after inserting dep rows to detect the race where a dep completed
        before the dependent job was inserted — in that case settle_dependents
        already ran and will never fire again, so the caller must settle immediately.

        Args:
            cur: Open database cursor to execute the check against.
            job_id: UUID string of the waiting job whose deps are being checked.

        Returns:
            True if every dependency is complete, failed, or aborted; False if
            any dependency is still in a non-terminal state.
        """
        await cur.execute(
            SQL("""
                SELECT NOT EXISTS (
                    SELECT 1 FROM {deps} d
                    JOIN {jobs} jd ON jd.id = d.depends_on
                    WHERE d.job_id = %(jid)s::uuid
                      AND jd.status NOT IN ('complete', 'failed', 'aborted')
                ) AS all_settled
            """).format(deps=self._t["deps"], jobs=self._t["jobs"]),
            {"jid": job_id},
        )
        row = await cur.fetchone()
        if row is None:
            return False
        return bool(row["all_settled"] if isinstance(row, dict) else row[0])

    async def delete(self, job_id: str) -> None:
        async with await self._connect() as conn:
            await conn.execute(
                SQL("DELETE FROM {jobs} WHERE id = %(id)s").format(jobs=self._t["jobs"]),
                {"id": job_id},
            )

    async def bulk_requeue(self, queue: str | None = None, function_name: str | None = None) -> int:
        """Reset all failed or aborted jobs back to the queued state.

        Mirrors the single-job :meth:`requeue` logic: ``max_attempts`` is bumped
        by the current ``attempts`` count so prior attempt history is preserved.

        Args:
            queue: Restrict to this queue name; ``None`` targets all queues.
            function_name: Restrict to jobs with this function path; ``None``
                targets all functions.

        Returns:
            Number of jobs requeued.
        """
        filters = ["status IN ('failed', 'aborted')"]
        params: dict[str, Any] = {}
        if queue:
            filters.append("queue = %(queue)s")
            params["queue"] = queue
        if function_name:
            filters.append("function = %(function_name)s")
            params["function_name"] = function_name
        where = SQL(" AND ".join(filters))

        async with await self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
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
                        max_attempts = max_attempts + attempts,
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

        async with await self._connect() as conn, conn.cursor() as cur:
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
        async with await self._connect() as conn, conn.cursor() as cur:
            await cur.execute(
                SQL("""
                    DELETE FROM {jobs}
                    WHERE status = ANY(%(statuses)s)
                      AND enqueued_at < NOW() - make_interval(days => %(days)s)
                """).format(jobs=self._t["jobs"]),
                {"statuses": statuses, "days": older_than_days},
            )
            return cur.rowcount

    async def list_schedule_stats(self) -> list[dict[str, Any]]:
        async with await self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                SQL("""
                    SELECT
                        schedule_name,
                        MAX(function)                             AS function,
                        MAX(queue)                                AS queue,
                        COUNT(*)                                  AS total_runs,
                        COUNT(*) FILTER (WHERE status = 'failed') AS failed_runs,
                        (array_agg(status ORDER BY enqueued_at DESC))[1] AS last_status,
                        MAX(enqueued_at)                          AS last_enqueued_at,
                        MAX(completed_at)                         AS last_completed_at
                    FROM {jobs}
                    WHERE schedule_name IS NOT NULL
                    GROUP BY schedule_name
                    ORDER BY MAX(enqueued_at) DESC
                """).format(jobs=self._t["jobs"])
            )
            return await cur.fetchall()

    async def reschedule_stuck(self) -> int:
        async with await self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
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
        connect: Connect,
        tables: dict[str, Any],
        prefix: str,
        serializer: Serializer,
        job_repo: JobRepository,
    ) -> None:
        self._connect = connect
        self._t = tables
        self._prefix = prefix
        self._serializer = serializer
        self._job_repo = job_repo

    # ------------------------------------------------------------------
    # Registration & heartbeat
    # ------------------------------------------------------------------

    async def register(self, worker_id: str, name: str, queues: list[str], metadata: str, role: str = "worker") -> None:
        async with await self._connect() as conn, conn.transaction():
            await conn.execute(
                SQL("""
                    INSERT INTO {worker} (id, name, queue, status, role, metadata, heartbeat_at)
                    VALUES (%(id)s, %(name)s, %(queue)s, 'active', %(role)s, %(meta)s, NOW())
                    ON CONFLICT (id) DO UPDATE
                        SET name = EXCLUDED.name,
                            queue = EXCLUDED.queue,
                            status = 'active',
                            role = EXCLUDED.role,
                            metadata = EXCLUDED.metadata,
                            heartbeat_at = NOW()
                """).format(worker=self._t["worker"]),
                {"id": worker_id, "name": name, "queue": ",".join(queues), "role": role, "meta": metadata},
            )

    async def deregister(self, worker_id: str) -> None:
        async with await self._connect() as conn, conn.transaction():
            await conn.execute(
                SQL("""
                    UPDATE {worker}
                    SET status = 'stopped', expires_at = NOW() + INTERVAL '1 hour'
                    WHERE id = %(id)s
                """).format(worker=self._t["worker"]),
                {"id": worker_id},
            )

    async def update_heartbeat(self, worker_id: str) -> None:
        async with await self._connect() as conn, conn.transaction():
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

        async with await self._connect() as conn, conn.transaction(), conn.cursor(row_factory=dict_row) as cur:
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
        async with await self._connect() as conn, conn.transaction(), conn.cursor() as cur:
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

        async with await self._connect() as conn, conn.transaction(), conn.cursor() as cur:
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
        async with await self._connect() as conn, conn.transaction():
            await conn.execute(
                SQL("DELETE FROM {jobs} WHERE id = %(id)s").format(jobs=self._t["jobs"]),
                {"id": job_id},
            )

    async def requeue_cancelled(self, worker_id: str, job: Job) -> None:
        async with await self._connect() as conn, conn.transaction(), conn.cursor() as cur:
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
        async with await self._connect() as conn:
            await conn.execute(SQL("NOTIFY {ch}").format(ch=Identifier(f"{self._prefix}:{job.queue}")))

    # ------------------------------------------------------------------
    # Abort polling
    # ------------------------------------------------------------------

    async def get_aborting(self, job_ids: list[str]) -> list[str]:
        async with await self._connect() as conn, conn.cursor() as cur:
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
        async with await self._connect() as conn:
            await conn.execute(SQL("NOTIFY {ch}").format(ch=Identifier(f"{self._prefix}:{queue}")))

    async def fetch(self) -> list[dict[str, Any]]:
        async with await self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                SQL("""
                    SELECT id::text, name, queue, status, role, metadata, heartbeat_at, started_at, expires_at
                    FROM {worker}
                    ORDER BY started_at DESC
                """).format(worker=self._t["worker"])
            )
            return await cur.fetchall()

    async def get(self, worker_id: str) -> dict[str, Any] | None:
        async with await self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                SQL("""
                    SELECT id::text, name, queue, status, role, metadata, heartbeat_at, started_at, expires_at
                    FROM {worker}
                    WHERE id = %(id)s
                """).format(worker=self._t["worker"]),
                {"id": worker_id},
            )
            return await cur.fetchone()

    async def list_jobs(self, worker_id: str, limit: int = 50, offset: int = 0) -> list[Job]:
        async with await self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
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
        connect: Connect,
        tables: dict[str, Any],
    ) -> None:
        self._connect = connect
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
        async with await self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
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
        async with await self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
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
        async with await self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                SQL(
                    self._SAMPLE_GRID
                    + """
                    , relevant_jobs AS (
                        SELECT id, enqueued_at, scheduled_at, started_at, completed_at
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
                              AND j.scheduled_at <= s.ts
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

    async def get_server_info(self, prefix: str) -> tuple[str, int, int, list[dict[str, Any]]]:
        async with await self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
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

        pgwerk_size_bytes: int = sum(r["size_bytes"] for r in table_rows)
        return pg_version, db_size_bytes, pgwerk_size_bytes, table_rows


# ---------------------------------------------------------------------------
# Schedules
# ---------------------------------------------------------------------------


class ScheduleAlreadyExists(Exception):
    """Raised when registering a schedule whose name is already in the DB."""


class ScheduleNotFound(Exception):
    """Raised when updating or deleting a schedule that does not exist."""


class ScheduleRepository:
    """Persistence for recurring-job definitions in ``_pgwerk_schedules``."""

    def __init__(
        self,
        connect: Connect,
        tables: dict[str, Any],
        prefix: str,
        serializer: Serializer,
    ) -> None:
        self._connect = connect
        self._t = tables
        self._prefix = prefix
        self._serializer = serializer

    # ------------------------------------------------------------------
    # CRUD
    # ------------------------------------------------------------------

    async def insert(self, schedule: Schedule) -> Schedule:
        """Insert a new schedule row. Raises if a row with this name already exists.

        If ``schedule.next_run_at`` is set it is honored as-is; otherwise it is
        computed from the schedule's policy.
        """
        next_run_at = schedule.next_run_at or compute_next_run(schedule.interval_secs, schedule.cron)
        async with await self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
            try:
                await cur.execute(
                    SQL("""
                        INSERT INTO {schedules} (
                            name, function, queue, args, kwargs,
                            interval_secs, cron, timeout_secs,
                            result_ttl, failure_ttl, meta, paused,
                            next_run_at, last_registered_at
                        ) VALUES (
                            %(name)s, %(function)s, %(queue)s, %(args)s, %(kwargs)s,
                            %(interval_secs)s, %(cron)s, %(timeout_secs)s,
                            %(result_ttl)s, %(failure_ttl)s, %(meta)s, %(paused)s,
                            %(next_run_at)s, NOW()
                        )
                        RETURNING *
                    """).format(schedules=self._t["schedules"]),
                    {
                        "name": schedule.name,
                        "function": schedule.function,
                        "queue": schedule.queue,
                        "args": encode(self._serializer, list(schedule.args)) if schedule.args else None,
                        "kwargs": encode(self._serializer, schedule.kwargs) if schedule.kwargs else None,
                        "interval_secs": schedule.interval_secs,
                        "cron": schedule.cron,
                        "timeout_secs": schedule.timeout_secs,
                        "result_ttl": schedule.result_ttl,
                        "failure_ttl": schedule.failure_ttl,
                        "meta": encode(self._serializer, schedule.meta),
                        "paused": schedule.paused,
                        "next_run_at": next_run_at,
                    },
                )
            except psycopg.errors.UniqueViolation as exc:
                raise ScheduleAlreadyExists(
                    f"schedule {schedule.name!r} is already registered; "
                    "call update() to modify it or unregister() to replace it"
                ) from exc
            row = await cur.fetchone()
            assert row is not None
            return Schedule.from_row(row, self._serializer)

    async def update(self, name: str, **fields: Any) -> Schedule:
        """Update mutable fields on an existing schedule row.

        When ``interval_secs`` or ``cron`` is supplied the other is cleared and
        ``next_run_at`` is recomputed from the new policy — unless ``next_run_at``
        was also supplied explicitly, in which case the caller's value wins
        (used by ``schedule_at()`` / ``schedule_in()`` to re-anchor the first run).
        Unknown or ``None``-by-default fields are left untouched; pass them
        explicitly to clear.
        """
        allowed = {
            "function",
            "queue",
            "args",
            "kwargs",
            "interval_secs",
            "cron",
            "timeout_secs",
            "result_ttl",
            "failure_ttl",
            "meta",
            "paused",
            "next_run_at",
        }
        updates: dict[str, Any] = {k: v for k, v in fields.items() if k in allowed}
        if "interval_secs" in updates:
            updates.setdefault("cron", None)
        if "cron" in updates:
            updates.setdefault("interval_secs", None)
        if "args" in updates:
            updates["args"] = encode(self._serializer, list(updates["args"])) if updates["args"] else None
        if "kwargs" in updates:
            updates["kwargs"] = encode(self._serializer, updates["kwargs"]) if updates["kwargs"] else None
        if "meta" in updates:
            updates["meta"] = encode(self._serializer, updates["meta"])

        # Recompute next_run_at when the policy changes — but only if the caller
        # did not supply next_run_at explicitly.
        recompute = ("interval_secs" in updates or "cron" in updates) and "next_run_at" not in updates

        async with await self._connect() as conn, conn.transaction(), conn.cursor(row_factory=dict_row) as cur:
            if recompute:
                await cur.execute(
                    SQL("SELECT interval_secs, cron FROM {schedules} WHERE name = %(name)s FOR UPDATE").format(
                        schedules=self._t["schedules"]
                    ),
                    {"name": name},
                )
                existing = await cur.fetchone()
                if existing is None:
                    raise ScheduleNotFound(f"schedule {name!r} not found")
                merged_interval = updates.get("interval_secs", existing["interval_secs"])
                merged_cron = updates.get("cron", existing["cron"])
                updates["next_run_at"] = compute_next_run(merged_interval, merged_cron)

            if not updates:
                await cur.execute(
                    SQL("SELECT * FROM {schedules} WHERE name = %(name)s").format(schedules=self._t["schedules"]),
                    {"name": name},
                )
                row = await cur.fetchone()
                if row is None:
                    raise ScheduleNotFound(f"schedule {name!r} not found")
                return Schedule.from_row(row, self._serializer)

            set_sql = SQL(", ").join(SQL("{c} = {p}").format(c=Identifier(k), p=Placeholder(k)) for k in updates)
            await cur.execute(
                SQL("UPDATE {schedules} SET {set_sql} WHERE name = %(name)s RETURNING *").format(
                    schedules=self._t["schedules"], set_sql=set_sql
                ),
                {**updates, "name": name},
            )
            row = await cur.fetchone()
            if row is None:
                raise ScheduleNotFound(f"schedule {name!r} not found")
            return Schedule.from_row(row, self._serializer)

    async def upsert(self, schedule: Schedule) -> Schedule:
        """Insert or update a schedule row and return the resulting state.

        If ``schedule.next_run_at`` is set it is honored (useful for delayed-start
        schedules); otherwise it is computed from the policy on both insert and
        update. This is the entry point for imperative registrations from the
        scheduler's ``schedule()`` / ``schedule_at()`` / ``schedule_in()``.
        """
        try:
            return await self.insert(schedule)
        except ScheduleAlreadyExists:
            update_fields: dict[str, Any] = {
                "function": schedule.function,
                "queue": schedule.queue,
                "args": list(schedule.args) if schedule.args else [],
                "kwargs": dict(schedule.kwargs) if schedule.kwargs else {},
                "interval_secs": schedule.interval_secs,
                "cron": schedule.cron,
                "timeout_secs": schedule.timeout_secs,
                "result_ttl": schedule.result_ttl,
                "failure_ttl": schedule.failure_ttl,
                "meta": schedule.meta,
            }
            if schedule.next_run_at is not None:
                update_fields["next_run_at"] = schedule.next_run_at
            return await self.update(schedule.name, **update_fields)

    async def delete(self, name: str) -> bool:
        async with await self._connect() as conn, conn.cursor() as cur:
            await cur.execute(
                SQL("DELETE FROM {schedules} WHERE name = %(name)s").format(schedules=self._t["schedules"]),
                {"name": name},
            )
            return cur.rowcount > 0

    async def get(self, name: str) -> Schedule | None:
        async with await self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                SQL("SELECT * FROM {schedules} WHERE name = %(name)s").format(schedules=self._t["schedules"]),
                {"name": name},
            )
            row = await cur.fetchone()
        return Schedule.from_row(row, self._serializer) if row else None

    async def list_all(self) -> list[Schedule]:
        async with await self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(SQL("SELECT * FROM {schedules} ORDER BY name").format(schedules=self._t["schedules"]))
            rows = await cur.fetchall()
        return [Schedule.from_row(r, self._serializer) for r in rows]

    async def list_names(self) -> list[str]:
        async with await self._connect() as conn, conn.cursor() as cur:
            await cur.execute(SQL("SELECT name FROM {schedules}").format(schedules=self._t["schedules"]))
            rows = await cur.fetchall()
        return [r[0] for r in rows]

    # ------------------------------------------------------------------
    # Tick
    # ------------------------------------------------------------------

    async def tick_once(self, enqueue_one: Callable[[Schedule], Any], limit: int = 100) -> int:
        """Atomically fire all due schedules.

        For each schedule returned by ``SELECT ... FOR NO KEY UPDATE SKIP LOCKED``,
        invokes ``enqueue_one(schedule)`` and advances ``next_run_at`` /
        ``last_run_at`` in the same transaction. Returns the number fired.

        Args:
            enqueue_one: Async callable that enqueues a job for a schedule.
                It must not swallow exceptions it cannot handle — raising
                aborts the whole tick transaction (nothing is marked run).
            limit: Maximum schedules to fire in a single tick.
        """
        fired = 0
        # FOR NO KEY UPDATE (not FOR UPDATE): enqueue_one() inserts a job on a
        # fresh connection whose FK check grabs FOR KEY SHARE on the schedule
        # row. FOR UPDATE would block that; FOR NO KEY UPDATE does not.
        async with await self._connect() as conn, conn.transaction(), conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                SQL("""
                    SELECT * FROM {schedules}
                    WHERE NOT paused
                      AND next_run_at IS NOT NULL
                      AND next_run_at <= NOW()
                    ORDER BY next_run_at
                    LIMIT %(limit)s
                    FOR NO KEY UPDATE SKIP LOCKED
                """).format(schedules=self._t["schedules"]),
                {"limit": limit},
            )
            rows = await cur.fetchall()
            if not rows:
                return 0

            now = datetime.now(timezone.utc)
            for row in rows:
                schedule = Schedule.from_row(row, self._serializer)
                await enqueue_one(schedule)
                next_run = compute_next_run(schedule.interval_secs, schedule.cron, base=now)
                await cur.execute(
                    SQL("""
                        UPDATE {schedules}
                        SET last_run_at = %(now)s, next_run_at = %(next)s
                        WHERE name = %(name)s
                    """).format(schedules=self._t["schedules"]),
                    {"now": now, "next": next_run, "name": schedule.name},
                )
                fired += 1
        return fired

    async def seconds_until_next_due(self, fallback: float = 60.0) -> float:
        """Return seconds until the next un-paused schedule is due.

        Returns ``fallback`` when no schedules are pending. Clamps to ``0``
        when overdue.
        """
        async with await self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                SQL("""
                    SELECT EXTRACT(EPOCH FROM (next_run_at - NOW()))::float AS secs
                    FROM {schedules}
                    WHERE NOT paused AND next_run_at IS NOT NULL
                    ORDER BY next_run_at
                    LIMIT 1
                """).format(schedules=self._t["schedules"])
            )
            row = await cur.fetchone()
        if row is None or row.get("secs") is None:
            return fallback
        return max(0.0, float(row["secs"]))

    # ------------------------------------------------------------------
    # Trigger (manual)
    # ------------------------------------------------------------------

    async def trigger(self, name: str) -> Schedule | None:
        """Advance ``next_run_at`` to NOW so the next tick fires this schedule.

        Returns the updated Schedule, or ``None`` if no schedule exists.
        """
        async with await self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                SQL("""
                    UPDATE {schedules}
                    SET next_run_at = NOW()
                    WHERE name = %(name)s
                    RETURNING *
                """).format(schedules=self._t["schedules"]),
                {"name": name},
            )
            row = await cur.fetchone()
        return Schedule.from_row(row, self._serializer) if row else None

    # ------------------------------------------------------------------
    # Registration reconciliation (orphan handling)
    # ------------------------------------------------------------------

    async def reconcile(self, known_names: list[str], on_unregistered: str) -> list[str]:
        """Apply the on_unregistered policy to schedules not in *known_names*.

        Args:
            known_names: Names currently registered in-process.
            on_unregistered: One of ``'keep'``, ``'pause'``, ``'delete'``.

        Returns:
            List of schedule names that were affected (paused or deleted).
            Empty for ``'keep'``.
        """
        if on_unregistered == "keep":
            return []
        if on_unregistered not in ("pause", "delete"):
            raise ValueError(f"Invalid on_unregistered: {on_unregistered!r}")

        async with await self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
            if on_unregistered == "pause":
                await cur.execute(
                    SQL("""
                        UPDATE {schedules}
                        SET paused = TRUE
                        WHERE name <> ALL(%(known)s::text[])
                          AND NOT paused
                        RETURNING name
                    """).format(schedules=self._t["schedules"]),
                    {"known": known_names},
                )
            else:  # delete
                await cur.execute(
                    SQL("""
                        DELETE FROM {schedules}
                        WHERE name <> ALL(%(known)s::text[])
                        RETURNING name
                    """).format(schedules=self._t["schedules"]),
                    {"known": known_names},
                )
            rows = await cur.fetchall()
        return [r["name"] for r in rows]
