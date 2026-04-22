from __future__ import annotations

import uuid
import asyncio
import logging

from unittest.mock import patch

import pytest
import psycopg

from psycopg.sql import SQL

from .tasks import noop
from .tasks import slow_pure
from .tasks import async_slow
from .tasks import on_failure
from .tasks import _callback_log
from .tasks import _execution_log
from .tasks import record_execution
from .tasks import reset_blocking_state
from .conftest import _TEST_DSN
from .conftest import make_worker

from tests.app import Wrk
from tests.cron import CronScheduler
from tests.worker import AsyncWorker
from tests.commons import JobStatus
from tests.schemas import Retry


@pytest.fixture(autouse=True)
def _reset_blocking_helpers():
    reset_blocking_state()
    _callback_log.clear()
    yield
    reset_blocking_state()
    _callback_log.clear()


def _unique_prefix(suffix: str) -> str:
    return f"_pgwerk_{suffix}_{uuid.uuid4().hex[:8]}"


class TestProdPaths:
    async def test_stale_job_handoff_keeps_new_owner_result(self, app, caplog):
        job = await app.enqueue(slow_pure, seconds=1.0, _heartbeat=30)

        worker_a = AsyncWorker(
            app=app,
            queues=["default"],
            concurrency=1,
            burst=False,
            heartbeat_interval=60,
            poll_interval=0.05,
            sweep_interval=9999,
            abort_interval=0.05,
        )
        worker_b = make_worker(app, concurrency=1)

        task_a = asyncio.create_task(worker_a.run())

        try:
            deadline = asyncio.get_running_loop().time() + 5.0
            while asyncio.get_running_loop().time() < deadline:
                current = await app.get_job(job.id)
                if current.status == JobStatus.Active:
                    break
                await asyncio.sleep(0.05)
            else:
                raise AssertionError("job never became active on worker A")

            pool = app._pool_or_raise()
            async with pool.connection() as conn:
                await conn.execute(
                    SQL("""
                        UPDATE {jobs}
                        SET started_at = NOW() - INTERVAL '2 hours',
                            touched_at = NOW() - INTERVAL '2 hours',
                            worker_id = gen_random_uuid()
                        WHERE id = %(id)s
                    """).format(jobs=app._t["jobs"]),
                    {"id": job.id},
                )

            with caplog.at_level(logging.WARNING, logger="wrk.worker.base"):
                await worker_b._setup_executor()
                await worker_b._register()
                try:
                    reclaimed = await worker_b._dequeue(limit=1)
                    assert len(reclaimed) == 1
                    await worker_b._handle_job(reclaimed[0])

                    deadline = asyncio.get_running_loop().time() + 5.0
                    while asyncio.get_running_loop().time() < deadline:
                        if any("ack race" in r.message.lower() for r in caplog.records):
                            break
                        await asyncio.sleep(0.05)
                finally:
                    await worker_b._deregister()
                    await worker_b._teardown_executor()
        finally:
            worker_a._request_shutdown()
            await asyncio.wait_for(task_a, timeout=5.0)

        done = await app.get_job(job.id)
        assert done.status == JobStatus.Complete
        assert done.result == "done"
        assert any("ack race" in r.message.lower() for r in caplog.records)

        execs = await app.get_executions(job.id)
        assert len(execs) >= 2
        assert any(
            e.worker_id and e.worker_id.replace("-", "") == worker_b.id and e.status.value == "complete" for e in execs
        )
        assert not any(
            e.worker_id and e.worker_id.replace("-", "") == worker_a.id and e.status.value == "complete" for e in execs
        )

    async def test_ack_transient_db_failure_retries_without_rerunning_handler(self, app):
        job = await app.enqueue(record_execution, label="ack-retry", _on_failure=on_failure)
        pool = app._pool_or_raise()
        original_connection = pool.connection
        injected = False

        class FlakyConnection:
            def __init__(self, cm):
                self._cm = cm
                self._inner = None

            async def __aenter__(self):
                nonlocal injected
                if _execution_log and not injected:
                    injected = True
                    raise psycopg.OperationalError("simulated ack connection failure")
                self._inner = await self._cm.__aenter__()
                return self._inner

            async def __aexit__(self, exc_type, exc, tb):
                return await self._cm.__aexit__(exc_type, exc, tb)

        def flaky_connection():
            return FlakyConnection(original_connection())

        with patch.object(pool, "connection", flaky_connection):
            await make_worker(app, concurrency=1).run()

        done = await app.get_job(job.id)
        assert injected is True
        assert len(_execution_log) == 1
        assert done.status == JobStatus.Complete
        assert not any(event == "failure" and jid == job.id for event, jid in _callback_log)

    async def test_shutdown_requeue_preserves_retry_budget_for_next_real_failure(self, app):
        job = await app.enqueue(async_slow, seconds=2.0, _retry=Retry(max=2))

        worker = make_worker(app, burst=False, concurrency=1, shutdown_timeout=0.1)
        worker_task = asyncio.create_task(worker.run())

        await asyncio.sleep(0.15)
        worker._request_shutdown()
        await asyncio.wait_for(worker_task, timeout=5.0)

        refreshed = await app.get_job(job.id)
        assert refreshed.status == JobStatus.Queued
        assert refreshed.attempts == 0

        pool = app._pool_or_raise()
        async with pool.connection() as conn:
            await conn.execute(
                SQL("""
                    UPDATE {jobs}
                    SET function = %(fn)s,
                        payload = NULL
                    WHERE id = %(id)s
                """).format(jobs=app._t["jobs"]),
                {"id": job.id, "fn": "tests.integration.tasks.fail_once"},
            )

        await make_worker(app, concurrency=1).run()
        done = await app.get_job(job.id)
        assert done.status == JobStatus.Complete
        assert done.result == "recovered"
        assert done.attempts == 2

    async def test_worker_makes_progress_with_tight_connection_pool(self):
        app = Wrk(_TEST_DSN, prefix=_unique_prefix("pool"), min_pool_size=1, max_pool_size=2)
        await app.connect()
        try:
            for _ in range(8):
                await app.enqueue(noop)

            worker = AsyncWorker(
                app=app,
                queues=["default"],
                concurrency=4,
                burst=True,
                heartbeat_interval=1,
                poll_interval=0.05,
                sweep_interval=9999,
                abort_interval=0.05,
            )
            await asyncio.wait_for(worker.run(), timeout=10.0)

            jobs = await app.list_jobs(limit=20)
            assert len(jobs) == 8
            assert all(j.status == JobStatus.Complete for j in jobs)
        finally:
            await app.disconnect()

    async def test_cron_failover_deduplicates_same_tick(self):
        prefix = _unique_prefix("cron")
        app_a = Wrk(
            _TEST_DSN,
            prefix=prefix,
            config={"prefix": prefix, "cron_standby_retry_interval": 0.05},
            min_pool_size=1,
            max_pool_size=2,
        )
        app_b = Wrk(
            _TEST_DSN,
            prefix=prefix,
            config={"prefix": prefix, "cron_standby_retry_interval": 0.05},
            min_pool_size=1,
            max_pool_size=2,
        )
        await app_a.connect()
        await app_b.connect()
        try:
            scheduler_a = CronScheduler(app_a)
            scheduler_b = CronScheduler(app_b)
            scheduler_a.register(noop, name="cron.failover.noop", interval=60)
            scheduler_b.register(noop, name="cron.failover.noop", interval=60)

            task_a = asyncio.create_task(scheduler_a.run())
            task_b = asyncio.create_task(scheduler_b.run())

            await asyncio.sleep(0.2)
            scheduler_a.stop()
            await asyncio.wait_for(task_a, timeout=5.0)

            await asyncio.sleep(0.2)
            scheduler_b.stop()
            await asyncio.wait_for(task_b, timeout=5.0)

            jobs = await app_a.list_jobs(limit=20)
            cron_jobs = [j for j in jobs if j.cron_name == "cron.failover.noop"]
            assert scheduler_b.get("cron.failover.noop").last_run_at is not None
            assert len(cron_jobs) == 1
        finally:
            await app_a.disconnect()
            await app_b.disconnect()
