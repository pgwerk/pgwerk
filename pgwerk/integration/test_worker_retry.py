"""Integration tests for worker retry logic."""

from __future__ import annotations

import logging

from unittest.mock import patch

import pytest

from psycopg.sql import SQL

from pgwerk.commons import JobStatus
from pgwerk.worker.aio import AsyncWorker

from .tasks import noop
from .tasks import fail_once
from .tasks import fail_always
from .tasks import clear_callback_log
from .conftest import make_worker


async def _fast_ack_with_retry(worker, job, result, max_attempts=5):
    """_ack_with_retry without sleep delays — for testing the retry logic only."""
    import psycopg

    _logger = logging.getLogger("wrk.worker.base")
    for attempt in range(1, max_attempts + 1):
        try:
            await worker._ack(job, result)
            return
        except (psycopg.OperationalError, psycopg.InterfaceError) as exc:
            if attempt == max_attempts:
                _logger.critical(
                    "Worker %s: ack failed after %d attempts for job %s [%s], sweep will recover: %s",
                    worker.name,
                    max_attempts,
                    job.function,
                    job.id,
                    exc,
                )
                return
            _logger.warning(
                "Worker %s: transient ack error for job %s (attempt %d/%d), retrying in %.1fs: %s",
                worker.name,
                job.id,
                attempt,
                max_attempts,
                0.0,
                exc,
            )


@pytest.fixture(autouse=True)
def _clear_callbacks():
    clear_callback_log()
    yield
    clear_callback_log()


class TestWorkerRetry:
    async def test_all_attempts_exhausted_results_in_failed(self, app):
        job = await app.enqueue(fail_always, _retry=3)
        await make_worker(app).run()
        done = await app.get_job(job.id)
        assert done.status == JobStatus.Failed
        assert done.attempts == 3

    async def test_retry_succeeds_on_second_attempt(self, app):
        job = await app.enqueue(fail_once, _retry=3)
        await make_worker(app).run()
        done = await app.get_job(job.id)
        assert done.status == JobStatus.Complete
        assert done.result == "recovered"
        assert done.attempts == 2

    async def test_multiple_executions_recorded_per_retry(self, app):
        job = await app.enqueue(fail_always, _retry=3)
        await make_worker(app).run()
        execs = await app.get_executions(job.id)
        assert len(execs) == 3

    async def test_retry_interval_reschedules_in_future(self, app):
        from datetime import datetime
        from datetime import timezone

        from pgwerk.schemas import Retry

        job = await app.enqueue(fail_always, _retry=Retry(max=2, intervals=[3600]))
        await make_worker(app).run()
        done = await app.get_job(job.id)
        assert done.status == JobStatus.Scheduled
        assert done.scheduled_at > datetime.now(timezone.utc)

    async def test_ack_with_retry_retries_on_transient_error(self, app, caplog):
        import psycopg

        job = await app.enqueue(noop)

        call_count = 0
        original_ack = AsyncWorker._ack

        async def flaky_ack(self, j, result=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise psycopg.OperationalError("transient connection error")
            await original_ack(self, j, result)

        with patch.object(AsyncWorker, "_ack", flaky_ack):
            with patch.object(
                AsyncWorker, "_ack_with_retry", lambda self, job, result, **_kw: _fast_ack_with_retry(self, job, result)
            ):
                with caplog.at_level(logging.WARNING, logger="wrk.worker.base"):
                    await make_worker(app).run()

        assert call_count == 2
        done = await app.get_job(job.id)
        assert done.status == JobStatus.Complete

    async def test_ack_with_retry_gives_up_after_max_attempts(self, app, caplog):
        import psycopg

        job = await app.enqueue(noop)

        async def always_fail_ack(_self, _j, _result=None):
            raise psycopg.OperationalError("persistent connection error")

        with patch.object(AsyncWorker, "_ack", always_fail_ack):
            with patch.object(
                AsyncWorker, "_ack_with_retry", lambda self, job, result, **_kw: _fast_ack_with_retry(self, job, result)
            ):
                with caplog.at_level(logging.CRITICAL, logger="wrk.worker.base"):
                    await make_worker(app).run()

        assert any("ack failed after" in r.message.lower() for r in caplog.records)

    async def test_requeue_failed_job(self, app):
        job = await app.enqueue(fail_always, _retry=1)
        await make_worker(app).run()
        done = await app.get_job(job.id)
        assert done.status == JobStatus.Failed

        requeued = await app.requeue_job(job.id)
        assert requeued is True
        refreshed = await app.get_job(job.id)
        assert refreshed.status == JobStatus.Queued
        assert refreshed.attempts == 0
        assert refreshed.error is None

    async def test_requeue_allows_job_to_run_again(self, app):
        job = await app.enqueue(fail_always, _retry=1)
        await make_worker(app).run()
        await app.requeue_job(job.id)

        pool = app._pool_or_raise()
        async with pool.connection() as conn:
            await conn.execute(
                SQL("UPDATE {jobs} SET function = 'tests.integration.tasks.noop' WHERE id = %s").format(
                    jobs=app._t["jobs"]
                ),
                (job.id,),
            )
        await make_worker(app).run()
        done = await app.get_job(job.id)
        assert done.status == JobStatus.Complete

    async def test_requeue_returns_false_for_queued_job(self, app):
        job = await app.enqueue(noop)
        result = await app.requeue_job(job.id)
        assert result is False

    async def test_requeue_returns_false_for_active_job(self, app):
        job = await app.enqueue(noop)
        pool = app._pool_or_raise()
        async with pool.connection() as conn:
            await conn.execute(
                SQL("UPDATE {jobs} SET status='active', worker_id=gen_random_uuid() WHERE id=%s").format(
                    jobs=app._t["jobs"]
                ),
                (job.id,),
            )
        result = await app.requeue_job(job.id)
        assert result is False
