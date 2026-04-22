"""Integration tests for ack and nack race detection."""

from __future__ import annotations

import logging

from unittest.mock import patch

import pytest

from psycopg.sql import SQL

from wrk.worker.aio import AsyncWorker

from .tasks import noop
from .tasks import fail_always
from .tasks import clear_callback_log
from .conftest import make_worker


@pytest.fixture(autouse=True)
def _clear_cbs():
    clear_callback_log()
    yield
    clear_callback_log()


class TestRaceDetection:
    async def test_ack_race_logs_warning_when_worker_id_stolen(self, app, caplog):
        """If worker_id is changed mid-flight, _ack is a no-op and logs a warning."""
        await app.enqueue(noop)
        pool = app._pool_or_raise()

        original_ack = AsyncWorker._ack

        async def ack_with_stolen_ownership(self, job, result=None):
            # Steal the worker_id before the ack UPDATE executes
            async with pool.connection() as conn:
                await conn.execute(
                    SQL("UPDATE {jobs} SET worker_id = gen_random_uuid() WHERE id = %s").format(jobs=app._t["jobs"]),
                    (job.id,),
                )
            await original_ack(self, job, result)

        with patch.object(AsyncWorker, "_ack", ack_with_stolen_ownership):
            with caplog.at_level(logging.WARNING, logger="wrk.worker.base"):
                await make_worker(app).run()

        assert any("ack race" in r.message.lower() for r in caplog.records)

    async def test_ack_race_does_not_overwrite_stolen_job(self, app):
        """When ack race is detected, the job row is not overwritten with our result."""
        job = await app.enqueue(noop)
        pool = app._pool_or_raise()

        original_ack = AsyncWorker._ack

        async def ack_after_steal(self, j, result=None):
            # Change worker_id and mark complete from another worker
            async with pool.connection() as conn:
                await conn.execute(
                    SQL("""
                        UPDATE {jobs}
                        SET worker_id = gen_random_uuid(), status = 'complete', result = '"stolen"'
                        WHERE id = %s
                    """).format(jobs=app._t["jobs"]),
                    (j.id,),
                )
            await original_ack(self, j, result)

        with patch.object(AsyncWorker, "_ack", ack_after_steal):
            await make_worker(app).run()

        # The stolen result should remain (our ack was a no-op)
        done = await app.get_job(job.id)
        assert done.result == "stolen"

    async def test_nack_race_logs_warning(self, app, caplog):
        """If worker_id is stolen before nack, nack is a no-op and logs a warning."""
        await app.enqueue(fail_always)
        pool = app._pool_or_raise()

        original_nack = AsyncWorker._nack

        async def nack_after_steal(self, j, error, **kwargs):
            async with pool.connection() as conn:
                await conn.execute(
                    SQL("UPDATE {jobs} SET worker_id = gen_random_uuid() WHERE id = %s").format(jobs=app._t["jobs"]),
                    (j.id,),
                )
            await original_nack(self, j, error, **kwargs)

        with patch.object(AsyncWorker, "_nack", nack_after_steal):
            with caplog.at_level(logging.WARNING, logger="wrk.worker.base"):
                await make_worker(app).run()

        assert any("nack race" in r.message.lower() for r in caplog.records)
