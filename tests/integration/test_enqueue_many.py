"""Integration tests for enqueue_many functionality."""

from __future__ import annotations

import pytest

from wrk.commons import JobStatus
from wrk.schemas import EnqueueParams

from .tasks import add
from .tasks import noop
from .tasks import fail_always
from .tasks import clear_callback_log
from .conftest import make_worker


@pytest.fixture(autouse=True)
def _clear_cbs():
    clear_callback_log()
    yield
    clear_callback_log()


class TestEnqueueMany:
    async def test_enqueue_many_inserts_all_jobs(self, app):
        specs = [
            EnqueueParams(func=noop),
            EnqueueParams(func=add, args=(1, 2)),
            EnqueueParams(func=fail_always),
        ]
        jobs = await app.enqueue_many(specs)
        assert len(jobs) == 3
        assert all(j is not None for j in jobs)

    async def test_enqueue_many_jobs_run_successfully(self, app):
        specs = [EnqueueParams(func=noop) for _ in range(3)]
        jobs = await app.enqueue_many(specs)
        await make_worker(app).run()
        for j in jobs:
            done = await app.get_job(j.id)
            assert done.status == JobStatus.Complete

    async def test_enqueue_many_deduplicates_by_key(self, app):
        specs = [
            EnqueueParams(func=noop, key="same-key"),
            EnqueueParams(func=noop, key="same-key"),
        ]
        jobs = await app.enqueue_many(specs)
        assert jobs[0] is not None
        assert jobs[1] is None

    async def test_enqueue_many_inside_existing_connection(self, app):
        pool = app._pool_or_raise()
        specs = [EnqueueParams(func=noop), EnqueueParams(func=noop)]
        async with pool.connection() as conn, conn.transaction():
            jobs = await app.enqueue_many(specs, _conn=conn)
        assert len(jobs) == 2
        assert all(j is not None for j in jobs)

    async def test_all_jobs_inserted_atomically(self, app):
        specs = [EnqueueParams(func=noop) for _ in range(5)]
        jobs = await app.enqueue_many(specs)
        assert len(jobs) == 5
        assert all(j is not None for j in jobs)
        all_in_db = await app.list_jobs(limit=100)
        assert len(all_in_db) == 5

    async def test_duplicate_key_in_batch_resolves_to_none(self, app):
        specs = [
            EnqueueParams(func=noop, key="batch-key"),
            EnqueueParams(func=noop, key="batch-key"),
            EnqueueParams(func=noop, key="batch-key"),
        ]
        jobs = await app.enqueue_many(specs)
        non_none = [j for j in jobs if j is not None]
        assert len(non_none) == 1

    async def test_batch_dedup_across_sequential_calls(self, app):
        specs1 = [EnqueueParams(func=noop, key="cross-batch-key")]
        specs2 = [EnqueueParams(func=noop, key="cross-batch-key")]
        jobs1 = await app.enqueue_many(specs1)
        jobs2 = await app.enqueue_many(specs2)
        assert jobs1[0] is not None
        assert jobs2[0] is None

    async def test_batch_with_mixed_queues_all_complete(self, app):
        specs = [
            EnqueueParams(func=noop, queue="batch-q1"),
            EnqueueParams(func=noop, queue="batch-q2"),
            EnqueueParams(func=noop, queue="batch-q1"),
        ]
        jobs = await app.enqueue_many(specs)
        assert jobs[0].queue == "batch-q1"
        assert jobs[1].queue == "batch-q2"
        assert jobs[2].queue == "batch-q1"

        worker = make_worker(app, queues=["batch-q1", "batch-q2"])
        await worker.run()
        for j in jobs:
            done = await app.get_job(j.id)
            assert done.status == JobStatus.Complete
