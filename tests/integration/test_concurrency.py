"""Integration tests for multiple concurrent workers (SKIP LOCKED correctness)."""

from __future__ import annotations

import asyncio

from pgwerk.commons import JobStatus

from .tasks import add
from .tasks import noop
from .tasks import fail_always
from .conftest import make_worker


class TestMultipleAsyncWorkers:
    async def test_each_job_processed_exactly_once(self, app):
        """SKIP LOCKED guarantees each job is claimed by exactly one worker."""
        N = 20
        for _ in range(N):
            await app.enqueue(noop)

        workers = [make_worker(app, concurrency=5) for _ in range(4)]
        await asyncio.gather(*[w.run() for w in workers])

        jobs = await app.list_jobs(limit=100)
        assert len(jobs) == N
        assert all(j.status == JobStatus.Complete for j in jobs)
        assert all(j.attempts == 1 for j in jobs), "Each job must be attempted exactly once"

    async def test_all_jobs_complete_with_multiple_workers(self, app):
        """All jobs finish even when workers start simultaneously."""
        for i in range(15):
            await app.enqueue(add, i, i)

        await asyncio.gather(*[make_worker(app, concurrency=4).run() for _ in range(3)])

        jobs = await app.list_jobs(limit=100)
        assert all(j.status == JobStatus.Complete for j in jobs)

    async def test_mixed_success_failure_across_workers(self, app):
        """Workers correctly handle success and failure jobs in parallel."""
        for _ in range(5):
            await app.enqueue(noop)
        for _ in range(5):
            await app.enqueue(fail_always, _retry=1)

        await asyncio.gather(*[make_worker(app, concurrency=5).run() for _ in range(2)])

        jobs = await app.list_jobs(limit=100)
        complete = [j for j in jobs if j.status == JobStatus.Complete]
        failed = [j for j in jobs if j.status == JobStatus.Failed]
        assert len(complete) == 5
        assert len(failed) == 5

    async def test_workers_on_separate_queues_dont_interfere(self, app):
        """Workers listening on different queues each drain their own queue."""
        for _ in range(5):
            await app.enqueue(noop, _queue="q-alpha")
        for _ in range(5):
            await app.enqueue(noop, _queue="q-beta")

        w1 = make_worker(app, queues=["q-alpha"], concurrency=5)
        w2 = make_worker(app, queues=["q-beta"], concurrency=5)
        await asyncio.gather(w1.run(), w2.run())

        all_jobs = await app.list_jobs(limit=100)
        assert all(j.status == JobStatus.Complete for j in all_jobs)
