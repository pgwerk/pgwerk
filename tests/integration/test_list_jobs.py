"""Integration tests for list_jobs."""

from __future__ import annotations

from psycopg.sql import SQL

from .tasks import noop
from .tasks import add_pure

from tests.commons import JobStatus


class TestListJobs:
    async def test_list_all(self, app):
        await app.enqueue(noop)
        await app.enqueue(noop)
        jobs = await app.list_jobs()
        assert len(jobs) == 2

    async def test_filter_by_queue(self, app):
        await app.enqueue(noop, _queue="high")
        await app.enqueue(noop, _queue="low")
        high = await app.list_jobs(queue="high")
        assert len(high) == 1
        assert all(j.queue == "high" for j in high)

    async def test_filter_by_status(self, app):
        await app.enqueue(noop)
        jobs = await app.list_jobs(status="queued")
        assert all(j.status == JobStatus.Queued for j in jobs)

    async def test_limit(self, app):
        for _ in range(5):
            await app.enqueue(noop)
        jobs = await app.list_jobs(limit=3)
        assert len(jobs) == 3

    async def test_search_by_function_name(self, app):
        await app.enqueue(noop)
        await app.enqueue(add_pure, 1, 2)
        results = await app.list_jobs(search="noop")
        assert all("noop" in j.function for j in results)
        assert len(results) == 1

    async def test_search_by_queue_name(self, app):
        await app.enqueue(noop, _queue="find-me")
        await app.enqueue(noop, _queue="ignore-me")
        results = await app.list_jobs(search="find-me")
        assert len(results) == 1
        assert results[0].queue == "find-me"

    async def test_search_returns_empty_when_no_match(self, app):
        await app.enqueue(noop)
        results = await app.list_jobs(search="xyzzy-does-not-exist")
        assert results == []

    async def test_filter_by_worker_id(self, app):
        """worker_id filter returns only jobs claimed by that worker."""
        fake_wid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        job = await app.enqueue(noop)
        pool = app._pool_or_raise()
        async with pool.connection() as conn:
            await conn.execute(
                SQL("UPDATE {jobs} SET worker_id = %(wid)s::uuid WHERE id = %(id)s").format(jobs=app._t["jobs"]),
                {"wid": fake_wid, "id": job.id},
            )

        results = await app.list_jobs(worker_id=fake_wid)
        assert len(results) == 1
        assert results[0].id == job.id

    async def test_offset_pagination(self, app):
        for _ in range(6):
            await app.enqueue(noop)
        page1 = await app.list_jobs(limit=3, offset=0)
        page2 = await app.list_jobs(limit=3, offset=3)
        assert len(page1) == 3
        assert len(page2) == 3
        assert {j.id for j in page1}.isdisjoint({j.id for j in page2})
