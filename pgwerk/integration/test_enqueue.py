"""Integration tests for the enqueue operation."""

from __future__ import annotations

from wrk.commons import JobStatus

from .tasks import add
from .tasks import noop


class TestEnqueue:
    async def test_returns_job(self, app):
        job = await app.enqueue(add, 1, 2)
        assert job is not None
        assert job.id
        assert job.function == "tests.integration.tasks.add"
        assert job.status == JobStatus.Queued
        assert job.queue == "default"

    async def test_payload_stored(self, app):
        job = await app.enqueue(add, 10, 20)
        fetched = await app.get_job(job.id)
        assert fetched.payload == {"args": [10, 20], "kwargs": {}}

    async def test_kwargs_stored(self, app):
        job = await app.enqueue(add, x=3, y=4)
        fetched = await app.get_job(job.id)
        assert fetched.payload["kwargs"] == {"x": 3, "y": 4}

    async def test_priority_stored(self, app):
        job = await app.enqueue(noop, _priority=5)
        fetched = await app.get_job(job.id)
        assert fetched.priority == 5

    async def test_queue_name_stored(self, app):
        job = await app.enqueue(noop, _queue="high")
        assert job.queue == "high"

    async def test_duplicate_key_returns_none(self, app):
        j1 = await app.enqueue(noop, _key="my-key")
        j2 = await app.enqueue(noop, _key="my-key")
        assert j1 is not None
        assert j2 is None

    async def test_delay_sets_scheduled_at(self, app):
        from datetime import datetime
        from datetime import timezone

        before = datetime.now(timezone.utc)
        job = await app.enqueue(noop, _delay=60)
        assert job.scheduled_at > before

    async def test_at_sets_scheduled_at(self, app):
        from datetime import datetime
        from datetime import timezone
        from datetime import timedelta

        future = datetime.now(timezone.utc) + timedelta(hours=1)
        job = await app.enqueue(noop, _at=future)
        assert abs((job.scheduled_at - future).total_seconds()) < 1

    async def test_max_attempts_stored(self, app):
        job = await app.enqueue(noop, _retry=3)
        fetched = await app.get_job(job.id)
        assert fetched.max_attempts == 3

    async def test_retry_intervals_stored(self, app):
        from wrk.schemas import Retry

        job = await app.enqueue(noop, _retry=Retry(max=3, intervals=[5, 10]))
        fetched = await app.get_job(job.id)
        assert fetched.retry_intervals == [5, 10]
        assert fetched.max_attempts == 3

    async def test_meta_stored(self, app):
        job = await app.enqueue(noop, _meta={"trace_id": "abc"})
        fetched = await app.get_job(job.id)
        assert fetched.meta == {"trace_id": "abc"}

    async def test_ttl_sets_expires_at(self, app):
        job = await app.enqueue(noop, _ttl=300)
        fetched = await app.get_job(job.id)
        assert fetched.expires_at is not None

    async def test_group_key_stored(self, app):
        job = await app.enqueue(noop, _group="batch-1")
        fetched = await app.get_job(job.id)
        assert fetched.group_key == "batch-1"

    async def test_timeout_stored(self, app):
        job = await app.enqueue(noop, _timeout=30)
        fetched = await app.get_job(job.id)
        assert fetched.timeout_secs == 30

    async def test_result_ttl_stored(self, app):
        job = await app.enqueue(noop, _result_ttl=600)
        fetched = await app.get_job(job.id)
        assert fetched.result_ttl == 600
