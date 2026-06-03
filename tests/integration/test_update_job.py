"""Integration tests for update_job."""

from __future__ import annotations

from datetime import datetime
from datetime import timezone
from datetime import timedelta

import pytest

from pgwerk.commons import JobStatus

from .tasks import noop
from .conftest import make_worker


class TestUpdateJobByKey:
    async def test_reschedule_future(self, app):
        """update_job with a future time keeps the job in scheduled status."""
        key = "update-key-reschedule-future"
        job = await app.enqueue(noop, _key=key, _delay=60)
        assert job is not None

        future = datetime.now(timezone.utc) + timedelta(hours=1)
        updated = await app.update_job(key=key, at=future)

        assert updated is not None
        assert updated.id == job.id
        assert updated.status == JobStatus.Scheduled
        assert abs((updated.scheduled_at - future).total_seconds()) < 1

    async def test_reschedule_past_becomes_queued(self, app):
        """update_job with a past time transitions the job to queued."""
        key = "update-key-reschedule-past"
        job = await app.enqueue(noop, _key=key, _delay=3600)
        assert job is not None

        past = datetime.now(timezone.utc) - timedelta(seconds=10)
        updated = await app.update_job(key=key, at=past)

        assert updated is not None
        assert updated.status == JobStatus.Queued

    async def test_reschedule_with_delay(self, app):
        """update_job with delay= computes scheduled_at from now."""
        key = "update-key-reschedule-delay"
        job = await app.enqueue(noop, _key=key, _delay=3600)
        assert job is not None

        updated = await app.update_job(key=key, delay=7200)

        assert updated is not None
        assert updated.id == job.id
        assert updated.status == JobStatus.Scheduled
        expected = datetime.now(timezone.utc) + timedelta(seconds=7200)
        assert abs((updated.scheduled_at - expected).total_seconds()) < 2

    async def test_update_priority(self, app):
        """update_job changes the priority of a pending job."""
        key = "update-key-priority"
        job = await app.enqueue(noop, _key=key, _priority=0)
        assert job is not None

        updated = await app.update_job(key=key, priority=10)

        assert updated is not None
        assert updated.priority == 10

    async def test_update_meta(self, app):
        """update_job replaces the meta dict of a pending job."""
        key = "update-key-meta"
        job = await app.enqueue(noop, _key=key, _meta={"v": 1})
        assert job is not None

        updated = await app.update_job(key=key, meta={"v": 2, "extra": True})

        assert updated is not None
        assert updated.meta == {"v": 2, "extra": True}

    async def test_update_multiple_fields(self, app):
        """update_job can change several fields in one call."""
        key = "update-key-multi"
        job = await app.enqueue(noop, _key=key, _delay=3600, _priority=0)
        assert job is not None

        future = datetime.now(timezone.utc) + timedelta(hours=2)
        updated = await app.update_job(key=key, at=future, priority=5, meta={"source": "test"})

        assert updated is not None
        assert updated.status == JobStatus.Scheduled
        assert updated.priority == 5
        assert updated.meta == {"source": "test"}

    async def test_nonexistent_key_returns_none(self, app):
        """update_job returns None when no pending job matches the key."""
        result = await app.update_job(key="no-such-key", priority=1)
        assert result is None

    async def test_completed_job_returns_none(self, app):
        """update_job cannot modify a job that has already completed."""
        key = "update-key-completed"
        job = await app.enqueue(noop, _key=key)
        assert job is not None
        await make_worker(app).run()
        assert (await app.get_job(job.id)).status == JobStatus.Complete

        result = await app.update_job(key=key, priority=99)
        assert result is None


class TestUpdateJobById:
    async def test_reschedule_future(self, app):
        """update_job by job_id with a future time keeps the job scheduled."""
        job = await app.enqueue(noop, _delay=60)
        assert job is not None

        future = datetime.now(timezone.utc) + timedelta(hours=1)
        updated = await app.update_job(job.id, at=future)

        assert updated is not None
        assert updated.id == job.id
        assert updated.status == JobStatus.Scheduled
        assert abs((updated.scheduled_at - future).total_seconds()) < 1

    async def test_reschedule_past_becomes_queued(self, app):
        """update_job by job_id with a past time transitions the job to queued."""
        job = await app.enqueue(noop, _delay=3600)
        assert job is not None

        past = datetime.now(timezone.utc) - timedelta(seconds=10)
        updated = await app.update_job(job.id, at=past)

        assert updated is not None
        assert updated.status == JobStatus.Queued

    async def test_update_priority(self, app):
        """update_job by job_id changes the priority."""
        job = await app.enqueue(noop, _priority=0)
        assert job is not None

        updated = await app.update_job(job.id, priority=10)

        assert updated is not None
        assert updated.priority == 10

    async def test_nonexistent_id_returns_none(self, app):
        """update_job returns None when no pending job matches the ID."""
        result = await app.update_job("00000000-0000-0000-0000-000000000000", priority=1)
        assert result is None


class TestUpdateJobValidation:
    async def test_no_fields_returns_none(self, app):
        """update_job with no fields to update returns None."""
        job = await app.enqueue(noop, _key="update-noop")
        assert job is not None

        result = await app.update_job(key="update-noop")
        assert result is None

    async def test_neither_id_nor_key_raises(self, app):
        """update_job raises when neither job_id nor key is provided."""
        with pytest.raises(ValueError, match="exactly one"):
            await app.update_job(priority=1)

    async def test_both_id_and_key_raises(self, app):
        """update_job raises when both job_id and key are provided."""
        job = await app.enqueue(noop, _key="update-both")
        assert job is not None

        with pytest.raises(ValueError, match="exactly one"):
            await app.update_job(job.id, key="update-both", priority=1)
