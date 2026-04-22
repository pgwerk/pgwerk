"""Integration tests for get_job."""

from __future__ import annotations

import uuid

import pytest

from pgwerk.exceptions import JobNotFound

from .tasks import noop


class TestGetJob:
    async def test_get_job_returns_job(self, app):
        job = await app.enqueue(noop)
        fetched = await app.get_job(job.id)
        assert fetched.id == job.id

    async def test_get_job_not_found_raises(self, app):
        with pytest.raises(JobNotFound):
            await app.get_job("00000000-0000-0000-0000-000000000000")

    async def test_get_job_raises_job_not_found(self, app):
        with pytest.raises(JobNotFound):
            await app.get_job(str(uuid.uuid4()))
