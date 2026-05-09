"""Integration tests for the lazy-connect ergonomics.

Producers (e.g. an external service or RQ worker enqueueing into pgwerk)
must be able to construct a `Werk` and call `enqueue()` without ever
awaiting `connect()`, as long as the schema has been migrated by some
other process.
"""

from __future__ import annotations

import os

import pytest

from pgwerk.app import Werk
from pgwerk.commons import JobStatus

from .tasks import add


_TEST_DSN = os.environ.get("PGWERK_TEST_DSN", "postgresql://pgwerk:pgwerk@localhost/pgwerk_test")
_TEST_PREFIX = "_pgwerk"


class TestLazyConnect:
    @pytest.mark.asyncio
    async def test_enqueue_without_connect(self, app):
        # `app` has already migrated the schema. Simulate a separate
        # producer process by constructing a fresh Werk and never calling
        # connect() on it.
        producer = Werk(_TEST_DSN, prefix=_TEST_PREFIX, auto_migrate=False)
        assert producer._connected is False

        job = await producer.enqueue(add, 1, 2)
        assert job.id
        assert job.status == JobStatus.Queued
        assert producer._connected is False

        fetched = await app.get_job(job.id)
        assert fetched.id == job.id
        assert fetched.payload == {"args": [1, 2], "kwargs": {}}

    @pytest.mark.asyncio
    async def test_get_job_without_connect(self, app):
        seeded = await app.enqueue(add, 5, 6)

        observer = Werk(_TEST_DSN, prefix=_TEST_PREFIX, auto_migrate=False)
        fetched = await observer.get_job(seeded.id)
        assert fetched.id == seeded.id
        assert observer._connected is False

    def test_repo_construction_is_lazy(self):
        # No DSN connectivity required — repos are built on first property
        # access, not in __init__, and don't open connections.
        producer = Werk("postgresql://invalid-host/none", auto_migrate=False)
        repo = producer._job_repo
        assert repo is not None
        assert producer._job_repo is repo  # cached across accesses
