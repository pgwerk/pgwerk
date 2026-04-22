from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from pgwerk.exceptions import JobError
from pgwerk.exceptions import WrkError
from pgwerk.exceptions import JobTimeout
from pgwerk.exceptions import JobNotFound
from pgwerk.exceptions import WorkerShutdown
from pgwerk.exceptions import DependencyFailed


class TestWrkErrorHierarchy:
    def test_wrk_error_is_exception(self):
        e = WrkError("base")
        assert isinstance(e, Exception)

    def test_job_not_found_is_wrk_error(self):
        e = JobNotFound("not found")
        assert isinstance(e, WrkError)

    def test_job_timeout_is_wrk_error(self):
        e = JobTimeout("timeout")
        assert isinstance(e, WrkError)

    def test_worker_shutdown_is_wrk_error(self):
        e = WorkerShutdown("shutdown")
        assert isinstance(e, WrkError)

    def test_dependency_failed_is_wrk_error(self):
        e = DependencyFailed("dep failed")
        assert isinstance(e, WrkError)

    def test_job_error_is_wrk_error(self):
        job = MagicMock()
        job.id = "j1"
        job.status = "failed"
        job.error = "some error"
        e = JobError(job)
        assert isinstance(e, WrkError)


class TestJobError:
    def test_message_contains_job_id(self):
        job = MagicMock()
        job.id = "abc-123"
        job.status = "failed"
        job.error = "something broke"
        exc = JobError(job)
        assert "abc-123" in str(exc)

    def test_message_contains_status(self):
        job = MagicMock()
        job.id = "j1"
        job.status = "aborted"
        job.error = None
        exc = JobError(job)
        assert "aborted" in str(exc)

    def test_job_attribute_stored(self):
        job = MagicMock()
        job.id = "j1"
        job.status = "failed"
        job.error = "err"
        exc = JobError(job)
        assert exc.job is job

    def test_without_attributes_uses_defaults(self):
        exc = JobError(object())
        assert "?" in str(exc)

    def test_can_be_raised(self):
        job = MagicMock()
        job.id = "j1"
        job.status = "failed"
        job.error = "crash"
        with pytest.raises(JobError):
            raise JobError(job)
