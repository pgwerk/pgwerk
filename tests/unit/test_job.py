from __future__ import annotations

from datetime import datetime
from datetime import timezone

import pytest

from tests.commons import JobStatus
from tests.commons import ExecutionStatus
from tests.schemas import Job
from tests.schemas import Retry
from tests.schemas import Repeat
from tests.schemas import Callback
from tests.schemas import Dependency
from tests.schemas import JobExecution


# ---------------------------------------------------------------------------
# Retry
# ---------------------------------------------------------------------------


class TestRetry:
    def test_max_below_one_raises(self):
        with pytest.raises(ValueError, match="max must be >= 1"):
            Retry(max=0)

    def test_negative_interval_raises(self):
        with pytest.raises(ValueError, match="intervals must be >= 0"):
            Retry(max=3, intervals=-1)

    def test_get_interval_uniform(self):
        r = Retry(max=5, intervals=10)
        assert r.get_interval(1) == 10
        assert r.get_interval(3) == 10

    def test_get_interval_list_within_bounds(self):
        r = Retry(max=5, intervals=[5, 10, 30])
        assert r.get_interval(1) == 5
        assert r.get_interval(2) == 10
        assert r.get_interval(3) == 30

    def test_get_interval_list_clamps_at_last(self):
        r = Retry(max=5, intervals=[5, 10])
        assert r.get_interval(4) == 10
        assert r.get_interval(5) == 10

    def test_get_interval_empty_list(self):
        r = Retry(max=3, intervals=[])
        assert r.get_interval(1) == 0

    def test_to_intervals_list_int_returns_none(self):
        r = Retry(max=3, intervals=5)
        assert r.to_intervals_list() is None

    def test_to_intervals_list_returns_list(self):
        r = Retry(max=3, intervals=[1, 2, 3])
        assert r.to_intervals_list() == [1, 2, 3]


# ---------------------------------------------------------------------------
# Repeat
# ---------------------------------------------------------------------------


class TestRepeat:
    def test_times_below_one_raises(self):
        with pytest.raises(ValueError, match="times must be >= 1"):
            Repeat(times=0)

    def test_negative_interval_raises(self):
        with pytest.raises(ValueError, match="interval must be >= 0"):
            Repeat(times=3, interval=-1)

    def test_get_interval_uniform(self):
        r = Repeat(times=3, interval=60)
        assert r.get_interval(0) == 60
        assert r.get_interval(2) == 60

    def test_get_interval_list_clamps_at_last(self):
        r = Repeat(times=5, intervals=[10, 20])
        assert r.get_interval(0) == 10
        assert r.get_interval(1) == 20
        assert r.get_interval(4) == 20

    def test_get_interval_no_intervals_falls_back_to_interval(self):
        r = Repeat(times=3, interval=30)
        assert r.get_interval(0) == 30


# ---------------------------------------------------------------------------
# Dependency
# ---------------------------------------------------------------------------


class TestDependency:
    def test_job_id_from_string(self):
        d = Dependency(job="abc-123")
        assert d.job_id == "abc-123"

    def test_job_id_from_job_object(self, _minimal_job):
        d = Dependency(job=_minimal_job)
        assert d.job_id == _minimal_job.id


# ---------------------------------------------------------------------------
# Callback
# ---------------------------------------------------------------------------


class TestCallback:
    def test_path_from_callable(self):
        def my_func():
            pass

        cb = Callback(func=my_func)
        assert "my_func" in cb.path()

    def test_path_from_string(self):
        cb = Callback(func="myapp.tasks.send_email")
        assert cb.path() == "myapp.tasks.send_email"


# ---------------------------------------------------------------------------
# Job
# ---------------------------------------------------------------------------


class TestJob:
    def test_status_coerced_from_string(self, _minimal_job):
        assert isinstance(_minimal_job.status, JobStatus)

    def test_status_already_enum(self):
        now = datetime.now(timezone.utc)
        job = Job(
            id="1",
            function="a.b",
            queue="default",
            status=JobStatus.Active,
            priority=0,
            attempts=0,
            max_attempts=1,
            scheduled_at=now,
            enqueued_at=now,
        )
        assert job.status is JobStatus.Active


# ---------------------------------------------------------------------------
# JobExecution
# ---------------------------------------------------------------------------


class TestJobExecution:
    def test_status_coerced_from_string(self):
        ex = JobExecution(
            id="e1",
            job_id="j1",
            attempt=1,
            status="running",
        )
        assert ex.status is ExecutionStatus.Running


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def _minimal_job():
    now = datetime.now(timezone.utc)
    return Job(
        id="job-123",
        function="myapp.tasks.fn",
        queue="default",
        status="queued",
        priority=0,
        attempts=0,
        max_attempts=1,
        scheduled_at=now,
        enqueued_at=now,
    )
