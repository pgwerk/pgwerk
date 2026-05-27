from __future__ import annotations

from datetime import datetime
from datetime import timezone
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from pgwerk.app import Werk
from pgwerk.utils import fn_path
from pgwerk.utils import import_fn
from pgwerk.utils import normalize_retry
from pgwerk.utils import normalize_callback
from pgwerk.utils import normalize_depends_on
from pgwerk.commons import JobStatus
from pgwerk.schemas import Job
from pgwerk.schemas import Retry
from pgwerk.schemas import Callback
from pgwerk.schemas import Dependency
from pgwerk.schemas import JobExecution
from pgwerk.serializers import JSONSerializer


# ---------------------------------------------------------------------------
# _normalize_retry
# ---------------------------------------------------------------------------


class TestNormalizeRetry:
    def test_none_default_backoff(self):
        assert normalize_retry(None) == (3, [2, 4])

    def test_none_no_backoff(self):
        assert normalize_retry(None, default_backoff=False) == (3, None)

    def test_zero(self):
        assert normalize_retry(0) == (1, None)

    def test_int_default_backoff(self):
        assert normalize_retry(5) == (5, [2, 4, 8, 16])

    def test_int_no_backoff(self):
        assert normalize_retry(5, default_backoff=False) == (5, None)

    def test_int_one_no_intervals(self):
        assert normalize_retry(1) == (1, None)

    def test_retry_object_uniform(self):
        assert normalize_retry(Retry(max=3, intervals=10)) == (3, [10])

    def test_retry_object_list(self):
        assert normalize_retry(Retry(max=3, intervals=[5, 10])) == (3, [5, 10])


# ---------------------------------------------------------------------------
# _normalize_callback
# ---------------------------------------------------------------------------


def _module_level_cb():
    pass


class TestNormalizeCallback:
    def test_none(self):
        assert normalize_callback(None) == (None, None)

    def test_callback_object(self):
        cb = Callback(func=_module_level_cb, timeout=30)
        path, timeout = normalize_callback(cb)
        assert "_module_level_cb" in path
        assert timeout == 30

    def test_callable(self):
        path, timeout = normalize_callback(_module_level_cb)
        assert "_module_level_cb" in path
        assert timeout is None

    def test_string(self):
        path, timeout = normalize_callback("myapp.tasks.on_done")
        assert path == "myapp.tasks.on_done"
        assert timeout is None


# ---------------------------------------------------------------------------
# _normalize_depends_on
# ---------------------------------------------------------------------------


class TestNormalizeDependsOn:
    def test_none(self):
        assert normalize_depends_on(None) == []

    def test_single_string(self):
        assert normalize_depends_on("abc") == [("abc", False)]

    def test_single_dependency(self):
        d = Dependency(job="xyz", allow_failure=True)
        assert normalize_depends_on(d) == [("xyz", True)]

    def test_job_object(self, _job):
        result = normalize_depends_on(_job)
        assert result == [(_job.id, False)]

    def test_list_mixed(self, _job):
        d = Dependency(job="dep-1", allow_failure=True)
        result = normalize_depends_on([d, "dep-2", _job])
        assert result == [("dep-1", True), ("dep-2", False), (_job.id, False)]


# ---------------------------------------------------------------------------
# Job.from_row
# ---------------------------------------------------------------------------


class TestJobFromRow:
    def test_basic(self, _job_row):
        job = Job.from_row(_job_row, JSONSerializer())
        assert job.id == _job_row["id"]
        assert job.function == _job_row["function"]
        assert isinstance(job.status, JobStatus)

    def test_payload_passed_through(self, _job_row):
        # psycopg3 auto-decodes JSONB; Job.from_row receives an already-parsed dict
        _job_row["payload"] = {"args": [1], "kwargs": {}}
        job = Job.from_row(_job_row, JSONSerializer())
        assert job.payload == {"args": [1], "kwargs": {}}

    def test_none_payload(self, _job_row):
        _job_row["payload"] = None
        job = Job.from_row(_job_row, JSONSerializer())
        assert job.payload is None

    def test_worker_id_stringified(self, _job_row):
        import uuid

        uid = uuid.uuid4()
        _job_row["worker_id"] = uid
        job = Job.from_row(_job_row, JSONSerializer())
        assert job.worker_id == str(uid)

    def test_worker_id_none(self, _job_row):
        _job_row["worker_id"] = None
        job = Job.from_row(_job_row, JSONSerializer())
        assert job.worker_id is None


# ---------------------------------------------------------------------------
# JobExecution.from_row
# ---------------------------------------------------------------------------


class TestRowToExecution:
    def test_basic(self, _exec_row):
        ex = JobExecution.from_row(_exec_row, JSONSerializer())
        assert ex.id == str(_exec_row["id"])
        assert ex.job_id == str(_exec_row["job_id"])

    def test_result_decoded(self, _exec_row):
        import json

        _exec_row["result"] = json.dumps(json.dumps(42))
        ex = JobExecution.from_row(_exec_row, JSONSerializer())
        assert ex.result == 42

    def test_worker_id_none(self, _exec_row):
        _exec_row["worker_id"] = None
        ex = JobExecution.from_row(_exec_row, JSONSerializer())
        assert ex.worker_id is None


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def _job():
    now = datetime.now(timezone.utc)
    return Job(
        id="job-abc",
        function="m.f",
        queue="default",
        status="queued",
        priority=0,
        attempts=0,
        max_attempts=1,
        scheduled_at=now,
        enqueued_at=now,
    )


@pytest.fixture
def _job_row():
    now = datetime.now(timezone.utc)
    return {
        "id": "row-job-1",
        "key": None,
        "function": "myapp.tasks.fn",
        "queue": "default",
        "status": "queued",
        "priority": 0,
        "group_key": None,
        "payload": None,
        "result": None,
        "error": None,
        "attempts": 0,
        "max_attempts": 1,
        "timeout_secs": None,
        "heartbeat_secs": None,
        "scheduled_at": now,
        "enqueued_at": now,
        "started_at": None,
        "completed_at": None,
        "touched_at": None,
        "expires_at": None,
        "worker_id": None,
        "meta": None,
        "result_ttl": None,
        "failure_ttl": None,
        "ttl": None,
        "on_success": None,
        "on_failure": None,
        "on_stopped": None,
        "on_success_timeout": None,
        "on_failure_timeout": None,
        "on_stopped_timeout": None,
        "retry_intervals": None,
        "repeat_remaining": None,
        "repeat_interval_secs": None,
        "repeat_intervals": None,
    }


@pytest.fixture
def _exec_row():
    now = datetime.now(timezone.utc)
    return {
        "id": "exec-1",
        "job_id": "job-1",
        "attempt": 1,
        "status": "running",
        "worker_id": None,
        "error": None,
        "result": None,
        "started_at": now,
        "completed_at": None,
    }


# ---------------------------------------------------------------------------
# _fn_path
# ---------------------------------------------------------------------------


def _module_fn_for_test():
    pass


class TestFnPath:
    def test_module_level_function(self):
        path = fn_path(_module_fn_for_test)
        assert "test_app_helpers" in path
        assert "_module_fn_for_test" in path

    def test_lambda_raises(self):
        with pytest.raises(ValueError, match="lambda"):
            fn_path(lambda: None)

    def test_local_function_raises(self):
        def local_fn():
            pass

        with pytest.raises(ValueError, match="locals"):
            fn_path(local_fn)

    def test_module_level_callback(self):
        path = fn_path(_module_level_cb)
        assert "_module_level_cb" in path


# ---------------------------------------------------------------------------
# import_fn
# ---------------------------------------------------------------------------


class TestImportFn:
    # -- module-level function ------------------------------------------------

    def test_module_level_function(self):
        import os

        assert import_fn("os.getcwd") is os.getcwd

    def test_module_level_function_deep_package(self):
        from pgwerk.utils import advisory_key

        assert import_fn("pgwerk.utils.advisory_key") is advisory_key

    # -- function re-exported through __init__ --------------------------------

    def test_function_via_package_init(self):
        # configure_logging lives in pgwerk.logging but is re-exported at pgwerk
        from pgwerk import configure_logging

        assert import_fn("pgwerk.configure_logging") is configure_logging

    def test_class_via_package_init(self):
        from pgwerk import Job

        assert import_fn("pgwerk.Job") is Job

    # -- classmethod ----------------------------------------------------------

    def test_classmethod(self):
        # fn_path(Job.from_row) → "pgwerk.schemas.Job.from_row"
        from pgwerk.schemas import Job

        fn = import_fn("pgwerk.schemas.Job.from_row")
        assert fn.__func__ is Job.from_row.__func__

    def test_classmethod_via_package_init(self):
        # same class, accessed through the re-exported __init__ path
        from pgwerk import Job

        fn = import_fn("pgwerk.Job.from_row")
        assert fn.__func__ is Job.from_row.__func__

    # -- staticmethod ---------------------------------------------------------

    def test_staticmethod(self):
        from pgwerk.worker.base import BaseWorker

        fn = import_fn("pgwerk.worker.base.BaseWorker._with_timeout")
        assert fn is BaseWorker._with_timeout

    # -- regular instance method ----------------------------------------------

    def test_instance_method(self):
        # import_fn should resolve instance methods too (even if rarely enqueued)
        from pgwerk.schemas import Job

        fn = import_fn("pgwerk.schemas.Job.__post_init__")
        assert fn is Job.__post_init__

    # -- class (not a callable attribute) -------------------------------------

    def test_imports_class(self):
        from pgwerk.schemas import Job

        assert import_fn("pgwerk.schemas.Job") is Job

    # -- error cases ----------------------------------------------------------

    def test_entirely_bogus_path(self):
        with pytest.raises(ImportError, match="Couldn't import"):
            import_fn("totally.bogus.path.that.does.not.exist")

    def test_valid_module_missing_attr(self):
        with pytest.raises(ImportError, match="Couldn't import"):
            import_fn("os.no_such_attr_xyz")

    def test_single_segment_raises(self):
        with pytest.raises(ImportError, match="Couldn't import"):
            import_fn("nonexistentmodule")

    def test_broken_transitive_import_surfaces_real_error(self):
        def fake_import(name):
            if name == "fake_pkg.dispatcher":
                raise ModuleNotFoundError("No module named 'missing_dep'", name="missing_dep")
            raise ModuleNotFoundError(f"No module named {name!r}", name=name)

        with patch("pgwerk.utils.importlib.import_module", side_effect=fake_import):
            with pytest.raises(ModuleNotFoundError, match="missing_dep"):
                import_fn("fake_pkg.dispatcher.run_workload")


# ---------------------------------------------------------------------------
# Werk init and pool
# ---------------------------------------------------------------------------


class TestWrkInit:
    def test_basic_init(self):
        wrk = Werk("postgresql://localhost/test")
        assert wrk.dsn == "postgresql://localhost/test"
        assert wrk.prefix == "_pgwerk"
        assert wrk.schema == "pgwerk"
        assert not wrk._connected

    def test_custom_prefix(self):
        wrk = Werk("postgresql://localhost/test", prefix="myapp")
        assert wrk.prefix == "myapp"

    def test_custom_schema(self):
        wrk = Werk("postgresql://localhost/test", schema="myschema")
        assert wrk.schema == "myschema"

    def test_job_repo_available_without_connect(self):
        wrk = Werk("postgresql://localhost/test")
        assert wrk._job_repo is not None
        assert wrk._job_repo is wrk._job_repo

    def test_register_before_enqueue(self):
        wrk = Werk("postgresql://localhost/test")

        def cb(job):
            pass

        wrk.register_before_enqueue(cb)
        assert id(cb) in wrk._before_enqueues

    def test_unregister_before_enqueue(self):
        wrk = Werk("postgresql://localhost/test")

        def cb(job):
            pass

        wrk.register_before_enqueue(cb)
        wrk.unregister_before_enqueue(cb)
        assert id(cb) not in wrk._before_enqueues

    def test_unregister_nonexistent_is_noop(self):
        wrk = Werk("postgresql://localhost/test")

        def cb(job):
            pass

        wrk.unregister_before_enqueue(cb)

    async def test_run_before_enqueue_sync(self):
        wrk = Werk("postgresql://localhost/test")
        called = []

        def cb(job):
            called.append(job)

        wrk.register_before_enqueue(cb)
        job = MagicMock()
        await wrk._run_before_enqueue(job)
        assert called == [job]

    async def test_run_before_enqueue_async(self):
        wrk = Werk("postgresql://localhost/test")
        called = []

        async def cb(job):
            called.append(job)

        wrk.register_before_enqueue(cb)
        job = MagicMock()
        await wrk._run_before_enqueue(job)
        assert called == [job]

    async def test_run_before_enqueue_no_hooks(self):
        wrk = Werk("postgresql://localhost/test")
        job = MagicMock()
        await wrk._run_before_enqueue(job)


# ---------------------------------------------------------------------------
# _normalize_retry extras
# ---------------------------------------------------------------------------


class TestNormalizeRetryExtra:
    def test_retry_no_intervals(self):
        assert normalize_retry(Retry(max=3, intervals=0)) == (3, None)

    def test_retry_empty_list(self):
        assert normalize_retry(Retry(max=3, intervals=[])) == (3, None)


# ---------------------------------------------------------------------------
# Werk.enqueue_at / enqueue_in wrappers
# ---------------------------------------------------------------------------


class TestEnqueueAtAndIn:
    async def test_enqueue_at_forwards_at_kwarg(self):
        from unittest.mock import AsyncMock

        wrk = Werk("postgresql://localhost/test")
        when = datetime(2030, 1, 1, tzinfo=timezone.utc)

        with patch.object(wrk, "enqueue", new=AsyncMock(return_value=None)) as m:
            await wrk.enqueue_at(when, "m.f", 1, x=2, _queue="q")
            m.assert_awaited_once_with("m.f", 1, _at=when, x=2, _queue="q")

    async def test_enqueue_in_forwards_delay_kwarg(self):
        from unittest.mock import AsyncMock

        wrk = Werk("postgresql://localhost/test")

        with patch.object(wrk, "enqueue", new=AsyncMock(return_value=None)) as m:
            await wrk.enqueue_in(300, "m.f", _queue="q", _retry=2)
            m.assert_awaited_once_with("m.f", _delay=300, _queue="q", _retry=2)

    async def test_enqueue_at_returns_job_from_enqueue(self):
        from unittest.mock import AsyncMock

        wrk = Werk("postgresql://localhost/test")
        sentinel = MagicMock(name="job")
        with patch.object(wrk, "enqueue", new=AsyncMock(return_value=sentinel)):
            result = await wrk.enqueue_at(datetime.now(timezone.utc), "m.f")
            assert result is sentinel
