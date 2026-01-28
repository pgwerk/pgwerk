from __future__ import annotations

import asyncio

from unittest.mock import MagicMock
from unittest.mock import patch

from wrk.utils import call_hook
from wrk.utils import wants_context
from wrk.utils import invoke_callback
from wrk.commons import JobStatus
from wrk.commons import DequeueStrategy
from wrk.commons import ExecutionStatus
from wrk.schemas import Context


class TestJobStatusEnum:
    def test_values(self):
        assert JobStatus.Queued == "queued"
        assert JobStatus.Waiting == "waiting"
        assert JobStatus.Active == "active"
        assert JobStatus.Aborting == "aborting"
        assert JobStatus.Complete == "complete"
        assert JobStatus.Failed == "failed"
        assert JobStatus.Aborted == "aborted"

    def test_is_str(self):
        assert isinstance(JobStatus.Queued, str)

    def test_membership(self):
        assert "queued" in [s.value for s in JobStatus]


class TestExecutionStatusEnum:
    def test_values(self):
        assert ExecutionStatus.Running == "running"
        assert ExecutionStatus.Complete == "complete"
        assert ExecutionStatus.Failed == "failed"
        assert ExecutionStatus.Aborted == "aborted"


class TestDequeueStrategyEnum:
    def test_values(self):
        assert DequeueStrategy.Priority == "priority"
        assert DequeueStrategy.RoundRobin == "round_robin"
        assert DequeueStrategy.Random == "random"


class TestContext:
    def test_creation(self):
        app = MagicMock()
        worker = MagicMock()
        job = MagicMock()
        ctx = Context(app=app, worker=worker, job=job)
        assert ctx.app is app
        assert ctx.worker is worker
        assert ctx.job is job
        assert ctx.exception is None

    def test_exception_field(self):
        exc = RuntimeError("boom")
        ctx = Context(app=MagicMock(), worker=MagicMock(), job=MagicMock(), exception=exc)
        assert ctx.exception is exc


class TestWantsContext:
    def test_no_params_false(self):
        def fn():
            pass

        assert wants_context(fn) is False

    def test_ctx_named_first_param(self):
        def fn(ctx):
            pass

        assert wants_context(fn) is True

    def test_ctx_named_among_many(self):
        def fn(ctx, a, b):
            pass

        assert wants_context(fn) is True

    def test_context_annotated(self):
        def fn(c: Context):
            pass

        assert wants_context(fn) is True

    def test_other_name_no_annotation(self):
        def fn(x, y):
            pass

        assert wants_context(fn) is False

    def test_other_annotation(self):
        def fn(x: int):
            pass

        assert wants_context(fn) is False

    def test_async_fn_ctx_named(self):
        async def fn(ctx):
            pass

        assert wants_context(fn) is True

    def test_builtin_returns_false(self):
        assert wants_context(len) is False

    def test_non_ctx_annotation_empty(self):
        def fn(x):
            pass

        assert wants_context(fn) is False


class TestCallHook:
    async def test_sync_hook(self):
        called = []

        def hook(ctx):
            called.append(ctx)

        ctx = MagicMock()
        await call_hook(hook, ctx)
        assert called == [ctx]

    async def test_async_hook(self):
        called = []

        async def hook(ctx):
            called.append(ctx)

        ctx = MagicMock()
        await call_hook(hook, ctx)
        assert called == [ctx]


class TestInvokeCallback:
    async def test_sync_callback(self):
        called = []

        def cb(job):
            called.append(job)

        job = MagicMock()
        with patch("wrk.utils.importlib.import_module") as m:
            m.return_value.cb = cb
            await invoke_callback("myapp.cb", job)
        assert job in called

    async def test_async_callback(self):
        called = []

        async def cb(job):
            called.append(job)

        job = MagicMock()
        with patch("wrk.utils.importlib.import_module") as m:
            m.return_value.cb = cb
            await invoke_callback("myapp.cb", job)
        assert job in called

    async def test_async_callback_with_timeout(self):
        called = []

        async def cb(job):
            called.append(job)

        job = MagicMock()
        with patch("wrk.utils.importlib.import_module") as m:
            m.return_value.cb = cb
            await invoke_callback("myapp.cb", job, timeout=5)
        assert job in called

    async def test_timeout_does_not_raise(self):
        async def slow_cb(job):
            await asyncio.sleep(100)

        job = MagicMock()
        job.id = "j1"
        with patch("wrk.utils.importlib.import_module") as m:
            m.return_value.slow_cb = slow_cb
            await invoke_callback("myapp.slow_cb", job, timeout=0.01)

    async def test_import_error_does_not_raise(self):
        job = MagicMock()
        job.id = "j1"
        await invoke_callback("no_such_module_xyz.fn", job)

    async def test_callback_exception_does_not_raise(self):
        def bad_cb(job):
            raise ValueError("oops")

        job = MagicMock()
        job.id = "j1"
        with patch("wrk.utils.importlib.import_module") as m:
            m.return_value.bad_cb = bad_cb
            await invoke_callback("myapp.bad_cb", job)
