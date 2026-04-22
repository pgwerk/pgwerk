from __future__ import annotations

import asyncio

from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from tests.utils import import_fn
from tests.worker.aio import AsyncWorker


def make_app():
    app = MagicMock()
    app.prefix = "_pgwerk"
    app.serializer = MagicMock()
    return app


def make_worker():
    return AsyncWorker(app=make_app())


def make_job(function, payload=None, timeout_secs=None, heartbeat_secs=None):
    job = MagicMock()
    job.function = function
    job.payload = payload
    job.timeout_secs = timeout_secs
    job.heartbeat_secs = heartbeat_secs
    return job


class TestAsyncWorkerImport:
    def test_imports_function(self):
        import os

        fn = import_fn("os.getcwd")
        assert fn is os.getcwd

    def test_imports_class(self):
        from tests.schemas import Job

        cls = import_fn("wrk.schemas.Job")
        assert cls is Job

    def test_import_missing_module_raises(self):
        with pytest.raises(Exception):
            import_fn("no_such_module_xyz.fn")

    def test_import_missing_attr_raises(self):
        with pytest.raises(AttributeError):
            import_fn("os.no_such_attr_xyz")


class TestAsyncWorkerExecute:
    async def test_sync_function(self):
        w = make_worker()

        def sync_fn():
            return 42

        job = make_job("mymodule.sync_fn")
        ctx = MagicMock()
        with patch("wrk.worker.aio.import_fn", return_value=sync_fn):
            result = await w._execute(job, ctx)
        assert result == 42

    async def test_async_function(self):
        w = make_worker()

        async def async_fn():
            return 43

        job = make_job("mymodule.async_fn")
        ctx = MagicMock()
        with patch("wrk.worker.aio.import_fn", return_value=async_fn):
            result = await w._execute(job, ctx)
        assert result == 43

    async def test_function_with_args(self):
        w = make_worker()

        def fn_with_args(x, y):
            return x + y

        job = make_job("mymodule.fn", payload={"args": [1, 2], "kwargs": {}})
        ctx = MagicMock()
        with patch("wrk.worker.aio.import_fn", return_value=fn_with_args):
            result = await w._execute(job, ctx)
        assert result == 3

    async def test_function_with_kwargs(self):
        w = make_worker()

        def fn_with_kwargs(x=0, y=0):
            return x * y

        job = make_job("mymodule.fn", payload={"args": [], "kwargs": {"x": 3, "y": 4}})
        ctx = MagicMock()
        with patch("wrk.worker.aio.import_fn", return_value=fn_with_kwargs):
            result = await w._execute(job, ctx)
        assert result == 12

    async def test_ctx_injection(self):
        w = make_worker()

        def fn_with_ctx(ctx):
            return ctx

        job = make_job("mymodule.fn")
        ctx = MagicMock()
        with patch("wrk.worker.aio.import_fn", return_value=fn_with_ctx):
            result = await w._execute(job, ctx)
        assert result is ctx

    async def test_async_with_ctx_injection(self):
        w = make_worker()

        async def async_fn_with_ctx(ctx):
            return ctx

        job = make_job("mymodule.fn")
        ctx = MagicMock()
        with patch("wrk.worker.aio.import_fn", return_value=async_fn_with_ctx):
            result = await w._execute(job, ctx)
        assert result is ctx

    async def test_timeout_enforced(self):
        w = make_worker()

        async def slow_fn():
            await asyncio.sleep(100)

        job = make_job("mymodule.fn", timeout_secs=0.01)
        ctx = MagicMock()
        with patch("wrk.worker.aio.import_fn", return_value=slow_fn):
            with pytest.raises(asyncio.TimeoutError):
                await w._execute(job, ctx)

    async def test_none_payload_handled(self):
        w = make_worker()

        def fn():
            return "ok"

        job = make_job("mymodule.fn", payload=None)
        ctx = MagicMock()
        with patch("wrk.worker.aio.import_fn", return_value=fn):
            result = await w._execute(job, ctx)
        assert result == "ok"
