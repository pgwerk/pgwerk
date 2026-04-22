from __future__ import annotations

import asyncio

from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from tests.worker.fork import ForkWorker
from tests.worker.fork import _fork_target
from tests.worker.thread import ThreadWorker
from tests.worker.process import ProcessWorker
from tests.worker.process import _run_in_subprocess


def make_app():
    app = MagicMock()
    app.prefix = "_pgwerk"
    app.serializer = MagicMock()
    return app


def make_job(function, payload=None, timeout_secs=None):
    job = MagicMock()
    job.function = function
    job.payload = payload
    job.timeout_secs = timeout_secs
    return job


# ---------------------------------------------------------------------------
# _run_in_subprocess
# ---------------------------------------------------------------------------


class TestRunInSubprocess:
    def test_sync_function(self):
        import os

        result = _run_in_subprocess("os.getcwd", [], {})
        assert result == os.getcwd()

    def test_async_function(self):
        result = _run_in_subprocess("asyncio.sleep", [0], {})
        assert result is None

    def test_with_kwargs(self):
        result = _run_in_subprocess("os.path.join", ["a", "b"], {})
        import os

        assert result == os.path.join("a", "b")


# ---------------------------------------------------------------------------
# _fork_target
# ---------------------------------------------------------------------------


class TestForkTarget:
    def test_success(self):
        import queue

        q = queue.Queue()
        _fork_target("os.getcwd", [], {}, q)
        kind, _value = q.get_nowait()
        assert kind == "ok"

    def test_error(self):
        import queue

        q = queue.Queue()
        _fork_target("os.path.join", [], {}, q)
        kind, _value = q.get_nowait()
        assert kind == "err"

    def test_async_success(self):
        import queue

        q = queue.Queue()
        _fork_target("asyncio.sleep", [0], {}, q)
        kind, _value = q.get_nowait()
        assert kind == "ok"


# ---------------------------------------------------------------------------
# ProcessWorker context rejection
# ---------------------------------------------------------------------------


class TestProcessWorkerContextRejection:
    async def test_context_wanting_fn_raises(self):
        w = ProcessWorker(app=make_app())
        job = make_job("os.getcwd")
        ctx = MagicMock()

        with patch("wrk.worker.process.wants_context", return_value=True):
            with pytest.raises(RuntimeError, match="Context"):
                await w._execute(job, ctx)

    def test_init_has_process_pool(self):
        w = ProcessWorker(app=make_app())
        assert w._process_pool is None

    async def test_setup_creates_pool(self):
        w = ProcessWorker(app=make_app())
        await w._setup_executor()
        assert w._process_pool is not None
        w._process_pool.shutdown(wait=False)

    async def test_teardown_shuts_down_pool(self):
        w = ProcessWorker(app=make_app())
        await w._setup_executor()
        await w._teardown_executor()


# ---------------------------------------------------------------------------
# ForkWorker context rejection
# ---------------------------------------------------------------------------


class TestForkWorkerContextRejection:
    async def test_context_wanting_fn_raises(self):
        w = ForkWorker(app=make_app())
        job = make_job("os.getcwd")
        ctx = MagicMock()

        with patch("wrk.worker.fork.wants_context", return_value=True):
            with pytest.raises(RuntimeError, match="Context"):
                await w._execute(job, ctx)

    async def test_terminate_dead_process(self):
        w = ForkWorker(app=make_app())
        proc = MagicMock()
        proc.is_alive.return_value = False
        loop = asyncio.get_event_loop()
        await w._terminate(proc, loop)
        proc.terminate.assert_not_called()


# ---------------------------------------------------------------------------
# ThreadWorker
# ---------------------------------------------------------------------------


class TestThreadWorkerInit:
    def test_thread_pool_initially_none(self):
        w = ThreadWorker(app=make_app())
        assert w._thread_pool is None

    async def test_setup_creates_pool(self):
        w = ThreadWorker(app=make_app())
        await w._setup_executor()
        assert w._thread_pool is not None
        w._thread_pool.shutdown(wait=False)

    async def test_teardown_shuts_down_pool(self):
        w = ThreadWorker(app=make_app())
        await w._setup_executor()
        await w._teardown_executor()

    async def test_execute_sync_function(self):
        w = ThreadWorker(app=make_app())
        await w._setup_executor()
        try:

            def sync_fn():
                return 77

            job = make_job("mymodule.fn")
            ctx = MagicMock()

            with patch("importlib.import_module") as m:
                m.return_value.fn = sync_fn
                result = await w._execute(job, ctx)
            assert result == 77
        finally:
            w._thread_pool.shutdown(wait=False)

    async def test_execute_async_function(self):
        w = ThreadWorker(app=make_app())
        await w._setup_executor()
        try:

            async def async_fn():
                return 88

            job = make_job("mymodule.fn")
            ctx = MagicMock()

            with patch("importlib.import_module") as m:
                m.return_value.fn = async_fn
                result = await w._execute(job, ctx)
            assert result == 88
        finally:
            w._thread_pool.shutdown(wait=False)
