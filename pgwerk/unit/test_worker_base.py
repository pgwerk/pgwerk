from __future__ import annotations

import asyncio

from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from pgwerk.commons import DequeueStrategy
from pgwerk.worker.base import BaseWorker


class ConcreteWorker(BaseWorker):
    async def _execute(self, job, ctx):
        return "result"


def make_worker(**kwargs):
    app = MagicMock()
    app.prefix = "_wrk"
    return ConcreteWorker(app=app, **kwargs)


class TestBaseWorkerInit:
    def test_default_queues(self):
        w = make_worker()
        assert w.queues == ["default"]

    def test_custom_queues(self):
        w = make_worker(queues=["a", "b"])
        assert w.queues == ["a", "b"]

    def test_concurrency(self):
        w = make_worker(concurrency=5)
        assert w.concurrency == 5

    def test_default_strategy_is_priority(self):
        w = make_worker()
        assert w.dequeue_strategy == DequeueStrategy.Priority

    def test_custom_strategy(self):
        w = make_worker(dequeue_strategy=DequeueStrategy.RoundRobin)
        assert w.dequeue_strategy == DequeueStrategy.RoundRobin

    def test_burst_false_by_default(self):
        w = make_worker()
        assert w.burst is False

    def test_burst_true(self):
        w = make_worker(burst=True)
        assert w.burst is True

    def test_id_is_uuid_string(self):
        import uuid

        w = make_worker()
        uuid.UUID(w.id)

    def test_name_contains_hostname(self):
        import socket

        w = make_worker()
        assert socket.gethostname() in w.name

    def test_running_initially_false(self):
        w = make_worker()
        assert w._running is False

    def test_before_after_process_empty(self):
        w = make_worker()
        assert w._before_process == []
        assert w._after_process == []

    def test_custom_before_after_process(self):
        def hook(ctx):
            pass

        w = make_worker(before_process=[hook], after_process=[hook])
        assert hook in w._before_process
        assert hook in w._after_process

    def test_active_sets_empty(self):
        w = make_worker()
        assert w._active == set()
        assert w._active_jobs == {}
        assert w._abort_requested == set()


class TestOrderedQueues:
    def test_priority_returns_queues_as_is(self):
        w = make_worker(queues=["a", "b", "c"], dequeue_strategy=DequeueStrategy.Priority)
        assert w._ordered_queues() == ["a", "b", "c"]

    def test_priority_multiple_calls_same_result(self):
        w = make_worker(queues=["a", "b"], dequeue_strategy=DequeueStrategy.Priority)
        assert w._ordered_queues() == w._ordered_queues()

    def test_round_robin_advances_offset(self):
        w = make_worker(queues=["a", "b", "c"], dequeue_strategy=DequeueStrategy.RoundRobin)
        first = w._ordered_queues()
        second = w._ordered_queues()
        assert first[0] == "a"
        assert second[0] == "b"

    def test_round_robin_wraps_around(self):
        w = make_worker(queues=["a", "b"], dequeue_strategy=DequeueStrategy.RoundRobin)
        results = [w._ordered_queues()[0] for _ in range(4)]
        assert results == ["a", "b", "a", "b"]

    def test_random_returns_all_queues(self):
        w = make_worker(queues=["a", "b", "c"], dequeue_strategy=DequeueStrategy.Random)
        result = w._ordered_queues()
        assert sorted(result) == ["a", "b", "c"]


class TestExceptionHandlers:
    def test_push_and_pop(self):
        w = make_worker()

        def handler(job, exc):
            pass

        w.push_exception_handler(handler)
        assert handler in w._exception_handlers
        popped = w.pop_exception_handler()
        assert popped is handler
        assert w._exception_handlers == []

    def test_pop_empty_raises(self):
        w = make_worker()
        with pytest.raises(IndexError):
            w.pop_exception_handler()

    async def test_invoke_sync_handler(self):
        w = make_worker()
        called = []

        def handler(job, exc):
            called.append((job, exc))

        w.push_exception_handler(handler)
        job = MagicMock()
        exc = ValueError("test")
        await w._invoke_exception_handlers(job, exc)
        assert called == [(job, exc)]

    async def test_invoke_async_handler(self):
        w = make_worker()
        called = []

        async def handler(job, exc):
            called.append((job, exc))

        w.push_exception_handler(handler)
        job = MagicMock()
        exc = ValueError("test")
        await w._invoke_exception_handlers(job, exc)
        assert called == [(job, exc)]

    async def test_handler_exception_does_not_propagate(self):
        w = make_worker()

        def bad_handler(job, exc):
            raise RuntimeError("handler failed")

        w.push_exception_handler(bad_handler)
        await w._invoke_exception_handlers(MagicMock(), ValueError())

    async def test_handlers_invoked_in_reverse_order(self):
        w = make_worker()
        order = []

        def h1(job, exc):
            order.append(1)

        def h2(job, exc):
            order.append(2)

        w.push_exception_handler(h1)
        w.push_exception_handler(h2)
        await w._invoke_exception_handlers(MagicMock(), ValueError())
        assert order == [2, 1]


class TestRequestShutdown:
    def test_sets_running_false(self):
        w = make_worker()
        w._running = True
        w._request_shutdown()
        assert w._running is False

    def test_sets_wakeup_event(self):
        w = make_worker()
        w._request_shutdown()
        assert w._wakeup.is_set()


class TestHookRegistration:
    def test_add_before_process(self):
        w = make_worker()

        def hook(ctx):
            pass

        w.add_before_process(hook)
        assert hook in w._before_process

    def test_add_after_process(self):
        w = make_worker()

        def hook(ctx):
            pass

        w.add_after_process(hook)
        assert hook in w._after_process

    def test_multiple_hooks(self):
        w = make_worker()

        def h1(ctx):
            pass

        def h2(ctx):
            pass

        w.add_before_process(h1)
        w.add_before_process(h2)
        assert w._before_process == [h1, h2]


class TestWithTimeout:
    async def test_no_timeout(self):
        async def coro():
            return 42

        result = await BaseWorker._with_timeout(coro(), None)
        assert result == 42

    async def test_with_timeout_completes(self):
        async def coro():
            return 99

        result = await BaseWorker._with_timeout(coro(), 5)
        assert result == 99

    async def test_with_timeout_raises(self):
        async def slow_coro():
            await asyncio.sleep(100)

        with pytest.raises(asyncio.TimeoutError):
            await BaseWorker._with_timeout(slow_coro(), 0.01)


class TestSetupTeardown:
    async def test_setup_executor_noop(self):
        w = make_worker()
        await w._setup_executor()

    async def test_teardown_executor_noop(self):
        w = make_worker()
        await w._teardown_executor()


class TestSweepLoop:
    async def test_sweep_loop_calls_sweep(self):
        w = make_worker()
        w._running = True
        w.app.sweep = AsyncMock(return_value=[])

        call_count = 0

        async def fast_sleep(delay):
            nonlocal call_count
            call_count += 1
            w._running = False

        with patch("wrk.worker.base.asyncio.sleep", fast_sleep):
            await w._sweep_loop()

        w.app.sweep.assert_called_once()

    async def test_sweep_loop_handles_error(self):
        w = make_worker()
        w._running = True
        w.app.sweep = AsyncMock(side_effect=RuntimeError("db error"))

        call_count = 0

        async def fast_sleep(delay):
            nonlocal call_count
            call_count += 1
            w._running = False

        with patch("wrk.worker.base.asyncio.sleep", fast_sleep):
            await w._sweep_loop()
