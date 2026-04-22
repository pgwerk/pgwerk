from __future__ import annotations

import os
import logging

import pytest
import psycopg
import pytest_asyncio

from tests.app import Wrk
from tests.worker import AsyncWorker


_TEST_PREFIX = "_pgwerk"
_TEST_DSN = os.environ.get("PGWERK_TEST_DSN", "postgresql://werk:wrk@localhost/wrk_test")

_TABLES = ["job_deps", "worker_jobs", "jobs_executions", "jobs", "worker"]
_DROP_TABLES = [*_TABLES, "versions"]


@pytest.fixture(scope="session", autouse=True)
def _reset_test_schema():
    """Drop all test tables at session start so auto-migrate rebuilds them clean."""
    with psycopg.connect(_TEST_DSN) as conn:
        for tbl in _DROP_TABLES:
            conn.execute(f'DROP TABLE IF EXISTS "{_TEST_PREFIX}_{tbl}" CASCADE')
        conn.commit()


@pytest.fixture(autouse=True)
def _reset_pgwerk_logger_propagation():
    """Restore wrk logger propagation after each test.

    configure_logging() sets propagate=False on the wrk logger, which prevents
    pytest's caplog from capturing records in subsequent tests.
    """
    wrk_logger = logging.getLogger("wrk")
    original = wrk_logger.propagate
    yield
    wrk_logger.propagate = original


@pytest_asyncio.fixture
async def app():
    a = Wrk(_TEST_DSN, prefix=_TEST_PREFIX, min_pool_size=1, max_pool_size=5)
    await a.connect()

    pool = a._pool_or_raise()
    async with pool.connection() as conn:
        for tbl in _TABLES:
            await conn.execute(f'TRUNCATE "{_TEST_PREFIX}_{tbl}" CASCADE')

    yield a
    await a.disconnect()


def make_worker(app: Wrk, queues: list[str] | None = None, **kwargs) -> AsyncWorker:
    """Return a burst-mode AsyncWorker with test-friendly defaults."""
    options = {
        "concurrency": 10,
        "burst": True,
        "heartbeat_interval": 1,
        "poll_interval": 0.05,
        "abort_interval": 0.05,
        "sweep_interval": 9999,
    }
    options.update(kwargs)
    return AsyncWorker(
        app=app,
        queues=queues or ["default"],
        **options,
    )
