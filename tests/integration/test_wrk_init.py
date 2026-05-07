"""Integration tests for Werk initialization."""

from __future__ import annotations

import os

import pytest
import pytest_asyncio
import psycopg

from pgwerk.app import Werk

from .tasks import clear_callback_log

_TEST_DSN = os.environ.get("PGWERK_TEST_DSN", "postgresql://pgwerk:pgwerk@localhost/pgwerk_test")


@pytest.fixture(autouse=True)
def _clear_cbs():
    clear_callback_log()
    yield
    clear_callback_log()


class TestWrkInit:
    def test_config_as_dict(self):
        app = Werk("postgresql://x/y", config={"prefix": "_test"})
        assert app.prefix == "_test"

    def test_log_level_configures_logging(self):
        Werk("postgresql://x/y", log_level="WARNING")

    def test_job_repo_raises_when_not_connected(self):
        app = Werk("postgresql://x/y")
        with pytest.raises(RuntimeError, match="Not connected"):
            _ = app._job_repo


class TestSchemaIsolation:
    @pytest_asyncio.fixture
    async def connected_app(self):
        a = Werk(_TEST_DSN)
        await a.connect()
        yield a
        await a.disconnect()

    @pytest.mark.asyncio
    async def test_tables_created_in_pgwerk_schema(self, connected_app):
        async with await psycopg.AsyncConnection.connect(_TEST_DSN) as conn, conn.cursor() as cur:
            await cur.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = %s ORDER BY table_name",
                ("pgwerk",),
            )
            tables = {r[0] for r in await cur.fetchall()}

        expected = {
            "_pgwerk_job_deps",
            "_pgwerk_jobs",
            "_pgwerk_jobs_executions",
            "_pgwerk_versions",
            "_pgwerk_worker",
            "_pgwerk_worker_jobs",
        }
        assert expected <= tables

    @pytest.mark.asyncio
    async def test_no_default_tables_in_public_schema(self, connected_app):
        standard_tables = (
            "_pgwerk_jobs",
            "_pgwerk_worker",
            "_pgwerk_worker_jobs",
            "_pgwerk_jobs_executions",
            "_pgwerk_job_deps",
            "_pgwerk_versions",
        )
        async with await psycopg.AsyncConnection.connect(_TEST_DSN) as conn, conn.cursor() as cur:
            await cur.execute(
                "SELECT table_name FROM information_schema.tables"
                " WHERE table_schema = 'public' AND table_name = ANY(%s)",
                (list(standard_tables),),
            )
            tables = [r[0] for r in await cur.fetchall()]

        assert tables == []
