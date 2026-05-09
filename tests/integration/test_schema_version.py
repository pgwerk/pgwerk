"""Integration tests for the schema version compatibility check."""

from __future__ import annotations

import os

import pytest
import psycopg
import pytest_asyncio

from pgwerk.app import Werk
from pgwerk.config import WerkConfig
from pgwerk.exceptions import SchemaVersionMismatch


_TEST_DSN = os.environ.get("PGWERK_TEST_DSN", "postgresql://pgwerk:pgwerk@localhost/pgwerk_test")
_TEST_SCHEMA = "pgwerk"
_TEST_PREFIX = "_pgwerk"
_VERSIONS_TABLE = f'"{_TEST_SCHEMA}"."{_TEST_PREFIX}_versions"'


async def _set_db_version(value: int) -> None:
    async with await psycopg.AsyncConnection.connect(_TEST_DSN, autocommit=True) as conn:
        await conn.execute(f"UPDATE {_VERSIONS_TABLE} SET version = %s", (value,))


async def _read_db_version() -> int:
    async with await psycopg.AsyncConnection.connect(_TEST_DSN, autocommit=True) as conn:
        cur = await conn.execute(f"SELECT version FROM {_VERSIONS_TABLE}")
        row = await cur.fetchone()
        return row[0] if row else 0


@pytest_asyncio.fixture
async def restore_version(app):
    """Snapshot the version row, run the test, restore it."""
    original = await _read_db_version()
    yield
    await _set_db_version(original)


@pytest_asyncio.fixture
def restore_compat_constants():
    """Snapshot WerkConfig min/max class vars and restore them after the test."""
    orig_min = WerkConfig.min_compatible_db_version
    orig_max = WerkConfig.max_compatible_db_version
    yield
    WerkConfig.min_compatible_db_version = orig_min
    WerkConfig.max_compatible_db_version = orig_max


class TestCheckVersion:
    @pytest.mark.asyncio
    async def test_passes_at_current_version(self, app):
        async with await psycopg.AsyncConnection.connect(_TEST_DSN, autocommit=True) as conn:
            result = await app._db.check_version(conn)
        assert result == WerkConfig.schema_version

    @pytest.mark.asyncio
    async def test_raises_when_db_below_min(self, app, restore_version):
        await _set_db_version(WerkConfig.min_compatible_db_version - 1)
        async with await psycopg.AsyncConnection.connect(_TEST_DSN, autocommit=True) as conn:
            with pytest.raises(SchemaVersionMismatch, match=r"requires >="):
                await app._db.check_version(conn)

    @pytest.mark.asyncio
    async def test_raises_when_db_above_max(self, app, restore_version, restore_compat_constants):
        WerkConfig.max_compatible_db_version = WerkConfig.schema_version
        await _set_db_version(WerkConfig.schema_version + 1)
        async with await psycopg.AsyncConnection.connect(_TEST_DSN, autocommit=True) as conn:
            with pytest.raises(SchemaVersionMismatch, match=r"only supports <="):
                await app._db.check_version(conn)

    @pytest.mark.asyncio
    async def test_passes_when_max_is_none(self, app, restore_version, restore_compat_constants):
        WerkConfig.max_compatible_db_version = None
        await _set_db_version(WerkConfig.schema_version + 5)
        async with await psycopg.AsyncConnection.connect(_TEST_DSN, autocommit=True) as conn:
            assert await app._db.check_version(conn) == WerkConfig.schema_version + 5


class TestConnectVersionGate:
    @pytest.mark.asyncio
    async def test_auto_migrate_false_succeeds_on_current_db(self, app):
        # `app` already migrated the DB to current. A second instance with
        # auto_migrate=False should connect cleanly.
        observer = Werk(_TEST_DSN, prefix=_TEST_PREFIX, auto_migrate=False)
        await observer.connect()
        try:
            assert observer._connected is True
        finally:
            await observer.disconnect()

    @pytest.mark.asyncio
    async def test_auto_migrate_false_refuses_old_db(self, app, restore_version):
        await _set_db_version(WerkConfig.min_compatible_db_version - 1)
        observer = Werk(_TEST_DSN, prefix=_TEST_PREFIX, auto_migrate=False)
        with pytest.raises(SchemaVersionMismatch):
            await observer.connect()
        assert observer._connected is False

