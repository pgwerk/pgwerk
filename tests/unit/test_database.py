from __future__ import annotations

from psycopg.sql import Identifier

from tests.config import WrkConfig
from tests.database import DatabaseManager


class TestDatabaseManagerTable:
    def test_no_schema(self):
        db = DatabaseManager(schema=None, prefix="wrk")
        result = db.table("jobs")
        assert result == Identifier("wrk_jobs")

    def test_public_schema_not_qualified(self):
        db = DatabaseManager(schema="public", prefix="wrk")
        result = db.table("jobs")
        assert result == Identifier("wrk_jobs")

    def test_custom_schema_qualified(self):
        db = DatabaseManager(schema="myapp", prefix="wrk")
        result = db.table("jobs")
        assert result == Identifier("myapp", "wrk_jobs")

    def test_prefix_applied(self):
        db = DatabaseManager(schema=None, prefix="myprefix")
        result = db.table("worker")
        assert result == Identifier("myprefix_worker")


class TestDatabaseManagerDDL:
    def test_ddl_returns_statements(self):
        db = DatabaseManager(schema=None, prefix="wrk")
        stmts = db._ddl()
        assert len(stmts) > 0

    def test_migrations_returns_list(self):
        db = DatabaseManager(schema=None, prefix="wrk")
        migrations = db._migrations()
        assert isinstance(migrations, list)

    def test_schema_version_positive(self):
        assert WrkConfig.schema_version > 0
