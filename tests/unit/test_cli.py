from __future__ import annotations

from unittest.mock import MagicMock
from unittest.mock import patch

import click
import pytest

from click.testing import CliRunner

from pgwerk.cli import cli
from pgwerk.cli.utils import load_app


class TestLoadApp:
    def test_valid_path(self):
        mock_app = MagicMock()
        with patch("pgwerk.cli.utils.importlib.import_module") as mock_import:
            mock_module = MagicMock()
            mock_module.app = mock_app
            mock_import.return_value = mock_module
            result = load_app("mymodule:app")
        assert result is mock_app

    def test_bad_format_no_colon(self):
        with pytest.raises(click.BadParameter):
            load_app("no_colon_here")

    def test_import_error_raises_click_exception(self):
        with patch("pgwerk.cli.utils.importlib.import_module", side_effect=ImportError("no module")):
            with pytest.raises(click.ClickException, match="Cannot import"):
                load_app("bad_module:app")

    def test_missing_attribute_raises_click_exception(self):
        with patch("pgwerk.cli.utils.importlib.import_module") as mock_import:
            mock_module = MagicMock(spec=[])
            mock_import.return_value = mock_module
            with pytest.raises(click.ClickException, match="no attribute"):
                load_app("mymodule:nonexistent_attr")


class TestCliCommands:
    def test_help_message(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "wrk" in result.output.lower()

    def test_worker_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["worker", "--help"])
        assert result.exit_code == 0

    def test_info_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["info", "--help"])
        assert result.exit_code == 0

    def test_purge_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["purge", "--help"])
        assert result.exit_code == 0

    def test_worker_bad_app_path(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["worker", "no_colon"])
        assert result.exit_code != 0

    def test_worker_import_error(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["worker", "no_such_module_xyz:app"])
        assert result.exit_code == 1
        assert "Cannot import" in result.output

    def test_info_import_error(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["info", "no_such_module_xyz:app"])
        assert result.exit_code == 1

    def test_worker_missing_attribute(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["worker", "os:no_such_attr_xyz"])
        assert result.exit_code == 1
        assert "no attribute" in result.output

    def test_cron_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["cron", "--help"])
        assert result.exit_code == 0

    def test_cron_bad_app_path(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["cron", "no_colon"])
        assert result.exit_code != 0

    def test_cron_import_error(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["cron", "no_such_module_xyz:scheduler"])
        assert result.exit_code == 1
        assert "Cannot import" in result.output

    def test_cron_missing_attribute(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["cron", "os:no_such_attr_xyz"])
        assert result.exit_code == 1
        assert "no attribute" in result.output


_PGWERK_KEYS = (
    "PGWERK_DSN",
    "PGWERK_SCHEMA",
    "PGWERK_PREFIX",
    "PGWERK_MAX_ACTIVE_SECS",
    "PGWERK_HEARTBEAT_INTERVAL",
    "PGWERK_POLL_INTERVAL",
    "PGWERK_ABORT_INTERVAL",
    "PGWERK_SWEEP_INTERVAL",
    "PGWERK_SHUTDOWN_TIMEOUT",
    "PGWERK_SIGTERM_GRACE",
    "PGWERK_EPHEMERAL_TABLES",
    "PGWERK_METRICS",
    "PGWERK_METRICS_INTERVAL",
    "PGWERK_NO_UI",
    "PGWERK_UI_AUTH",
    "PGWERK_API_TOKEN",
    "PGWERK_DEFAULT_RETRY_BACKOFF",
    "PGWERK_ALLOW_TRUNCATE",
    "PGWERK_LISTEN",
)


class TestApiCommand:
    @pytest.fixture(autouse=True)
    def _isolate_env(self, monkeypatch):
        # to_env() writes os.environ directly; registering every key with monkeypatch undoes those writes.
        for key in _PGWERK_KEYS:
            monkeypatch.delenv(key, raising=False)

    def test_api_preserves_env_only_config(self, monkeypatch):
        import sys

        from pgwerk.config import WerkConfig

        fake_uvicorn = MagicMock()
        monkeypatch.setitem(sys.modules, "uvicorn", fake_uvicorn)
        monkeypatch.setenv("PGWERK_ALLOW_TRUNCATE", "true")
        monkeypatch.setenv("PGWERK_EPHEMERAL_TABLES", "true")
        monkeypatch.setenv("PGWERK_LISTEN", "false")
        monkeypatch.setenv("PGWERK_POLL_INTERVAL", "1.5")

        result = CliRunner().invoke(cli, ["api", "--dsn", "postgresql://x/y", "--schema", "custom"])

        assert result.exit_code == 0, result.output
        fake_uvicorn.run.assert_called_once()
        cfg = WerkConfig.from_env()
        assert cfg.allow_truncate is True
        assert cfg.ephemeral_tables is True
        assert cfg.listen is False
        assert cfg.poll_interval == 1.5
        assert cfg.schema == "custom"
        assert cfg.dsn == "postgresql://x/y"
