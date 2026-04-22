from __future__ import annotations

from unittest.mock import MagicMock
from unittest.mock import patch

import click
import pytest

from click.testing import CliRunner

from tests.cli import cli
from tests.cli.utils import load_app


class TestLoadApp:
    def test_valid_path(self):
        mock_app = MagicMock()
        with patch("wrk.cli.utils.importlib.import_module") as mock_import:
            mock_module = MagicMock()
            mock_module.app = mock_app
            mock_import.return_value = mock_module
            result = load_app("mymodule:app")
        assert result is mock_app

    def test_bad_format_no_colon(self):
        with pytest.raises(click.BadParameter):
            load_app("no_colon_here")

    def test_import_error_raises_click_exception(self):
        with patch("wrk.cli.utils.importlib.import_module", side_effect=ImportError("no module")):
            with pytest.raises(click.ClickException, match="Cannot import"):
                load_app("bad_module:app")

    def test_missing_attribute_raises_click_exception(self):
        with patch("wrk.cli.utils.importlib.import_module") as mock_import:
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
