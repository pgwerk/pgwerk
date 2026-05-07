from __future__ import annotations

import click

from ..logging import configure_logging


@click.command()
@click.option("--dsn", envvar="PGWERK_DSN", required=True, help="Postgres connection string.")
@click.option("--host", "-h", default="127.0.0.1", show_default=True, help="Host to bind.")
@click.option("--port", "-p", default=8000, show_default=True, help="Port to bind.")
@click.option("--reload", is_flag=True, default=False, help="Enable auto-reload (development).")
@click.option("--schema", default=None, help="Postgres schema for wrk tables.")
@click.option("--prefix", default=None, help="Table-name prefix.")
@click.option("--metrics", is_flag=True, default=False, help="Serve Prometheus metrics at GET /metrics.")
@click.option("--metrics-interval", default=15.0, show_default=True, help="Metrics collection interval in seconds.")
@click.option(
    "--log-level",
    "-l",
    default="INFO",
    show_default=True,
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"], case_sensitive=False),
)
@click.option(
    "--log-format",
    default="text",
    show_default=True,
    type=click.Choice(["text", "json"], case_sensitive=False),
)
@click.option("--no-ui", is_flag=True, default=False, help="Disable the SPA dashboard.")
@click.option("--auth", default=None, envvar="PGWERK_AUTH", help="Basic Auth for the SPA as user:password.")
@click.option("--token", default=None, envvar="PGWERK_TOKEN", help="Bearer token for the API.")
@click.option("--no-color", is_flag=True, default=False, help="Disable colored log output.")
def api(
    dsn: str,
    host: str,
    port: int,
    reload: bool,
    schema: str | None,
    prefix: str | None,
    metrics: bool,
    metrics_interval: float,
    no_ui: bool,
    auth: str | None,
    token: str | None,
    log_level: str,
    log_format: str,
    no_color: bool,
) -> None:
    """Start the HTTP API server."""
    try:
        import uvicorn
    except ImportError:
        raise click.ClickException("This command requires 'uvicorn'. Install with: pip install 'wrk[api]'")

    configure_logging(level=log_level, format=log_format, color=False if no_color else None)

    from ..api.app import create_app

    if reload:
        uvicorn.run(
            "pgwerk.api.app:create_app",
            factory=True,
            host=host,
            port=port,
            reload=True,
            log_level=log_level.lower(),
        )
    else:
        litestar_app = create_app(
            dsn=dsn,
            schema=schema,
            prefix=prefix,
            metrics=metrics,
            metrics_interval=metrics_interval,
            ui=not no_ui,
            auth=auth,
            token=token,
        )
        uvicorn.run(litestar_app, host=host, port=port, log_level=log_level.lower())
