from __future__ import annotations

import os

from litestar.di import Provide

from ..app import Werk


async def _create_pgwerk(
    dsn: str | None,
    schema: str | None,
    prefix: str | None,
) -> Werk:
    """Create and connect a Werk instance.

    Args:
        dsn: Postgres connection string. Falls back to PGWERK_DSN if None.
        schema: Postgres schema for wrk tables, or None for the default.
        prefix: Table-name prefix, or None for the default.

    Returns:
        A connected Werk instance.
    """
    werk = Werk(dsn or os.environ["PGWERK_DSN"], schema=schema, prefix=prefix, auto_migrate=False)
    await werk.connect()
    return werk


def init_werk(
    dsn: str | None,
    schema: str | None,
    prefix: str | None,
    state: dict,
    dependencies: dict,
    on_startup: list,
    on_shutdown: list,
) -> None:
    """Register the Werk dependency and its lifecycle hooks.

    Creates a Werk instance on startup from the given config (or PGWERK_DSN)
    and disconnects it on shutdown.

    Args:
        dsn: Postgres connection string. Falls back to PGWERK_DSN if None.
        schema: Postgres schema for wrk tables, or None for the default.
        prefix: Table-name prefix, or None for the default.
        state: Shared mutable state dict; the instance is stored under "werk".
        dependencies: Litestar dependency map to register the werk provider into.
        on_startup: List of startup callables to append to.
        on_shutdown: List of shutdown callables to append to.
    """

    async def _startup() -> None:
        state["werk"] = await _create_pgwerk(dsn, schema, prefix)

    async def _shutdown() -> None:
        if "werk" in state:
            await state["werk"].disconnect()

    on_startup.append(_startup)
    on_shutdown.append(_shutdown)

    async def _get_werk() -> Werk:
        return state["werk"]

    dependencies["werk"] = Provide(_get_werk, use_cache=True)
