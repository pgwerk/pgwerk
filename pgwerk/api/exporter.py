from __future__ import annotations

from ..exporter import WerkExporter

_state: dict = {}


async def get_exporter() -> WerkExporter | None:
    """Return the active WerkExporter instance, or None if not yet started.

    Returns:
        The running WerkExporter, or None during startup or if metrics are disabled.
    """
    return _state.get("exporter")


def init_exporter(exporter_interval: float, werk_state: dict, on_startup: list, on_shutdown: list) -> None:
    """Register Prometheus exporter lifecycle hooks.

    Creates and starts the exporter on startup once Werk is available,
    and stops it on shutdown. The running instance is accessible via
    get_exporter() for injection into MetricsController.

    Args:
        exporter_interval: Scrape interval in seconds.
        werk_state: Shared state dict holding the connected Werk instance under "werk".
        on_startup: List of startup callables to append to.
        on_shutdown: List of shutdown callables to append to.
    """

    async def _start_exporter() -> None:
        _state["exporter"] = WerkExporter(werk_state["werk"], interval=exporter_interval)
        await _state["exporter"].start()

    async def _stop_exporter() -> None:
        if "exporter" in _state:
            await _state["exporter"].stop()
            del _state["exporter"]

    on_startup.append(_start_exporter)
    on_shutdown.append(_stop_exporter)
