from __future__ import annotations

import asyncio
import logging

from typing import TYPE_CHECKING
from typing import Any


if TYPE_CHECKING:
    from prometheus_client import CollectorRegistry

    from ..app import Wrk

logger = logging.getLogger(__name__)

_STATUSES = ("scheduled", "queued", "active", "waiting", "failed", "complete", "aborted", "aborting")


class WrkExporter:
    """Prometheus metrics exporter for wrk.

    Runs a background asyncio task that polls the database every *interval*
    seconds and updates a set of Prometheus gauges.  The metrics are served
    via either :meth:`asgi_app` (returns an ASGI callable suitable for
    mounting under any ASGI framework) or :meth:`start_http_server` (spins
    up a lightweight HTTP server in a daemon thread).

    Usage::

        exporter = WrkExporter(wrk, interval=15)
        await exporter.start()          # begin background collection
        app = exporter.asgi_app()       # mount at /metrics in your ASGI app
        # or
        exporter.start_http_server(9090)

    The exporter must be stopped when the application shuts down::

        await exporter.stop()
    """

    def __init__(
        self,
        wrk: "Wrk",
        interval: float = 15.0,
        registry: "CollectorRegistry | None" = None,
        namespace: str = "wrk",
        latency_window_secs: int = 600,
    ) -> None:
        try:
            from prometheus_client import Gauge
            from prometheus_client import CollectorRegistry as _Registry
        except ImportError as exc:
            raise ImportError(
                "prometheus-client is required for WrkExporter. Install it with: pip install prometheus-client"
            ) from exc

        self._wrk = wrk
        self._interval = interval
        self._latency_window = latency_window_secs
        self._task: asyncio.Task[None] | None = None

        self._registry: CollectorRegistry = registry or _Registry()

        def _g(name: str, doc: str, labels: list[str] | None = None) -> Any:
            return Gauge(f"{namespace}_{name}", doc, labels or [], registry=self._registry)

        self._jobs = _g("jobs", "Number of jobs by queue and status", ["queue", "status"])
        self._workers_online = _g("workers_online", "Workers with a heartbeat in the last 60 s")
        self._workers_total = _g("workers_total", "Total registered workers")
        self._wait_seconds = _g(
            "job_wait_seconds",
            f"Average queue wait time (scheduled→started) over the last {latency_window_secs}s, by queue",
            ["queue"],
        )
        self._duration_seconds = _g(
            "job_duration_seconds",
            f"Average processing time (started→completed) over the last {latency_window_secs}s, by queue",
            ["queue"],
        )
        self._throughput = _g(
            "jobs_completed_total",
            f"Jobs completed in the last {latency_window_secs}s, by queue",
            ["queue"],
        )
        self._failed_total = _g(
            "jobs_failed_total",
            f"Jobs failed in the last {latency_window_secs}s, by queue",
            ["queue"],
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Start the background collection loop."""
        if self._task is not None:
            return
        self._task = asyncio.create_task(self._loop(), name="wrk-exporter")
        logger.info("wrk exporter: started (interval=%.1fs)", self._interval)

    async def stop(self) -> None:
        """Cancel the background collection loop."""
        if self._task is None:
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        self._task = None
        logger.info("wrk exporter: stopped")

    async def __aenter__(self) -> "WrkExporter":
        await self.start()
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.stop()

    # ------------------------------------------------------------------
    # Collection loop
    # ------------------------------------------------------------------

    async def _loop(self) -> None:
        while True:
            try:
                await self.collect()
            except Exception:
                logger.exception("wrk exporter: collect failed")
            await asyncio.sleep(self._interval)

    async def collect(self) -> None:
        """Run a single collection pass and update all gauges."""
        from psycopg.sql import SQL
        from psycopg.rows import dict_row

        pool = self._wrk._pool_or_raise()
        t = self._wrk._t

        async with pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                # --- job counts by queue + status ---
                await cur.execute(
                    SQL("""
                        SELECT
                            queue,
                            COUNT(*) FILTER (WHERE status = 'scheduled') AS scheduled,
                            COUNT(*) FILTER (WHERE status = 'queued')   AS queued,
                            COUNT(*) FILTER (WHERE status = 'active')   AS active,
                            COUNT(*) FILTER (WHERE status = 'waiting')  AS waiting,
                            COUNT(*) FILTER (WHERE status = 'failed')   AS failed,
                            COUNT(*) FILTER (WHERE status = 'complete') AS complete,
                            COUNT(*) FILTER (WHERE status = 'aborted')  AS aborted,
                            COUNT(*) FILTER (WHERE status = 'aborting') AS aborting
                        FROM {jobs}
                        GROUP BY queue
                    """).format(jobs=t["jobs"])
                )
                for row in await cur.fetchall():
                    q = row["queue"]
                    for status in _STATUSES:
                        self._jobs.labels(queue=q, status=status).set(row[status])

                # --- worker counts ---
                await cur.execute(
                    SQL("""
                        SELECT
                            COUNT(*) AS total,
                            COUNT(*) FILTER (WHERE last_heartbeat > NOW() - INTERVAL '30 seconds') AS online
                        FROM {worker}
                    """).format(worker=t["worker"])
                )
                row = await cur.fetchone()
                if row:
                    self._workers_total.set(row["total"])
                    self._workers_online.set(row["online"])

                # --- latency / throughput over window ---
                await cur.execute(
                    SQL("""
                        SELECT
                            queue,
                            AVG(EXTRACT(EPOCH FROM (started_at - scheduled_at)))    AS avg_wait,
                            AVG(EXTRACT(EPOCH FROM (completed_at - started_at)))    AS avg_duration,
                            COUNT(*) FILTER (WHERE status = 'complete')             AS completed,
                            COUNT(*) FILTER (WHERE status IN ('failed', 'aborted')) AS failed
                        FROM {jobs}
                        WHERE completed_at > NOW() - %(window)s * INTERVAL '1 second'
                          AND started_at IS NOT NULL
                          AND completed_at IS NOT NULL
                        GROUP BY queue
                    """).format(jobs=t["jobs"]),
                    {"window": self._latency_window},
                )
                for row in await cur.fetchall():
                    q = row["queue"]
                    if row["avg_wait"] is not None:
                        self._wait_seconds.labels(queue=q).set(float(row["avg_wait"]))
                    if row["avg_duration"] is not None:
                        self._duration_seconds.labels(queue=q).set(float(row["avg_duration"]))
                    self._throughput.labels(queue=q).set(row["completed"])
                    self._failed_total.labels(queue=q).set(row["failed"])

    # ------------------------------------------------------------------
    # Exposition
    # ------------------------------------------------------------------

    def metrics_bytes(self) -> tuple[bytes, str]:
        """Return ``(body, content_type)`` in Prometheus text exposition format."""
        from prometheus_client import CONTENT_TYPE_LATEST
        from prometheus_client import generate_latest

        return generate_latest(self._registry), CONTENT_TYPE_LATEST

    def asgi_app(self):
        """Return an ASGI callable that serves the /metrics endpoint.

        Mount it at ``/metrics`` in your ASGI framework::

            from litestar import Litestar
            from litestar.middleware.base import ASGIMiddlewareProtocol
            # or just expose as a raw ASGI app at a sub-path
        """
        exporter = self

        async def _app(scope, _receive, send):
            if scope["type"] != "http":
                return
            body, content_type = exporter.metrics_bytes()
            await send(
                {
                    "type": "http.response.start",
                    "status": 200,
                    "headers": [
                        [b"content-type", content_type.encode()],
                        [b"content-length", str(len(body)).encode()],
                    ],
                }
            )
            await send({"type": "http.response.body", "body": body, "more_body": False})

        return _app

    def start_http_server(self, port: int = 9090, addr: str = "") -> None:
        """Start a Prometheus HTTP server in a daemon thread.

        This is a thin wrapper around ``prometheus_client.start_http_server``
        using this exporter's private registry so it does not conflict with
        the default global registry.
        """
        from prometheus_client import start_http_server

        start_http_server(port, addr=addr, registry=self._registry)
        logger.info("wrk exporter: HTTP server listening on %s:%d", addr or "0.0.0.0", port)
