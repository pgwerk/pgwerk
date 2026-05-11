"""Integration tests for WerkExporter.collect() against a real database."""

from __future__ import annotations

import pytest

from pgwerk.exporter import WerkExporter

from .tasks import noop, add
from .conftest import make_worker

pytest.importorskip("prometheus_client")


class TestWerkExporterCollect:
    async def test_collect_does_not_raise_on_empty_db(self, app):
        exporter = WerkExporter(app)
        await exporter.collect()

    async def test_collect_returns_zero_counts_on_empty_db(self, app):
        exporter = WerkExporter(app)
        await exporter.collect()

        assert exporter._workers_online._value.get() == 0
        assert exporter._workers_total._value.get() == 0

    async def test_collect_counts_queued_jobs(self, app):
        await app.enqueue(noop)
        await app.enqueue(noop)

        exporter = WerkExporter(app)
        await exporter.collect()

        queued = sum(
            g._value.get()
            for labels, g in exporter._jobs._metrics.items()
            if "queued" in labels
        )
        assert queued == 2

    async def test_collect_counts_completed_jobs(self, app):
        await app.enqueue(add, 1, 2)
        await make_worker(app).run()

        exporter = WerkExporter(app)
        await exporter.collect()

        complete = sum(
            g._value.get()
            for labels, g in exporter._jobs._metrics.items()
            if "complete" in labels
        )
        assert complete == 1

    async def test_collect_populates_throughput_after_completion(self, app):
        await app.enqueue(add, 1, 2)
        await make_worker(app).run()

        exporter = WerkExporter(app)
        await exporter.collect()

        total_completed = sum(
            g._value.get() for g in exporter._throughput._metrics.values()
        )
        assert total_completed == 1

    async def test_collect_populates_duration_after_completion(self, app):
        await app.enqueue(add, 1, 2)
        await make_worker(app).run()

        exporter = WerkExporter(app)
        await exporter.collect()

        total_duration = sum(
            g._value.get() for g in exporter._duration_seconds._metrics.values()
        )
        assert total_duration >= 0

    async def test_collect_counts_online_workers(self, app):
        await app.enqueue(noop)
        await make_worker(app).run()

        exporter = WerkExporter(app)
        await exporter.collect()

        assert exporter._workers_total._value.get() >= 1

    async def test_metrics_bytes_returns_valid_prometheus_text(self, app):
        exporter = WerkExporter(app)
        await exporter.collect()

        body, content_type = exporter.metrics_bytes()
        assert b"wrk_jobs" in body
        assert b"wrk_workers_online" in body
        assert "text/plain" in content_type
