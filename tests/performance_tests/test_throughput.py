"""Performance tests for ETL pipeline throughput"""

import pytest
import asyncio
from pytest_codspeed import BenchmarkFixture
from conftest import PerfTestETLPipelineFactory


@pytest.mark.benchmark(group="throughput")
def test_pipeline_throughput_small_batch(
    perf_etl_pipeline_factory: PerfTestETLPipelineFactory, benchmark: BenchmarkFixture
) -> None:
    """Test throughput with small batch of items"""
    pipeline = perf_etl_pipeline_factory.create(work_items_count=100, items_size_range=(1024, 4096))

    def run_pipeline_sync() -> None:
        asyncio.run(pipeline.run())

    benchmark(run_pipeline_sync)
