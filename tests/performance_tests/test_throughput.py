"""Performance tests for ETL pipeline throughput"""

import pytest
from conftest import PerfTestETLPipelineFactory


@pytest.mark.asyncio
@pytest.mark.benchmark(group="throughput")
async def test_pipeline_throughput_small_batch(perf_etl_pipeline_factory: PerfTestETLPipelineFactory) -> None:
    """Test throughput with small batch of items"""
    pipeline = perf_etl_pipeline_factory.create(work_items_count=1000, items_size_range=(1024, 4096))

    async def run_pipeline() -> int:
        results = await pipeline.run()
        return len([r for r in results if r.success])

    processed_count = await run_pipeline()

    assert processed_count == 1000
