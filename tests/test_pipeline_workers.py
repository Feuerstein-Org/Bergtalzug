"""Tests for the pipeline workers."""

import pytest
from bergtalzug import WorkItem
from conftest import MockETLPipelineFactory


class TestPipelineWorkers:
    """Test individual worker components"""

    @pytest.mark.asyncio
    async def test_fetch_worker_flow(self, mock_etl_pipeline_factory: MockETLPipelineFactory) -> None:
        """Test fetch worker processes items correctly"""
        pipeline = mock_etl_pipeline_factory.create()
        await pipeline.setup_queues()

        item = WorkItem(data=b"test")
        await pipeline.fetch_queue.async_put(item)
        await pipeline.fetch_queue.async_put(None)  # Poison pill

        await pipeline.fetch_worker()

        pipeline.mock_fetch.assert_called_once_with(item)
        item_to_process = await pipeline.process_queue.async_get()
        assert item_to_process is not None

    @pytest.mark.asyncio
    async def test_process_worker_flow(self, mock_etl_pipeline_factory: MockETLPipelineFactory) -> None:
        """Test process worker in processes items correctly"""
        pipeline = mock_etl_pipeline_factory.create()
        await pipeline.setup_queues()

        item = WorkItem(data=b"test")
        await pipeline.process_queue.async_put(item)
        await pipeline.process_queue.async_put(None)  # Poison pill

        await pipeline.process_worker()

        pipeline.mock_process.assert_called_once_with(item)
        item_to_store = await pipeline.store_queue.async_get()
        assert item_to_store is not None
        assert item_to_store.data == b"processed_test"

    @pytest.mark.asyncio
    async def test_store_worker_flow(self, mock_etl_pipeline_factory: MockETLPipelineFactory) -> None:
        """Test store worker processes items"""
        pipeline = mock_etl_pipeline_factory.create()
        await pipeline.setup_queues()

        item = WorkItem(data=b"test")
        await pipeline.store_queue.async_put(item)
        await pipeline.store_queue.async_put(None)  # Poison pill

        await pipeline.store_worker()
        pipeline.mock_store.assert_called_once_with(item)
