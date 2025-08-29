"""Tests for the complete ETL pipeline integration."""

import pytest
import asyncio
from conftest import work_item_factory, MockETLPipelineFactory
from bergtalzug import WorkItem


class TestPipelineIntegration:
    """Test complete pipeline flow"""

    @pytest.mark.asyncio
    async def test_pipeline_starts_and_dies_naturally(self, mock_etl_pipeline_factory: MockETLPipelineFactory) -> None:
        """Test that pipeline starts and finishes when there are no items to process."""
        # Create pipeline instance with no items to process
        pipeline = mock_etl_pipeline_factory.create()

        assert pipeline.items_to_process == []  # pipeline should start with no items
        await pipeline.run()
        assert pipeline.items_to_process == []  # still empty after run

        # Verify refill_queue was called at least once to check for items
        pipeline.mock_refill_queue.assert_called()

    @pytest.mark.asyncio
    async def test_simple_pipeline_flow(self, mock_etl_pipeline_factory: MockETLPipelineFactory) -> None:
        """Test basic end-to-end flow"""
        pipeline = mock_etl_pipeline_factory.create_items(5)
        results = await pipeline.run()

        # Verify all items processed
        assert len(results) == 5
        assert all(r.success for r in results)

        # Verify all stages were called
        assert pipeline.mock_fetch.call_count == 5
        assert pipeline.mock_process.call_count == 5
        assert pipeline.mock_store.call_count == 5

    @pytest.mark.asyncio
    async def test_pipeline_with_errors(self, mock_etl_pipeline_factory: MockETLPipelineFactory) -> None:
        """Test pipeline handles errors gracefully"""
        items = work_item_factory(count=3)
        pipeline = mock_etl_pipeline_factory.pass_items(items)

        # Make second item fail during processing
        async def process_with_error(item: WorkItem) -> WorkItem:
            if item.job_id == items[1].job_id:
                raise ValueError("Processing error")
            return item

        pipeline.mock_process.side_effect = process_with_error

        results = await pipeline.run()

        # Check that error was handled
        success_count = sum(1 for r in results if r.success)
        error_count = sum(1 for r in results if not r.success)

        assert success_count == 2
        assert error_count == 1

    @pytest.mark.asyncio
    async def test_pipeline_queue_management(self, mock_etl_pipeline_factory: MockETLPipelineFactory) -> None:
        """Test queue refill mechanism"""
        pipeline = mock_etl_pipeline_factory.create_items(20, fetch_queue_size=5)
        await pipeline.run()

        # Verify refill_queue was called 6 times:
        # currently queue always refills to 90%.
        # That is 4 items per refill with a queue size of 5.
        # 20 items / 4 items per refill = 5 refills
        # At the end one more to initiate shutdown
        assert pipeline.mock_refill_queue.call_count == 6

    @pytest.mark.asyncio
    async def test_pipeline_concurrent_processing(self, mock_etl_pipeline_factory: MockETLPipelineFactory) -> None:
        """Test concurrent processing maintains data integrity"""
        pipeline = mock_etl_pipeline_factory.create_items(
            50, fetch_workers=5, process_workers=5, store_workers=5, process_sleep=0.01
        )
        results = await pipeline.run()

        # All items should be processed despite concurrency
        assert len(results) == 50

        # Check no duplicate processing
        job_ids = [r.job_id for r in results]
        assert len(job_ids) == len(set(job_ids))

    @pytest.mark.asyncio
    async def test_pipeline_updates_tracker_stages(self, mock_etl_pipeline_factory: MockETLPipelineFactory) -> None:
        """Test that pipeline correctly updates tracker stages"""
        pipeline = mock_etl_pipeline_factory.create_items(2, process_sleep=0.01)
        assert pipeline.tracker is not None
        tracker = pipeline.tracker

        # Start pipeline but don't await yet
        task = asyncio.create_task(pipeline.run())

        # Give pipeline time to start processing
        await asyncio.sleep(0.005)

        # Check that items are in progress (not all completed)
        stats = await tracker.get_statistics()
        assert stats["active_items"] > 0

        # Wait for completion
        await task

        # Verify all items completed
        final_stats = await tracker.get_statistics()
        assert final_stats["active_items"] == 0
        assert final_stats["completed_items"] == 2
        assert final_stats["success_rate"] == 1.0
