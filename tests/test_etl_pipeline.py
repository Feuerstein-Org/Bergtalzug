"""Contains the various tests for the ETL Pipeline implementation."""

import pytest
from conftest import MockETLPipelineFactory


@pytest.mark.asyncio
async def test_pipeline_starts_and_dies_naturally(mock_etl_pipeline: MockETLPipelineFactory) -> None:
    """Test that pipeline starts and finishes when there are no items to process."""
    # Create pipeline instance with no items to process
    pipeline = mock_etl_pipeline()

    # Verify pipeline starts with empty queue
    assert pipeline.items_to_process == []

    # Start the pipeline - it should complete immediately since no items to process
    await pipeline.run()

    # Verify pipeline completed without errors
    assert pipeline.items_to_process == []

    # Verify refill_queue was called at least once to check for items
    pipeline.mock_refill_queue.assert_called()
