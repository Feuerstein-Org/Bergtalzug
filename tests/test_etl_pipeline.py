"""Contains the various tests for the ETL Pipeline implementation."""

import pytest
from pytest_mock import MockerFixture
from conftest import mock_etl_pipeline_factory


@pytest.mark.asyncio
async def test_pipeline_starts_and_dies_naturally(mocker: MockerFixture) -> None:
    """Test that pipeline starts and finishes when there are no items to process."""
    # Create pipeline instance with no items to process
    pipeline = mock_etl_pipeline_factory(mocker)

    assert pipeline.items_to_process == []  # pipeline should start with no items
    await pipeline.run()
    assert pipeline.items_to_process == []  # still empty after run

    # Verify refill_queue was called at least once to check for items
    pipeline.mock_refill_queue.assert_called()


# TODO: add tests that verify config + validation behavior.
# e.g. Validation errors, default values, overrides, etc.
