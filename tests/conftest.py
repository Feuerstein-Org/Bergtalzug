"""Test fixtures for ETL Pipeline tests."""

from dataclasses import dataclass
from bergtalzug import ETLPipeline, WorkItem
from pytest_mock import MockerFixture
from typing import Any
from uuid import uuid4
import asyncio
import time


def work_item_factory(
    count: int = 1, data: str | bytes = b"test_data", metadata: Any = None, job_id: str | None = None
) -> list[WorkItem]:
    """
    Create a list of WorkItem instances with custom data, by default one item.

    count determines how many items to create
    job_id is only applied to the first item, others get a random value
    data and metadata will be the same across all items created
    """
    work_items: list[WorkItem] = []
    if metadata is None:
        metadata = {"source": "test"}
    if isinstance(data, str):
        data = data.encode()
    if job_id is None:
        job_id = str(uuid4().hex[:6])
    for _ in range(count):
        work_items.append(WorkItem(data=data, metadata=metadata, job_id=job_id))
        job_id = str(uuid4().hex[:6])  # TODO: Later this will be handled in real class
    return work_items


@dataclass
class MockETLPipelineConfig:
    """
    Configuration for the MockETLPipeline class.

    Values that are not set will use the default values from the ETLPipeline class.
    """

    pipeline_name: str = "mock_etl_pipeline"
    fetch_workers: int = 3
    process_workers: int = 2
    upload_workers: int = 3
    fetch_queue_size: int = 10
    process_queue_size: int = 5
    store_queue_size: int = 10
    # Test specific parameters
    items_to_process: list[WorkItem] | None = None
    refill_queue_sleep: float | None = None
    fetch_sleep: float | None = None
    process_sleep: float | None = None
    store_sleep: float | None = None


class MockETLPipeline(ETLPipeline):
    """
    Mock implementation of ETLPipeline for testing purposes.

    This class overrides the abstract methods to provide mock behavior.
    You can call mock methods on mock_refill_queue, mock_fetch, mock_process, mock_store
    """

    def __init__(self, mocker: MockerFixture, config: MockETLPipelineConfig | None = None) -> None:
        """Initialize the mock ETL pipeline with configuration."""
        # Use default config if none provided
        if config is None:
            config = MockETLPipelineConfig()

        # Pass ETL pipeline parameters to parent constructor
        etl_kwargs = {
            "fetch_workers": config.fetch_workers,
            "process_workers": config.process_workers,
            "upload_workers": config.upload_workers,
            "fetch_queue_size": config.fetch_queue_size,
            "process_queue_size": config.process_queue_size,
            "store_queue_size": config.store_queue_size,
        }

        super().__init__(config=None, pipeline_name=config.pipeline_name, **etl_kwargs)

        # Store configuration
        self.config = config

        # Should be replaced soon once we allow WorkItems to be set on startup
        self.items_to_process: list[WorkItem] = config.items_to_process or []

        # Create mocks with implementation
        self.mock_refill_queue = mocker.AsyncMock(side_effect=self._refill_queue_impl)
        self.mock_fetch = mocker.AsyncMock(side_effect=self._fetch_impl)
        self.mock_process = mocker.Mock(side_effect=self._process_impl)
        self.mock_store = mocker.AsyncMock(side_effect=self._store_impl)

        # Replace the actual methods with mocks
        self.refill_queue = self.mock_refill_queue
        self.fetch = self.mock_fetch
        self.process = self.mock_process
        self.store = self.mock_store

    async def _refill_queue_impl(self, count: int) -> list[WorkItem]:
        if self.config.refill_queue_sleep is not None and self.config.refill_queue_sleep > 0:
            await asyncio.sleep(self.config.refill_queue_sleep)

        if self.items_to_process:
            batch = self.items_to_process[:count]
            self.items_to_process = self.items_to_process[count:]
            return batch
        return []

    async def _fetch_impl(self, item: WorkItem) -> WorkItem:
        if self.config.fetch_sleep is not None and self.config.fetch_sleep > 0:
            await asyncio.sleep(self.config.fetch_sleep)

        item.metadata["fetched"] = True
        return item

    def _process_impl(self, item: WorkItem) -> WorkItem:
        if self.config.process_sleep is not None and self.config.process_sleep > 0:
            time.sleep(self.config.process_sleep)

        item.data = b"processed_" + item.data
        item.metadata["processed"] = True
        return item

    async def _store_impl(self, item: WorkItem) -> None:
        if self.config.store_sleep is not None and self.config.store_sleep > 0:
            await asyncio.sleep(self.config.store_sleep)

        item.metadata["stored"] = True


# This is needed to disable the abstract methods in the base class
# since we are mocking them
MockETLPipeline.__abstractmethods__ = frozenset()


# NOTE: Currently this is not a fixture, but a factory function,
# The reason is that currently there are no tests sharing the same mock ETL pipeline.
# This means the mocker fixture must be passed explicitly in tests.
def mock_etl_pipeline_factory(mocker: MockerFixture, config: MockETLPipelineConfig | None = None) -> MockETLPipeline:
    """Create a mock ETL pipeline implementation for testing."""
    return MockETLPipeline(mocker, config)
