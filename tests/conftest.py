"""Test fixtures for ETL Pipeline tests."""

from dataclasses import dataclass
from bergtalzug import ETLPipeline, ETLPipelineConfig, WorkItem
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

    No validation is performed here. This class is used to have lower testing defaults
    and is passed to the ETLPipelineConfig class which performs validation.
    Validation can be disabled by setting "bypass_validation" to True.
    """

    pipeline_name: str = "mock_etl_pipeline"
    fetch_workers: int = 3
    process_workers: int = 2
    upload_workers: int = 3
    fetch_queue_size: int = 10
    process_queue_size: int = 5
    store_queue_size: int = 10
    enable_tracking: bool = True
    stats_interval_seconds: float = 10.0
    # Test specific parameters
    bypass_validation: bool = False  # If true, skips config validation in ETLPipeline
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

    Each worker pool can have the size set to 0 to disable it for testing.
    """

    def __init__(self, mocker: MockerFixture, config: MockETLPipelineConfig | None = None) -> None:
        """Initialize the mock ETL pipeline with configuration."""
        # Use default config if none provided
        if config is None:
            config = MockETLPipelineConfig()

        # Pass ETL pipeline parameters to parent constructor
        if config.bypass_validation:
            # Bypass validation
            etl_config = ETLPipelineConfig.model_construct(
                pipeline_name=config.pipeline_name,
                fetch_workers=config.fetch_workers,
                process_workers=config.process_workers,
                upload_workers=config.upload_workers,
                fetch_queue_size=config.fetch_queue_size,
                process_queue_size=config.process_queue_size,
                store_queue_size=config.store_queue_size,
            )
        else:
            etl_config = ETLPipelineConfig(
                pipeline_name=config.pipeline_name,
                fetch_workers=config.fetch_workers,
                process_workers=config.process_workers,
                upload_workers=config.upload_workers,
                fetch_queue_size=config.fetch_queue_size,
                process_queue_size=config.process_queue_size,
                store_queue_size=config.store_queue_size,
            )

        # Conditionally patch ThreadPoolExecutor only if process_workers are disabled (set to 0)
        if etl_config.process_workers == 0:
            mock_executor = mocker.MagicMock()
            mock_executor.submit = mocker.MagicMock()
            mock_executor.shutdown = mocker.MagicMock()

            # Patch ThreadPoolExecutor during parent initialization
            mocker.patch("bergtalzug.etl_pipeline.ThreadPoolExecutor", return_value=mock_executor)
            super().__init__(config=etl_config)

            self.executor = mock_executor
        else:
            super().__init__(config=etl_config)

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

    async def setup_queues(self) -> None:
        """Setup queues, overridden to avoid calling protected method."""
        await self._setup_queues()
        self.fetch_queue = self._fetch_queue
        self.process_queue = self._process_queue
        self.store_queue = self._store_queue

    async def fetch_worker(self) -> None:
        """Run a single fetch worker, overridden to avoid calling protected method."""
        await self._fetch_worker()

    def process_worker(self) -> None:
        """Run a single process worker, overridden to avoid calling protected method."""
        self._process_worker()

    async def store_worker(self) -> None:
        """Run a single store worker, overridden to avoid calling protected method."""
        await self._store_worker()

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
        return item

    def _process_impl(self, item: WorkItem) -> WorkItem:
        if self.config.process_sleep is not None and self.config.process_sleep > 0:
            time.sleep(self.config.process_sleep)
        item.data = b"processed_" + item.data
        return item

    async def _store_impl(self, item: WorkItem) -> None:
        if self.config.store_sleep is not None and self.config.store_sleep > 0:
            await asyncio.sleep(self.config.store_sleep)


# This is needed to disable the abstract methods in the base class
# since we are mocking them
MockETLPipeline.__abstractmethods__ = frozenset()


# NOTE: Currently this is not a fixture, but a factory function,
# The reason is that currently there are no tests sharing the same mock ETL pipeline.
# This means the mocker fixture must be passed explicitly in tests.
def mock_etl_pipeline_factory(mocker: MockerFixture, config: MockETLPipelineConfig | None = None) -> MockETLPipeline:
    """Create a mock ETL pipeline implementation for testing."""
    return MockETLPipeline(mocker, config)
