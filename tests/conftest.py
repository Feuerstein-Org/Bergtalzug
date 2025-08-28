"""Test fixtures for ETL Pipeline tests."""

from dataclasses import dataclass
import pytest
from typing import Any
from bergtalzug import ETLPipeline, ETLPipelineConfig, WorkItem
from pytest_mock import MockerFixture
import asyncio
import time


def work_item_factory(count: int = 1, data: bytes = b"test_data") -> list[WorkItem]:
    """Create a list of WorkItem instances with custom data."""
    work_items: list[WorkItem] = []
    for _ in range(count):
        work_items.append(WorkItem(data=data))
    return work_items


@dataclass
class MockETLPipelineConfig:
    """
    Configuration for the MockETLPipeline class.

    No validation is performed here. This class is used to have lower testing defaults
    and is passed to the ETLPipelineConfig class which performs validation.
    """

    pipeline_name: str = "mock_etl_pipeline"
    fetch_workers: int = 1
    process_workers: int = 1
    store_workers: int = 1
    fetch_queue_size: int = 10
    process_queue_size: int = 5
    store_queue_size: int = 10
    queue_refresh_rate: float = 0.01  # seconds
    enable_tracking: bool = True
    stats_interval_seconds: float = 1.0
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

    Each worker pool can have the size set to 0 to disable it for testing.
    """

    def __init__(self, mocker: MockerFixture, config: MockETLPipelineConfig | None = None) -> None:
        """Initialize the mock ETL pipeline with configuration."""
        # Use default config if none provided
        if config is None:
            config = MockETLPipelineConfig()

        # Pass ETL pipeline parameters to parent constructor
        etl_config = ETLPipelineConfig(
            pipeline_name=config.pipeline_name,
            fetch_workers=config.fetch_workers,
            process_workers=config.process_workers,
            store_workers=config.store_workers,
            fetch_queue_size=config.fetch_queue_size,
            process_queue_size=config.process_queue_size,
            store_queue_size=config.store_queue_size,
            queue_refresh_rate=config.queue_refresh_rate,
            enable_tracking=config.enable_tracking,
            stats_interval_seconds=config.stats_interval_seconds,
        )

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


class MockETLPipelineFactory:
    """Collection of factory methods to create MockETLPipeline instances."""

    def __init__(self, mocker: MockerFixture) -> None:
        """Initialize the mocker."""
        self._mocker = mocker

    def create(self, config: MockETLPipelineConfig | None = None) -> MockETLPipeline:
        """
        Create a MockETLPipeline with the given configuration.

        Args:
            config: Optional pipeline configuration. Uses defaults if None.

        Returns:
            MockETLPipeline: A configured mock pipeline instance.

        Example:
            ```python
            pipeline = builder.create(MockETLPipelineConfig(items_to_process=items))
            ```

        """
        return MockETLPipeline(self._mocker, config)

    def create_items(self, count: int, **kwargs: Any) -> MockETLPipeline:
        """
        Create a pipeline with a number of items to process.

        Args:
            count: Number of WorkItem instances to process.
            **kwargs: Additional configuration parameters.

        Returns:
            MockETLPipeline: A configured pipeline with the given items.

        Example:
            ```python
            pipeline = builder.create_items(count=5)
            ```

        """
        items = work_item_factory(count=count)
        config = MockETLPipelineConfig(items_to_process=items, **kwargs)
        return self.create(config)

    def pass_items(self, items: list[WorkItem], **kwargs: Any) -> MockETLPipeline:
        """
        Create a pipeline with the passed item instances to process.

        Args:
            items: List of WorkItem instances to process.
            **kwargs: Additional configuration parameters.

        Returns:
            MockETLPipeline: A configured pipeline with the given items.

        Example:
            ```python
            pipeline = builder.pass_items(work_item_factory(count=5))
            ```

        """
        config = MockETLPipelineConfig(items_to_process=items, **kwargs)
        return self.create(config)


@pytest.fixture
def mock_etl_pipeline_factory(mocker: MockerFixture) -> MockETLPipelineFactory:
    """Fixture providing a MockETLPipelineFactory for creating test pipelines"""
    return MockETLPipelineFactory(mocker)
