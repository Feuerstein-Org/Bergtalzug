"""Test fixtures for ETL Pipeline tests."""

import pytest
from bergtalzug import ETLPipeline, WorkItem
from pytest_mock import MockerFixture
from typing import Any
from uuid import uuid4
from functools import partial

WorkItemFactory = partial[list[WorkItem]]


@pytest.fixture
def work_item() -> partial[list[WorkItem]]:
    """
    Create a list of WorkItem instances with custom data, by default one item.

    count determines how many items to create
    job_id is only applied to the first item, others get a random value
    data and metadata will be the same across all items created
    """

    def _create_work_item(count: int, data: str | bytes, metadata: Any, job_id: str) -> list[WorkItem]:
        work_items: list[WorkItem] = []
        for _ in range(count):
            if metadata is None:
                metadata = {"source": "test"}
            if isinstance(data, str):
                data = data.encode()
            work_items.append(WorkItem(data=data, metadata=metadata, job_id=job_id))
            job_id = str(uuid4().hex[:6])  # TODO: Later this will be handled in real class
        return work_items

    return partial(_create_work_item, count=1, data=b"test_data", metadata=None, job_id=str(uuid4().hex[:6]))


class MockETLPipeline(ETLPipeline):
    """
    Mock implementation of ETLPipeline for testing purposes.

    This class overrides the abstract methods to provide mock behavior.
    you can call mock methods on mock_refill_queue, mock_fetch, mock_process, mock_store
    """

    # TODO: Needs to be changed once pydantic config is in place
    # also allow to pass in various parameters to the constructor
    # e.g. how long to sleep on each stage, etc.
    def __init__(self, mocker: MockerFixture, *args: Any, **kwargs: Any) -> None:
        """Initialize the mock ETL pipeline."""
        super().__init__(*args, **kwargs)
        # Should be replaced soon once we allow WorkItems to be set on startup
        self.items_to_process: list[WorkItem] = []

        self.mock_refill_queue = mocker.AsyncMock(side_effect=self._refill_queue_impl)
        self.mock_fetch = mocker.AsyncMock(side_effect=self._fetch_impl)
        self.mock_process = mocker.Mock(side_effect=self._process_impl)
        self.mock_store = mocker.AsyncMock(side_effect=self._store_impl)

        # Replacing the actual methods with mocks
        self.refill_queue = self.mock_refill_queue
        self.fetch = self.mock_fetch
        self.process = self.mock_process
        self.store = self.mock_store

    async def _refill_queue_impl(self, count: int) -> list[WorkItem]:
        if self.items_to_process:
            batch = self.items_to_process[:count]
            self.items_to_process = self.items_to_process[count:]
            return batch
        return []

    async def _fetch_impl(self, item: WorkItem) -> WorkItem:
        item.metadata["fetched"] = True
        return item

    def _process_impl(self, item: WorkItem) -> WorkItem:
        item.data = b"processed_" + item.data
        item.metadata["processed"] = True
        return item

    async def _store_impl(self, item: WorkItem) -> None:
        item.metadata["stored"] = True


# This is needed to disable the abstract methods in the base class
# since we are mocking them
MockETLPipeline.__abstractmethods__ = frozenset()
MockETLPipelineFactory = partial[MockETLPipeline]


@pytest.fixture
def mock_etl_pipeline(mocker: MockerFixture) -> MockETLPipelineFactory:
    """Create a mock ETL pipeline implementation for testing."""
    return partial(MockETLPipeline, mocker)
