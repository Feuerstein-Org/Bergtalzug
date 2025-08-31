"""Tests whether @property decorators are working as expected."""

import pytest
from conftest import MockETLPipelineFactory


@pytest.mark.asyncio
async def test_fetch_workers_property(mock_etl_pipeline_factory: MockETLPipelineFactory) -> None:
    """Test basic end-to-end flow"""
    pipeline = mock_etl_pipeline_factory.create(fetch_workers=11)
    assert pipeline.fetch_workers == 11


@pytest.mark.asyncio
async def test_process_workers_property(mock_etl_pipeline_factory: MockETLPipelineFactory) -> None:
    """Test basic end-to-end flow"""
    pipeline = mock_etl_pipeline_factory.create(process_workers=11)
    assert pipeline.process_workers == 11


@pytest.mark.asyncio
async def test_store_workers_property(mock_etl_pipeline_factory: MockETLPipelineFactory) -> None:
    """Test basic end-to-end flow"""
    pipeline = mock_etl_pipeline_factory.create(store_workers=11)
    assert pipeline.store_workers == 11


@pytest.mark.asyncio
async def test_fetch_queue_size_property(mock_etl_pipeline_factory: MockETLPipelineFactory) -> None:
    """Test basic end-to-end flow"""
    pipeline = mock_etl_pipeline_factory.create(fetch_queue_size=11)
    assert pipeline.fetch_queue_size == 11


@pytest.mark.asyncio
async def test_process_queue_size_property(mock_etl_pipeline_factory: MockETLPipelineFactory) -> None:
    """Test basic end-to-end flow"""
    pipeline = mock_etl_pipeline_factory.create(process_queue_size=11)
    assert pipeline.process_queue_size == 11


@pytest.mark.asyncio
async def test_store_queue_size_property(mock_etl_pipeline_factory: MockETLPipelineFactory) -> None:
    """Test basic end-to-end flow"""
    pipeline = mock_etl_pipeline_factory.create(store_queue_size=11)
    assert pipeline.store_queue_size == 11


@pytest.mark.asyncio
async def test_process_is_sync_property(mock_etl_pipeline_factory: MockETLPipelineFactory) -> None:
    """Test basic end-to-end flow"""
    pipeline = mock_etl_pipeline_factory.create_sync()
    assert pipeline.process_is_sync == True

    pipeline_async = mock_etl_pipeline_factory.create()
    assert pipeline_async.process_is_sync == False


@pytest.mark.asyncio
async def test_queue_refresh_rate_property(mock_etl_pipeline_factory: MockETLPipelineFactory) -> None:
    """Test basic end-to-end flow"""
    pipeline = mock_etl_pipeline_factory.create(queue_refresh_rate=11)
    assert pipeline.queue_refresh_rate == 11


@pytest.mark.asyncio
async def test_all_properties_with_custom_config(mock_etl_pipeline_factory: MockETLPipelineFactory) -> None:
    """Test all properties together with custom configuration"""
    pipeline = mock_etl_pipeline_factory.create(
        fetch_workers=2,
        process_workers=3,
        store_workers=4,
        fetch_queue_size=50,
        process_queue_size=75,
        store_queue_size=100,
        queue_refresh_rate=1.5,
    )

    # Verify all properties return expected values
    assert pipeline.fetch_workers == 2
    assert pipeline.process_workers == 3
    assert pipeline.store_workers == 4
    assert pipeline.fetch_queue_size == 50
    assert pipeline.process_queue_size == 75
    assert pipeline.store_queue_size == 100
    assert pipeline.queue_refresh_rate == 1.5
    assert pipeline.process_is_sync is False
