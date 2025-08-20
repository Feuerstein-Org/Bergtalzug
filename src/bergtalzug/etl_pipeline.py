"""
ETL Pipeline Implementation

This module implements an ETL (Extract, Transform, Load) pipeline using asyncio, multthreading and culsans for concurrency.
It defines an abstract base class `ETLPipeline` and a test implementation
"""

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from abc import ABC, abstractmethod
from typing import Any
import culsans


@dataclass
class WorkItem:
    """
    Represents a work item in the ETL pipeline.

    It contains the data to be processed, metadata about the item, and a unique job ID.
    Objects of this class will be passed through all the stages of the ETL pipeline.
    """

    data: bytes
    # TODO: At each stage add metadata to the item, e.g. fetched, processed, stored
    metadata: dict[Any, Any]  # specify with pydantic later
    job_id: str


class ETLPipeline(ABC):
    """
    Abstract base class for an ETL pipeline.

    Data will be passed through the stages of the pipeline as `WorkItem` objects.
    All steps will be executed concurrently and asynchronously. Just override the abstract methods.

    The specific methods are:
    refill_queue: which is called periodically to add items to the queue if it falls below a threshold.
    fetch: which is called to fetch data.
    process: which is called to process the fetched data.
    store: which is called to store the processed data.
    """

    def __init__(  # noqa: PLR0913
        self,  # TODO: potentially add a config class later
        pipeline_name: str = "etl_pipeline",
        fetch_workers: int = 10,
        process_workers: int = 5,
        upload_workers: int = 10,
        fetch_queue_size: int = 1000,
        process_queue_size: int = 500,
        store_queue_size: int = 1000,
    ) -> None:
        """
        Initialize the ETL pipeline with the given parameters.

        Args:
            pipeline_name (str): Name of the pipeline for logging.
            fetch_workers (int): Number of concurrent fetch workers.
            process_workers (int): Number of processing workers.
            upload_workers (int): Number of upload workers.
            fetch_queue_size (int): Size of the fetch queue.
            process_queue_size (int): Size of the process queue.
            store_queue_size (int): Size of the store queue.

        """
        self.logger = logging.getLogger(f"etl.{pipeline_name}")
        self.pipeline_name = pipeline_name
        self._fetch_workers = fetch_workers
        self._process_workers = process_workers
        self._store_workers = upload_workers
        self._fetch_queue_size = fetch_queue_size
        self._process_queue_size = process_queue_size
        self._store_queue_size = store_queue_size

        # Thread pool for processing
        self.executor = ThreadPoolExecutor(max_workers=process_workers)

    async def _setup_queues(self) -> None:
        """
        Initialize queues.

        This method sets up the queues used in the ETL pipeline.
        """
        # Job queue for initial distribution
        self._fetch_queue = asyncio.Queue[WorkItem | None](maxsize=self._fetch_queue_size)

        # culsans queues provide both async and sync interfaces
        self._process_queue = culsans.Queue[WorkItem | None](maxsize=self._process_queue_size)
        self._store_queue = culsans.Queue[WorkItem | None](maxsize=self._store_queue_size)

    @abstractmethod
    async def refill_queue(self, count: int) -> list[WorkItem]:
        """
        Abstract method that gets called periodically to add items to the queue if falls below a threshold.

        A list of `count` WorkItems should be returned which will be passed to the fetch function one by one.
        e.g. if count is 10, then 10 items should be returned.
        If an Empty list is returned this signifies that the ETL job is finished.
        """
        pass

    @abstractmethod
    async def fetch(self, item: WorkItem) -> WorkItem:
        """
        Abstract method that gets called when an item from the queue needs to be fetched.

        After the item is fetched it should be simply returned as a WorkItem.
        """
        pass

    # TODO: Add a way to switch between ThreadPoolExecutor and ProcessPoolExecutor
    @abstractmethod
    def process(self, item: WorkItem) -> WorkItem:
        """
        Abstract method that gets called when an item was passed from the fetch method.

        Note that this method is synchronous and runs in a thread pool.
        Any code that doesn't release the GIL should be avoided.
        Once processing is done the item should be returned as a WorkItem.
        """
        pass

    @abstractmethod
    async def store(self, item: WorkItem) -> None:
        """
        Abstract method that gets called when an item was processed.

        This method should handle the storage of the processed data.
        It is expected to be asynchronous, e.g., uploading to S3.
        """
        pass

    async def _periodic_queue_manager(self) -> None:
        """Periodically check queue size and add items if below 50% capacity."""
        while True:
            current_size = self._fetch_queue.qsize()
            threshold = self._fetch_queue_size / 2  # 50% threshold

            if current_size < threshold:
                self.logger.debug(
                    "Fetch queue size below threshold (%s/%s), adding items", current_size, self._fetch_queue_size
                )
                target_size = int(self._fetch_queue_size * 0.9)
                available_space = target_size - current_size
                # Request 90% of available space as safety buffer, later might need to add buffer due to overflow risk
                count = max(1, available_space)  # even if available space is 0, we want to refill at least one item
                try:
                    items = await self.refill_queue(count)

                    if not items:
                        self.logger.info("No more items available, initiating shutdown")
                        break

                    # Add all items to the fetch queue
                    for item in items:
                        await self._fetch_queue.put(item)

                    self.logger.debug("Added %s items to fetch queue", len(items))

                except Exception as e:
                    self.logger.error("Error in refill_queue: %s", e)
                    # Continue running - don't break the pipeline for this error
            # Check every second
            await asyncio.sleep(1.0)  # TODO: let this value be configured instead of hardcoding

    async def _fetch_worker(self) -> None:
        """
        Async fetching worker.

        Gets fetch jobs from the fetch queue and gets data asynchronously.
        """
        while True:
            work_item = await self._fetch_queue.get()
            if work_item is None:  # Poison pill
                self.logger.info("Fetch worker received poison pill, shutting down")
                break

            try:
                self.logger.debug(
                    "Fetched fetch job from queue, calling abstract fetch function for job %s", work_item.job_id
                )
                fetched_item = await self.fetch(work_item)

                await self._process_queue.async_q.put(fetched_item)
            except Exception as e:
                self.logger.error(
                    "Fetch error: %s", e
                )  # TODO: Add retry logic or error handling or dead-letter queue + log
            finally:
                # For now still need to mark as done even on failure to prevent deadlock
                # This WILL loose items in case of error!!
                self._fetch_queue.task_done()

    def _process_worker(self) -> None:
        """
        Sync processing worker.

        Gets process jobs from the process queue and processes them.
        """
        while True:
            work_item = self._process_queue.sync_q.get()

            if work_item is None:  # Poison pill
                self.logger.info("Process worker received poison pill, shutting down")
                break

            try:
                self.logger.debug(
                    "Fetched process job from queue, calling abstract process function for job %s", work_item.job_id
                )
                processed_item = self.process(work_item)

                self._store_queue.sync_q.put(processed_item)
            except Exception as e:
                self.logger.error(
                    "Processing error: %s", e
                )  # TODO: Add retry logic or error handling or dead-letter queue + log
            finally:
                # For now still need to mark as done even on failure to prevent deadlock
                # This WILL loose items in case of error!!
                self._process_queue.sync_q.task_done()

    async def _store_worker(self) -> None:
        """
        Async storing worker.

        Gets store jobs from the store queue and stores data asynchronously.
        """
        while True:
            work_item = await self._store_queue.async_q.get()

            if work_item is None:  # Poison pill
                self.logger.info("Store worker received poison pill, shutting down")
                break

            try:
                self.logger.debug(
                    "Fetched store job from queue, calling abstract store function for job %s", work_item.job_id
                )
                await self.store(work_item)
            except Exception as e:
                self.logger.error(
                    "Storing error: %s", e
                )  # TODO: Add retry logic or error handling or dead-letter queue + log
            finally:
                # For now still need to mark as done even on failure to prevent deadlock
                # This WILL loose items in case of error!!
                self._store_queue.async_q.task_done()

    # NOTE: Potentially allow to pass initial jobs to the pipeline
    # Also might be worth adding a setup abstract method to initialize the pipeline
    async def run(self) -> None:
        """
        Start the ETL pipeline.

        This method sets up the queues, starts the workers, and manages the lifecycle of the ETL process.
        """
        self.logger.info("Initializing ETL pipeline: %s", self.pipeline_name)

        # Setup queues
        await self._setup_queues()
        # Start periodic queue manager
        queue_manager_task = asyncio.create_task(self._periodic_queue_manager())

        # Start download workers (async)
        fetch_tasks: list[asyncio.Task[None]] = []
        for _ in range(self._fetch_workers):
            task = asyncio.create_task(self._fetch_worker())
            fetch_tasks.append(task)

        # Start processing workers (in thread pool)
        loop = asyncio.get_event_loop()
        process_futures: list[asyncio.Future[None]] = []
        for _ in range(self._process_workers):
            future = loop.run_in_executor(self.executor, self._process_worker)
            process_futures.append(future)

        # Start upload workers (async)
        store_tasks: list[asyncio.Task[None]] = []
        for _ in range(self._store_workers):
            task = asyncio.create_task(self._store_worker())
            store_tasks.append(task)

        self.logger.info(
            "ETL pipeline %s started | fetch_workers: %s (queue: %s) "
            "| process_workers: %s (queue: %s) | store_workers: %s (queue: %s)",
            self.pipeline_name,
            self._fetch_workers,
            self._fetch_queue_size,
            self._process_workers,
            self._process_queue_size,
            self._store_workers,
            self._store_queue_size,
        )

        # Wait for the queue manager to signal completion
        await queue_manager_task

        self.logger.info("Waiting for all jobs to complete a")

        # Sequential shutdown with proper synchronization
        await self._fetch_queue.join()
        self.logger.info("All fetch jobs completed, shutting down fetch workers")
        for _ in range(self._fetch_workers):
            await self._fetch_queue.put(None)

        await self._process_queue.async_q.join()
        self.logger.info("All process jobs completed, shutting down process workers")
        for _ in range(self._process_workers):
            await self._process_queue.async_q.put(None)

        await self._store_queue.async_q.join()
        self.logger.info("All store jobs completed, shutting down store workers")
        for _ in range(self._store_workers):
            await self._store_queue.async_q.put(None)

        # Wait for workers to finish
        await asyncio.gather(*fetch_tasks)
        await asyncio.gather(*process_futures)
        await asyncio.gather(*store_tasks)

        # Close queues
        self._process_queue.close()
        self._store_queue.close()
        await self._process_queue.wait_closed()
        await self._store_queue.wait_closed()

        self.logger.info("ETL pipeline %s completed", self.pipeline_name)

        # Shutdown executor
        self.executor.shutdown(wait=True)
