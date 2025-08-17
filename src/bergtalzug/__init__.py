"""
Package provides a library containing a base class for ETL processes.

The ETL base class is designed to be extended and customized for specific data pipeline implementations.
Users can inherit from this base class to implement their own extract, transform, and load logic.

The specific methods are:
add_items_to_queue which is called periodically to add items to the queue if it falls below a threshold.
fetch which is called to fetch data.
process which is called to process the fetched data.
store which is called to store the processed data.

Example usage:
    from etl_pipeline import ETLPipeline, WorkItem

    class MyETL(ETLPipeline):
        async def add_items_to_queue(self, count: int) -> list[WorkItem]:
            # Your implementation here
            pass
"""

from bergtalzug.etl_pipeline import ETLPipeline, WorkItem

__all__ = ["ETLPipeline", "WorkItem"]
