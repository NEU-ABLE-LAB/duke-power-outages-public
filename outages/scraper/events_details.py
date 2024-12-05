"""
Get all event data for a list of events using asynchronous requests with per-proxy rate limiting and backoff.
"""
from typing import List, Optional
from pathlib import Path
from datetime import datetime
import asyncio

import typer
from loguru import logger
from tqdm.asyncio import tqdm

import httpx

import pandas as pd

from outages.config import RAW_EVENTS_DIR
from outages.utils_geo import BoundingBox
from outages.scraper.utils_http import (
    NoContentError,
    PriorityLevels,
    TaskManager,
)
from outages.scraper.dashboard import DashboardApp
from outages.scraper.event_details import (
    get_event_full_info,
)

# Constants and configuration
TEST_SOURCE_EVENT_NUMBERS = [
    '13278236', '13322591', '13275187', '13293544', '13282087',
    '13264060', '13261563', '13323897', '13263517', '5912711'
]

app = typer.Typer()

REQUESTS_TIMEOUT = 10
REQUESTS_BATCH_SIZE = 500

async def fetch_event(
    source_event_number: str,
    manager: TaskManager,
    region: BoundingBox = None,
    dashboard: DashboardApp = None,
) -> pd.DataFrame:
    """
    Fetch a single event.

    Args:
        source_event_number (str): The event number to fetch.
        manager (TaskManager): The TaskManager instance to use for managing tasks.
        region (BoundingBox): The region to fetch the event from.
        dashboard (DashboardApp): The DashboardApp instance to use for displaying progress.

    Returns:
        pd.DataFrame: The event data as a DataFrame, or an empty DataFrame on failure.
    """

    request_timestamp = datetime.now()

    try:
        df_event, response_status = await manager.submit_task(
            priority=PriorityLevels.EVENT,
            fetch_func=get_event_full_info,
            # `client` passed by ProxyWorker.fetch()
            output_path=False,
            source_event_number=source_event_number,
            jurisdiction=region.jurisdiction,
        )
        
    except NoContentError as e:
        logger.error(
            f"Received 204 No Content for event {source_event_number}. Retries exhausted: {e}"
            f"Retries exhausted: {e}"
        )
        df_event = pd.DataFrame()
        response_status = 204

    except (httpx.RequestError, httpx.HTTPStatusError) as e:
        logger.error(
            f"Error fetching event {source_event_number}. Retries exhausted: {e}"
        )
        df_event = pd.DataFrame()
        response_status = e.response.status_code if isinstance(e, httpx.HTTPStatusError) else 'httpx.RequestError'
        
    except Exception as e:
        logger.error(
            f"Unexpected error fetching event {source_event_number}. Retries exhausted: {e}"
        )
        df_event = pd.DataFrame()
        response_status = 'UnexpectedError'
        
    response_timestamp = datetime.now()

    if not(isinstance(df_event, pd.DataFrame) and not df_event.empty):
        df_event = pd.DataFrame()
        df_event['sourceEventNumber'] = source_event_number

    df_event['request_timestamp'] = request_timestamp
    df_event['response_timestamp'] = response_timestamp
    df_event['response_status'] = response_status
    df_event['jurisdiction'] = region.jurisdiction

    dashboard.pbar_update(region.dashboard_index*2 + 1)

    return df_event

async def get_events_full_info(
    manager: TaskManager,
    output_path: Optional[Path] = RAW_EVENTS_DIR,
    source_event_numbers: Optional[List[str]] = None,
    region: BoundingBox = None,
    dashboard: DashboardApp = None,
) -> pd.DataFrame:
    """
    Scrape the website for outage information for a list of events.

    Args:
        manager (TaskManager): The TaskManager instance to use for managing tasks.
        output_path (Optional[Path]): The path to save the output data. Defaults to RAW_EVENTS_DIR.
        source_event_numbers (Optional[List[str]]): The list of event numbers to fetch. Defaults to test data.
        region (BoundingBox): The region to fetch the event from.
        dashboard (DashboardApp): The DashboardApp instance to use for displaying progress.

    Returns:
        pd.DataFrame: The DataFrame containing event data.
    """
    if output_path is not False:
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        output_file = output_path / f"events-{timestamp}.parquet"

        # Create the necessary directory(ies) if needed
        output_file.parent.mkdir(parents=True, exist_ok=True)

    if source_event_numbers is None:
        source_event_numbers = TEST_SOURCE_EVENT_NUMBERS

    logger.info(f"Get details for {len(source_event_numbers)} events.")

    tasks = [
        fetch_event(
            source_event_number=source_event_number, 
            manager=manager,
            region=region,
            dashboard=dashboard,
        )
        for source_event_number in source_event_numbers
    ]

    # Execute tasks concurrently
    if dashboard is None:
        # Use tqdm to show progress bar
        results = await tqdm.gather(*tasks)
    else:
        # Use dashboard to show progress
        dashboard.pbar_total(region.dashboard_index*2+1, len(source_event_numbers))
        results = await asyncio.gather(*tasks)


    # Process results, handling exceptions or empty DataFrames
    df_events_list = []
    for result in results:
        if isinstance(result, pd.DataFrame) and not result.empty:
            df_events_list.append(result)
        elif isinstance(result, Exception):
            logger.error(f"Error fetching event: {result}")

    df_events = pd.concat(df_events_list, ignore_index=True) if df_events_list else pd.DataFrame()

    # Save the response to a file
    if output_path is not False and not df_events.empty:
        df_events.to_parquet(output_file, engine='pyarrow')
        logger.info(f"Data saved to {output_file}")
    else:
        logger.info("No data to save.")

    return df_events

# async def main_async():

#     async with create_task_manager(dashboard) as manager:
#         await get_events_full_info(
#             manager=manager,
#             output_path=RAW_EVENTS_DIR,
#             source_event_numbers=TEST_SOURCE_EVENT_NUMBERS,
#         )


# @app.command()
# def main():
#     """
#     #TODO pass in command line arguments
#     """

#     asyncio.run(main_async())

# if __name__ == "__main__":
#     app()
