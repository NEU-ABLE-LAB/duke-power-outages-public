"""
Sweep a tiles across a lat-long grid of region to get outage information using 
synchronous requests with per-proxy rate limiting and backoff.
"""

from pathlib import Path
from datetime import datetime
from typing import Tuple
import threading
import asyncio
import hashlib

import typer
from loguru import logger
from tqdm.asyncio import tqdm

import httpx

import pandas as pd

from outages.config import (
    RAW_REGIONS_DIR,
)
from outages.utils_geo import (
    BoundingBox,
    DUKE_CAROLINAS_REGION,
    DUKE_FLORIDA_REGION,
)
from outages.scraper.utils_http import (
    NoContentError,
    PriorityLevels,
    TaskManager,
    create_task_manager,
)
from outages.scraper.dashboard import DashboardApp
from outages.scraper.tile import get_events_from_tile
from outages.scraper.events_details import get_events_full_info

REQUESTS_TIMEOUT = 10

app = typer.Typer()

async def fetch_tile(
    tile: BoundingBox,
    manager: TaskManager,
    dashboard: DashboardApp = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Fetch events from a tile and return status information.

    Args:
        tile (BoundingBox): The bounding box for the tile.
        manager (TaskManager): The TaskManager instance to use for managing tasks.
        dashboard (DashboardApp): The DashboardApp instance to use for displaying progress.

    Returns:
        Tuple[pd.DataFrame, Optional[int], Optional[int]]: 
            - The events data as a DataFrame
            - The tile status as a DataFrame
    """
    
    request_timestamp = datetime.now()
    hash_input = f"{tile.name}-{request_timestamp}"
    run_id = hashlib.md5(hash_input.encode()).hexdigest()

    try:
        df_region_events, response_status = await manager.submit_task(
            priority=PriorityLevels.COUNTY,
            fetch_func=get_events_from_tile,
            # client passed by ProxyWorker.fetch()
            tile=tile,
            output_path=False,
            run_id=run_id,
        )
        
        df_region_events = df_region_events if df_region_events is not None else pd.DataFrame()
        num_events = len(df_region_events) if df_region_events is not None else 0

    except NoContentError as e:
        logger.error(
            f"Received 204 No Content for tile {tile.name}. Retries exhausted: {e}"
        )
        df_region_events = pd.DataFrame()
        num_events = None
        response_status = 204
        
    except (httpx.RequestError, httpx.HTTPStatusError) as e:
        logger.error(
            f"Error fetching tile {tile.name}. Retries exhausted: {e}"
        )
        df_region_events = pd.DataFrame()
        num_events = None
        response_status = e.response.status_code if isinstance(e, httpx.HTTPStatusError) else 'httpx.RequestError'
        
    except Exception as e:
        #TODO raise this exception or catch it better
        logger.error(
            f"Unexpected error fetching tile {tile.name}: {e}"
        )
        df_region_events = pd.DataFrame()
        num_events = None
        response_status = 'UnexpectedError'
        
    response_timestamp = datetime.now()
    df_status = pd.DataFrame({
        'run_id': [run_id],
        'tile': [tile.name],
        'tile_sw_lat': [tile.sw_lat],
        'tile_sw_long': [tile.sw_long],
        'tile_ne_lat': [tile.ne_lat],
        'tile_ne_long': [tile.ne_long],
        'jurisdiction': [tile.jurisdiction],
        'status': [response_status],
        'num_events': [num_events],
        'request_timestamp': [request_timestamp],
        'response_timestamp': [response_timestamp],
    })
    
    dashboard.pbar_update(tile.dashboard_index*2)

    return (
        df_region_events,
        df_status,
    )

async def sweep_all(
    manager: TaskManager,
    output_path: Path = RAW_REGIONS_DIR,
    region: BoundingBox = DUKE_CAROLINAS_REGION,
    step_lat: float = 0.5,
    step_long: float = 0.5,
    dashboard: DashboardApp = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Sweep a tile across a lat-long grid to get outage information and track tile status.

    Args:
        manager (TaskManager): The TaskManager instance to use for managing tasks.
        output_path (Path): The path to save the output data. Defaults to RAW_REGIONS_DIR.
        region (BoundingBox): The bounding box for the region. Defaults to Duke Energy NC/SC.
        step_lat (float): Latitude step size. Defaults to 0.5.
        step_long (float): Longitude step size. Defaults to 0.5.
        dashboard (DashboardApp): The DashboardApp instance to use for displaying progress.

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: The concatenated DataFrame of all events,
                                           and a DataFrame with status information.
    """
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    region_name = f"{region.name}_{step_lat}_{step_long}"
    events_file = output_path / region_name / f"region-{timestamp}-data.parquet"
    status_file = output_path / region_name / f"region-{timestamp}-status.parquet"

    # Create the necessary directory(ies) if needed
    events_file.parent.mkdir(parents=True, exist_ok=True)

    lat_steps = int((float(region.ne_lat) - float(region.sw_lat)) / step_lat) + 1
    long_steps = int((float(region.ne_long) - float(region.sw_long)) / step_long) + 1
    total_tiles = lat_steps * long_steps

    logger.info(
        f"Grid has {lat_steps} latitude cells and {long_steps} longitude cells, totaling {total_tiles} tiles."
    )

    # Generate all tiles to process
    tiles = [
        BoundingBox(
            sw_lat=float(region.sw_lat) + i * step_lat,
            sw_long=float(region.sw_long) + j * step_long,
            ne_lat=float(region.sw_lat) + (i + 1) * step_lat,
            ne_long=float(region.sw_long) + (j + 1) * step_long,
            jurisdiction=region.jurisdiction,
            dashboard_index=region.dashboard_index
        )
        for i in range(lat_steps) for j in range(long_steps)
    ]

    # Create tasks for each tile
    tasks = [
        fetch_tile(
            tile=tile,
            manager=manager,
            dashboard=dashboard,
        )
        for tile in tiles
    ]

    # Execute tasks concurrently
    #TODO if one or more tasks raise exceptions, gather will cancel all the other tasks by default unless return_exceptions=True is set.
    if dashboard is None:
        # Use tqdm to show progress bar
        results = await tqdm.gather(*tasks)
    else:

        # Use dashboard to show progress
        dashboard.pbar_total(region.dashboard_index*2, total_tiles)
        results = await asyncio.gather(*tasks)

    # Separate events and status info
    df_events_list = [result[0] for result in results if not result[0].empty]
    df_status_list = [result[1] for result in results if not result[1].empty]

    # Combine event data and status information
    df_events = pd.concat(df_events_list, ignore_index=True) if df_events_list else pd.DataFrame()
    df_status = pd.concat(df_status_list, ignore_index=True) if df_status_list else pd.DataFrame()

    # Save event data
    if not df_events.empty:
        df_events.to_parquet(events_file, engine='pyarrow')
        logger.info(f"Event data saved to {events_file}")
    else:
        logger.info("No event data to save.")

    # Save status data
    if not df_status.empty:
        df_status.to_parquet(status_file, engine='pyarrow')
        logger.info(f"Status data saved to {status_file}")
    else:
        logger.info("No status data to save.")
               
    # Get full event information
    # jurisdiction = region.jurisdiction
    # if not df_events.empty:
    #     event_numbers = df_events['sourceEventNumber'].tolist()
    #     await get_events_full_info(
    #         manager=manager,
    #         source_event_numbers=event_numbers,
    #         output_path= RAW_EVENTS_DIR / region_name,
    #         jurisdiction=jurisdiction,
    #         dashboard=dashboard,
    #     )

    return df_events, df_status



async def main_async(
    dashboard: DashboardApp = None,
    output_path: str = RAW_REGIONS_DIR,
    # region_sw_lat: str = DUKE_CAROLINAS_REGION.sw_lat,
    # region_sw_long: str = DUKE_CAROLINAS_REGION.sw_long,
    # region_ne_lat: str = DUKE_CAROLINAS_REGION.ne_lat,
    # region_ne_long: str = DUKE_CAROLINAS_REGION.ne_long,
    # region_name: str = DUKE_CAROLINAS_REGION.name,
    region_sw_lat: str = DUKE_FLORIDA_REGION.sw_lat,
    region_sw_long: str = DUKE_FLORIDA_REGION.sw_long,
    region_ne_lat: str = DUKE_FLORIDA_REGION.ne_lat,
    region_ne_long: str = DUKE_FLORIDA_REGION.ne_long,
    region_name: str = DUKE_FLORIDA_REGION.name,
    step_lat: str = 0.5,
    step_long: str = 0.5,
) -> None:
    """
    Main function to sweep all tiles and fetch outage information.

    Args:
        output_path (str): The path to save the output data.
        region_sw_lat (str): The southwest latitude of the region.
        region_sw_long (str): The southwest longitude of the region.
        region_ne_lat (str): The northeast latitude of the region.
        region_ne_long (str): The northeast longitude of the region.
        region_name (str): The name of the region.
        step_lat (str): Latitude step size.
        step_long (str): Longitude step size.
    """

    region = BoundingBox(
        sw_lat=float(region_sw_lat),
        sw_long=float(region_sw_long),
        ne_lat=float(region_ne_lat),
        ne_long=float(region_ne_long),
        name=region_name,
    )

    async with create_task_manager(dashboard) as manager:
        await sweep_all(
            manager=manager,
            output_path=Path(output_path),
            region=region,
            step_lat=float(step_lat),
            step_long=float(step_long),
            dashboard=dashboard,
        )

def main_in_background(dashboard: DashboardApp):
    """
    Run the main_async function in the background
    """
    asyncio.run(main_async(dashboard))

@app.command()
def main():
    """
    #TODO pass in commandline arguments
    """

    # Start the async tasks in a separate thread
    #TODO update n_queues based on number of proxy workers
    num_queues = 12
    dashboard = DashboardApp(num_queues=num_queues)
    async_thread = threading.Thread(
        target=main_in_background,
        kwargs={'dashboard': dashboard},
        daemon=True
    )
    async_thread.start()

    # Run the dashboard in the main thread (must be in the main thread to avoid signal handling issues)
    dashboard.run(debug=True, use_reloader=False)

if __name__ == "__main__":
    app()
