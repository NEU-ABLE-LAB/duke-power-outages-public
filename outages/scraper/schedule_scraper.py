"""
Schedule scraper to run at fixed time intervals
"""
import asyncio
import threading
import time

from apscheduler.schedulers.asyncio import AsyncIOScheduler
import typer
from loguru import logger

from outages.config import RAW_EVENTS_DIR
from outages.scraper.counties import main as scrape_counties
from outages.scraper.dashboard import DashboardApp
from outages.scraper.events_details import get_events_full_info
from outages.scraper.region import sweep_all
from outages.scraper.utils_http import create_task_manager, proxy_list
from outages.utils_geo import REGIONS

app = typer.Typer()

MIN_UPDATE_PERIOD = 30

async def scheduled_tasks(manager, dashboard):
    """
    Tasks to run at fixed intervals to get all the data
    """
    # Get the county data from each region
    for region in REGIONS:
        logger.info(f'Scraping counties for {region.name}')
        scrape_counties(
            jurisdiction=region.jurisdiction
        )

    # Get the event data from each region
    df_region_events_dict = {}
    for region in REGIONS:
        logger.info(f'Scraping events for {region.name}')
        df_region_events, _ = await sweep_all(
            manager=manager,
            region=region,
            dashboard=dashboard,
        )
        logger.info(f'Found {len(df_region_events)} events for {region.name}')
        df_region_events_dict[region.name] = df_region_events

    # Get full event details for each region
    for region in REGIONS:
        logger.info(f'Getting full event details for {region.name}')
        df_region_events = df_region_events_dict[region.name]
        if not df_region_events.empty:
            event_numbers = df_region_events['sourceEventNumber'].tolist()
            await get_events_full_info(
                manager=manager,
                source_event_numbers=event_numbers,
                output_path= RAW_EVENTS_DIR / region.name,
                region=region,
                dashboard=dashboard,
            )



def sync_scheduled_tasks(
        loop: asyncio.AbstractEventLoop,
        manager: asyncio.Task,
        dashboard: DashboardApp
    ) -> None:
    """
    Synchronous wrapper for scheduled_tasks to be used with the schedule library.
    This function uses asyncio.run_coroutine_threadsafe to run the async task safely.
    """
    asyncio.run_coroutine_threadsafe(scheduled_tasks(manager, dashboard), loop)

async def main_async(dashboard: DashboardApp):
    """
    Main function to run the scraper
    """
    async with create_task_manager(dashboard) as manager:

        scheduler = AsyncIOScheduler()
        scheduler.add_job(
            scheduled_tasks,
            'interval',
            minutes=MIN_UPDATE_PERIOD,
            kwargs={'manager': manager, 'dashboard': dashboard}
        )
        scheduler.start()

        # Initial run of the scheduled tasks
        await scheduled_tasks(manager, dashboard)

        while True:
            await asyncio.sleep(1)  # Keep the loop running

def start_event_loop(loop: asyncio.AbstractEventLoop) -> None:
    """
    Start the event loop in the background thread
    """
    asyncio.set_event_loop(loop)
    loop.run_forever()

def main_in_background(dashboard: DashboardApp, loop: asyncio.AbstractEventLoop) -> None:
    """
    Run the main_async function in the background
    """
    # Use run_coroutine_threadsafe to submit tasks to the main event loop
    asyncio.run_coroutine_threadsafe(
        main_async(dashboard=dashboard),
        loop=loop
    )

@app.command()
def main():
    """
    Entry point for the schedule scraper script.
    This function initializes and runs the asynchronous main function using asyncio.run().
    """
    
    # Start the async tasks in a separate thread
    num_queues = len(proxy_list)
    dashboard = DashboardApp(num_queues=num_queues)

    # Create a new event loop
    loop = asyncio.new_event_loop()

    # Start the event loop in a background thread
    async_thread = threading.Thread(
        target=start_event_loop,
        args=(loop,),
        daemon=True
    )
    async_thread.start()

    # Start the main async function in the background
    main_in_background(dashboard, loop)

    # Run the dashboard in the main thread (must be in the main thread to avoid signal handling issues)
    dashboard.run(debug=True, use_reloader=False)

if __name__ == "__main__":
    app()
