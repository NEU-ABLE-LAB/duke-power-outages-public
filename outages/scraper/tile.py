"""
Get the outage information for a lat-long tile asynchronously using per-proxy rate limiting and backoff.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Optional

import httpx
import pandas as pd
from loguru import logger

from outages.config import RAW_TILES_DIR
from outages.utils_geo import BoundingBox
from outages.scraper.auth import get_auth_token
from outages.scraper.utils_http import NoContentError

REQUESTS_TIMEOUT = 10

async def get_events_from_tile(
    client: Optional[httpx.AsyncClient] = None,
    tile: BoundingBox = BoundingBox(33.1, -84.4, 33.2, -84.3),
    output_path: Optional[Path] = RAW_TILES_DIR,
    proxy: Optional[str] = None,
    run_id: Optional[str] = None,
) -> pd.DataFrame:
    """
    Asynchronously scrape the website for outage information in a lat-long tile.

    Args:
        box (BoundingBox): The bounding box for the tile.
        output_path (Optional[Path]): Path to save the output data. Defaults to RAW_AREAS_DIR.
        proxy (Optional[str]): Proxy URL to use for the request.
        client (Optional[httpx.AsyncClient]): The HTTP client to use.
        run_id (Optional[str]): The run ID unique for this tile and time

    Returns:
        pd.DataFrame: DataFrame containing the outage information.

    Raises:
        NoContentError: If the server returns a 204 No Content response.
        httpx.HTTPStatusError: If the HTTP response status is an error.
        httpx.RequestError: If an error occurs while making the request.
    """
    if output_path is not False and output_path is not None:
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        tile_name = tile.name
        output_file = output_path / tile_name / f"tiles-{timestamp}.parquet"

        # Create the necessary directory(ies) if needed
        output_file.parent.mkdir(parents=True, exist_ok=True)

    # Define the URL and parameters
    url = 'https://prod.apigee.duke-energy.app/outage-maps/v1/outages'
    params = {
        'jurisdiction': tile.jurisdiction,
        'swLat': tile.sw_lat,
        'swLong': tile.sw_long,
        'neLat': tile.ne_lat,
        'neLong': tile.ne_long,
    }

    # Get authorization token
    authorization_token = get_auth_token()

    # Headers for the GET request
    get_headers = {
        'Host': 'prod.apigee.duke-energy.app',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:130.0) Gecko/20100101 Firefox/130.0',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.5',
        'Authorization': authorization_token,
        'Origin': 'https://outagemap.duke-energy.com',
        'Connection': 'keep-alive',
    }

    # Use the provided client or create a new one
    if client is None:
        async with httpx.AsyncClient(timeout=REQUESTS_TIMEOUT, proxies=proxy) as client:
            try:
                response = await client.get(url, headers=get_headers, params=params)
            except httpx.RequestError as e:
                logger.error(f"An error occurred while requesting {e.request.url!r}.")
                raise
    else:
        try:
            response = await client.get(url, headers=get_headers, params=params)
        except httpx.RequestError as e:
            logger.error(f"An error occurred while requesting {e.request.url!r}.")
            raise

    # Check if the response was successful
    if response.status_code == 200:
        # Read JSON
        json_response = response.json()

        # Convert to DataFrame
        df_tile_events = pd.json_normalize(json_response['data'])
        if not df_tile_events.empty:
            df_tile_events['convexHull'] = df_tile_events['convexHull'].apply(json.dumps)
            df_tile_events['run_id'] = run_id
            df_tile_events['jurisdiction'] = tile.jurisdiction

        # Save DataFrame to Parquet file
        if output_path is not False and output_path is not None:
            df_tile_events.to_parquet(output_file, engine='pyarrow')
            logger.info(f"JSON response saved to '{output_file}'")

        return df_tile_events

    # Check if the response code is 204 (No Content)
    elif response.status_code == 204:
        raise NoContentError("No content available")

    else:
        # logger.error(f"GET request failed for proxy with status code {response.status_code}")
        # logger.error(f"Response: {response.text}")
        response.raise_for_status()
