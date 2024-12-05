"""
Get the full outage information for an event
"""

from pathlib import Path
import datetime
import json
from typing import Optional
import asyncio

import typer
from loguru import logger

import httpx

import pandas as pd

from outages.config import RAW_EVENT_DIR
from outages.scraper.auth import get_auth_token
from outages.scraper.utils_http import (
    NoContentError,
)

REQUESTS_TIMEOUT = 30
N_TRIES = 3
TEST_SOURCE_EVENT_NUMBER = '13275187'

app = typer.Typer()

async def get_event_full_info(
    client: Optional[httpx.AsyncClient] = None,
    output_path: Path = RAW_EVENT_DIR,
    source_event_number: str = TEST_SOURCE_EVENT_NUMBER,
    proxy: Optional[str] = None,
    jurisdiction: str = 'DEC',
) -> pd.DataFrame:
    """
    Scrapes the Duke Energy website for outage information for a specific event.
    """

    # Define the URL and parameters
    outages_url = 'https://prod.apigee.duke-energy.app/outage-maps/v1/outages/outage'
    params = {
        'jurisdiction': jurisdiction,
        'sourceEventNumber': source_event_number,
    }

    # Get authorization token
    authorization_token = get_auth_token()

    # Headers for the GET request
    get_headers = {
        'Host': 'prod.apigee.duke-energy.app',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:130.0) Gecko/20100101 Firefox/130.0',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'Authorization': authorization_token,
        'Origin': 'https://outagemap.duke-energy.com',
        'Connection': 'keep-alive',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'cross-site',
        'Pragma': 'no-cache',
        'Cache-Control': 'no-cache',
        'TE': 'trailers'
    }

    if client is None:
        async with httpx.AsyncClient(proxy=proxy) as client:
            df = await fetch_event_data(
                client,
                output_path,
                source_event_number,
                get_headers,
                outages_url,
                params,
            )
    else:
        df = await fetch_event_data(
            client,
            output_path,
            source_event_number,
            get_headers,
            outages_url,
            params,
        )

    return df

async def fetch_event_data(
        client: httpx.AsyncClient,
        output_path: Path,
        source_event_number: str,
        get_headers: dict,
        outages_url: str,
        params: dict
) -> pd.DataFrame:
    """
    Fetch event data from the specified outages URL and save it to a parquet file.
    Args:
        client (httpx.AsyncClient): The HTTP client to use for making the request.
        output_path (Path): The path where the output parquet file should be saved.
        source_event_number (str): The event number used to create the output file path.
        get_headers (dict): The headers to include in the GET request.
        outages_url (str): The URL to fetch the event data from.
        params (dict): The parameters to include in the GET request.
    Returns:
        pd.DataFrame: A DataFrame containing the fetched event data.
    Raises:
        NoContentError: If the response status code is 204 (No Content).
        httpx.HTTPStatusError: If the response contains an HTTP error status.
    """

    response = await client.get(outages_url, headers=get_headers, params=params)
    response.raise_for_status()

    # Check if the response code is 204 (No Content)
    if response.status_code == 204:
        raise NoContentError("No content available")
    
    # Parse the JSON response
    json_response = response.json()

    # Convert to DataFrame
    if 'errorMessages' in json_response and json_response['errorMessages']:
        logger.error(f"Error messages received: {json_response['errorMessages']}")
        return pd.DataFrame()

    df = pd.json_normalize(json_response['data'])
    if not df.empty:
        df['countiesAffected'] = df['countiesAffected'].apply(json.dumps)
        df['statesAffected'] = df['statesAffected'].apply(json.dumps)

    # Save the data to a parquet file
    if output_path is not False:

        timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
        output_file = output_path / source_event_number / f"event-{timestamp}.parquet"

        # Create the necessary directory(ies) if needed
        output_file.parent.mkdir(parents=True, exist_ok=True)

        df.to_parquet(output_file, engine='pyarrow')
        logger.info(f"Response saved to '{output_file}'")

    return df
        
@app.command()
def main():
    """
    Get the full outage information for an event
    """
    typer.echo("Running main function")

    # Run the main function
    asyncio.run(get_event_full_info())

if __name__ == "__main__":
    app()
