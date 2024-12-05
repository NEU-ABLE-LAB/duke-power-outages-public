"""
Get County outage information from Duke Energy
"""

from pathlib import Path
from datetime import datetime

import typer
from loguru import logger

import requests

from outages.config import RAW_COUNTIES_DIR
from outages.scraper.auth import get_auth_token

REQUESTS_TIMEOUT = 10

app = typer.Typer()

@app.command()
def main(
        output_json_path: Path = RAW_COUNTIES_DIR,
        jurisdiction: str = 'DEC'
    ) -> None:
    """
    Scrapes the Duke Energy website for county outage information.

    Args:
        input_path (Path, optional): The path to the input JSON file or directory. 
                                     If not provided, a timestamped JSON file will be created.
        jurisdiction (str, optional): The jurisdiction to scrape. Defaults to 'DEC'.
    """
    
    # Name timestamp filename if output_json_path is a directory
    #TODO Fix if RAW_COUNTIES_DIR does not exist, then it creates the path, but saves to the path as a file
    # logger.debug(f"output_json_path: {output_json_path}")
    if output_json_path.is_dir():
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        output_json_path = output_json_path / jurisdiction / f"counties-{timestamp}.json"

    # Create the necessary directory(ies) if needed
    output_json_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Define the URL and parameters
    url = 'https://prod.apigee.duke-energy.app/outage-maps/v1/counties'
    params = {
        'jurisdiction': jurisdiction
    }

    # Headers for the OPTIONS request
    options_headers = {
        'Host': 'prod.apigee.duke-energy.app',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:130.0) Gecko/20100101 Firefox/130.0',
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'Access-Control-Request-Method': 'GET',
        'Access-Control-Request-Headers': 'authorization',
        'Origin': 'https://outagemap.duke-energy.com',
        'Connection': 'keep-alive',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'cross-site',
        'Pragma': 'no-cache',
        'Cache-Control': 'no-cache'
    }

    # # Send the OPTIONS request
    # options_response = requests.options(url, headers=options_headers, params=params, timeout=REQUESTS_TIMEOUT)

    # # Optional: Print the OPTIONS response headers
    # print("OPTIONS Response Headers:")
    # for key, value in options_response.headers.items():
    #     print(f"{key}: {value}")

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

    # Send the GET request
    get_response = requests.get(url, headers=get_headers, params=params, timeout=REQUESTS_TIMEOUT)

    # Check if the response was successful
    if get_response.status_code == 200:
        # Save the JSON response to a file
        with open(output_json_path, 'w', encoding='utf-8') as f:
            f.write(get_response.text)
        logger.info(f"JSON response saved to '{output_json_path}'")
    else:
        logger.error(f"GET request failed with status code {get_response.status_code}")
        logger.error("Response:", get_response.text)
    
if __name__ == "__main__":
    app()