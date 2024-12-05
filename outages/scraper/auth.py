"""
Get an authorization token from Duke Energy
"""

import time
from typing import Optional

import typer
from loguru import logger

# pylint: disable=import-error
from seleniumwire import webdriver # Pylint does not recognize the seleniumwire package
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException, WebDriverException

from outages.config import DUKE_AUTH_TOKEN

BASE_URL = 'https://outagemap.duke-energy.com/#/carolinas'
ONBOARDING_URL = 'https://outagemap.duke-energy.com/assets/JSON/onboarding.json'

app = typer.Typer()

@app.command()
def get_auth_token() -> Optional[str]:
    """
    Retrieves the authorization token for Duke Energy outage portal.
    This function attempts to retrieve the authorization token from the environment variable 
    'DUKE_AUTH_TOKEN'. If the environment variable is not set or is empty, it initializes a 
    Selenium WebDriver to navigate to the Duke Energy Carolinas webpage, selects the appropriate 
    jurisdiction, and extracts the authorization token from the request headers.

    Returns:
        Optional[str]: The authorization token if found, otherwise None.
    """

    # Check if DUKE_AUTH_TOKEN exists and is a non-empty string
    if DUKE_AUTH_TOKEN:
        # logger.debug(f"Using existing DUKE_AUTH_TOKEN from environment: {DUKE_AUTH_TOKEN}")
        #TODO validate token
        return DUKE_AUTH_TOKEN

    # Initialize the webdriver
    driver = webdriver.Chrome()

    try:
        # Visit the webpage
        driver.get(BASE_URL)

        # Find the div with the specified class and child elements
        try:
            element = driver.find_element(By.XPATH, "//div[contains(@class, 'jurisdiction-selection-select-state__item') and .//a[.//img[@alt='Duke Energy Carolinas icon']]]")
            element.click()
            time.sleep(5)

            # Reprocess the requests after clicking
            for request in driver.requests:
                logger.debug(f"Request URL: {request.url}")

                if request.url.startswith(ONBOARDING_URL):
                    # Get the request headers
                    headers = request.headers
                    logger.debug(f"Request headers: {headers}")

                    # Get the response headers
                    response_headers = request.response.headers
                    logger.debug(f"Response headers: {response_headers}")
                    
                    auth_token = headers.get('Authorization')
                    logger.debug(f"Authorization token: {auth_token}")

                    # Return the value of the 'Authorization' field
                    return auth_token
                
        except NoSuchElementException as e:
            logger.error(f"Could not find the Duke Energy Carolinas icon: {e}")
        except WebDriverException as e:
            logger.error(f"WebDriver encountered an error: {e}")


    finally:
        # Ensure the driver is quit regardless of success or failure
        driver.quit()

    # Return None if the 'Authorization' field is not found
    return None

if __name__ == "__main__":
    app()
