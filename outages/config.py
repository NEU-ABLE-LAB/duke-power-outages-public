import os
from pathlib import Path

from dotenv import load_dotenv
from loguru import logger

# Load environment variables from .env file if it exists
load_dotenv()

# Paths
PROJ_ROOT = Path(__file__).resolve().parents[1]
logger.info(f"PROJ_ROOT path is: {PROJ_ROOT}")

DATA_DIR = PROJ_ROOT / "data"

RAW_DATA_DIR = DATA_DIR / "raw"
RAW_COUNTIES_DIR = RAW_DATA_DIR / "counties"
RAW_TILES_DIR = RAW_DATA_DIR / "tiles"
RAW_EVENT_DIR = RAW_DATA_DIR / "event"
RAW_EVENTS_DIR = RAW_DATA_DIR / "events"
RAW_REGIONS_DIR = RAW_DATA_DIR / "regions"

INTERIM_DATA_DIR = DATA_DIR / "interim"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
EXTERNAL_DATA_DIR = DATA_DIR / "external"

MODELS_DIR = PROJ_ROOT / "models"

REPORTS_DIR = PROJ_ROOT / "reports"
FIGURES_DIR = REPORTS_DIR / "figures"

# Secrets
DUKE_AUTH_TOKEN = os.getenv('DUKE_AUTH_TOKEN')
if DUKE_AUTH_TOKEN:
    logger.debug(f"Using existing DUKE_AUTH_TOKEN from environment: {DUKE_AUTH_TOKEN}")
    

# If tqdm is installed, configure loguru with tqdm.write
# https://github.com/Delgan/loguru/issues/135
try:
    from tqdm import tqdm

    logger.remove(0)
    logger.add(lambda msg: tqdm.write(msg, end=""), colorize=True)
except ModuleNotFoundError:
    pass
