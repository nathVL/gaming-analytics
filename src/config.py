import logging
import os
from pathlib import Path
from dotenv import load_dotenv

# Constants
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
DB_DIR = PROJECT_ROOT / "database"
DB_PATH = DB_DIR / "warehouse.duckdb"

load_dotenv(PROJECT_ROOT / ".env")

# Logging config
def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    return logging.getLogger("gaming-analytics")

# Env variables
DATASET_URL = os.getenv("DATASET_URL")