import os
import zipfile
import tempfile
from pathlib import Path
import requests

from config import setup_logging, DATA_DIR, DATASET_URL

logger = setup_logging()

def download_file(url: str, destination: Path) -> None:
    """Downloads a file in a secure way."""
    logger.info(f"Downloading from : {url}")
    response = requests.get(url, stream=True, timeout=30)
    response.raise_for_status()
    
    with open(destination, "wb") as f:
        for chunk in response.iter_content(chunk_size=1024 * 8):
            f.write(chunk)

def extract_zip(zip_path: Path, extract_to: Path) -> None:
    """Extracts a ZIP file to a destination."""
    logger.info(f"Extracting to : {extract_to}")
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)

def run_data_ingestion() -> None:
    """Main orchestrator for data retrieval."""
    if not DATASET_URL:
        logger.error("DATASET_URL isn't defined in .env")
        return

    # Ensure data directory exists
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Create temp directory for download
    temp_dir = tempfile.mkdtemp()
    temp_file = Path(temp_dir) / "dataset.zip"

    try:
        download_file(DATASET_URL, temp_file)
        extract_zip(temp_file, DATA_DIR)
        logger.info("Dataset successfully updated.")
    except requests.exceptions.RequestException as e:
        logger.error(f"Network error: {e}")
    except zipfile.BadZipFile:
        logger.error("The downloaded file is not a valid ZIP.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        # Cleanup
        if temp_file.exists():
            os.remove(temp_file)
            os.rmdir(temp_dir)

if __name__ == "__main__":
    run_data_ingestion()