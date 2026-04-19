from fetch_data import run_data_ingestion
from load_bronze import load_bronze
from config import setup_logging

logger = setup_logging()

def run_pipeline():
    """Orchestrates the full ETL pipeline."""
    run_data_ingestion()
    load_bronze()

if __name__ == "__main__":
    run_pipeline()
