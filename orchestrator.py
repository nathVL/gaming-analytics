import sys
import os
import subprocess

from src.fetch_data import run_data_ingestion
from src.load_bronze import load_bronze
from src.config import setup_logging

logger = setup_logging()

def run_pipeline():
    """Orchestrates the full ETL pipeline."""
    logger.info("--- Pipeline starting ---")

    run_data_ingestion()
    load_bronze()
    # TODO: add the dbt run
    
    logger.info("--- Pipeline ended successfully ! ---")

if __name__ == "__main__":
    run_pipeline()