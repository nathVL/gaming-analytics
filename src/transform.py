import subprocess
import sys
import logging
from src.config import PROJECT_ROOT

logger = logging.getLogger()

def run_dbt():
    """Runs the dbt transformations."""
    logger.info("Starting dbt transformations...")
    
    transform_dir = PROJECT_ROOT / "transform"
    
    try:
        subprocess.run(
            [
                "dbt", "run",
                "--project-dir", str(transform_dir),
                "--profiles-dir", str(transform_dir)
            ],
            check=True,
            cwd=str(PROJECT_ROOT)
        )
        logger.info("dbt transformations completed successfully.")
    except subprocess.CalledProcessError as e:
        logger.error(f"dbt run failed: {e}")
        sys.exit(1)
    except FileNotFoundError:
        logger.error("dbt command not found. Please ensure dbt is installed and in your PATH.")
        sys.exit(1)
