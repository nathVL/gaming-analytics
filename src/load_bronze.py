import duckdb

from src.config import setup_logging, DB_PATH, DB_DIR, DATA_DIR

logger = setup_logging()

def load_bronze():
    # Ensure the database directory exists
    DB_DIR.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Connecting to DuckDB at {DB_PATH}...")
    conn = duckdb.connect(str(DB_PATH))
    
    twitch_data_path = str(DATA_DIR / "saesave" / "twitch" / "*.csv")
    steam_data_path = str(DATA_DIR / "saesave" / "steam" / "*" / "*.json")
    
    logger.info("Loading Twitch data into table `bronze_twitch`...")
    conn.execute(f"""
        CREATE OR REPLACE TABLE bronze_twitch AS 
        SELECT * FROM read_csv_auto('{twitch_data_path}');
    """)
    
    logger.info("Loading Steam data into table `bronze_steam`...")
    conn.execute(f"""
        CREATE OR REPLACE TABLE bronze_steam AS 
        SELECT * FROM read_json_auto('{steam_data_path}');
    """)

    logger.info("Bronze layer load complete.")
    logger.info(f"Total rows in bronze_twitch: {conn.execute('SELECT COUNT(*) FROM bronze_twitch').fetchone()[0]:,}")
    logger.info(f"Total rows in bronze_steam: {conn.execute('SELECT COUNT(*) FROM bronze_steam').fetchone()[0]:,}")
    
    conn.close()

if __name__ == "__main__":
    load_bronze()
