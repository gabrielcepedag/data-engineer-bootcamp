import os
from dotenv import load_dotenv 
from loguru import logger

from pipelines.extract_earnings import run_earnings_pipeline

if __name__ == "__main__":
    load_dotenv()

    db_host = os.getenv("DB_HOST")
    db_name = os.getenv("DB_NAME")
    db_port = os.getenv("DB_PORT")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")

    config_schema = os.getenv("CONFIG_SCHEMA")
    bronze_schema = os.getenv("BRONZE_SCHEMA")
    silver_schema = os.getenv("SILVER_SCHEMA")
    gold_schema = os.getenv("GOLD_SCHEMA")

    api_key = os.getenv("SB_API_KEY")

    run_earnings_pipeline(
        server_name=db_host,
        database_name=db_name,
        username=db_user,
        password=db_password,
        port=db_port,
        api_key=api_key,
        records_per_page=500,
        config_schema=config_schema,
        bronze_schema=bronze_schema,
        silver_schema=silver_schema,
        gold_schema=gold_schema
    )
