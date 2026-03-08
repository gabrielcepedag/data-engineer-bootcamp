from loguru import logger
from datetime import date
from sqlalchemy import MetaData
from connectors.postgresql import PostgreSqlClient
from connectors.sb_api import SBApiClient
from assets.earnings_data import extract_earnings_data, load_earnings_data

def run_earnings_pipeline(
        server_name,
        database_name,
        username,
        password,
        port,
        api_key,
        records_per_page,
        config_schema="config",
        bronze_schema="bronze",
        silver_schema="silver",
        gold_schema="gold",
    ):
    logger.info("Beginning Earnings Pipeline...")
    postgres_client = PostgreSqlClient(
        server_name=server_name,
        database_name=database_name,
        username=username,
        password=password,
        port=port)
    sb_client = SBApiClient(api_key=api_key)

    raw_data = extract_earnings_data(sb_client, ['BM'], date(2026,1,1), records_per_page)

    if len(raw_data) == 0:
        logger.error("No data found. Stopping.")
        return
    
    bronze_metadata = MetaData(schema=bronze_schema)

    logger.info("Loading extracted data into DB")
    load_earnings_data(df=raw_data, client=postgres_client, metadata=bronze_metadata)
    logger.info("Data loading complete.")