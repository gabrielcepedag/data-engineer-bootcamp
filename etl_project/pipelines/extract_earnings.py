from loguru import logger
from datetime import date
from sqlalchemy import MetaData

from connectors.postgresql import PostgreSqlClient
from connectors.sb_api import SBApiClient

from assets.earnings_data import (
    extract_earnings_data,
    load_earnings_data,
    read_bronze_earnings_data,
)

from assets.silver_earnings_data import (
    transform_earnings_data,
    load_silver_earnings_data
)


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

    logger.info("Starting Earnings Pipeline...")


    # -----------------------------------------
    # Clients
    # -----------------------------------------

    postgres_client = PostgreSqlClient(
        server_name=server_name,
        database_name=database_name,
        username=username,
        password=password,
        port=port
    )

    sb_client = SBApiClient(api_key=api_key)

    # -----------------------------------------
    # EXTRACT
    # -----------------------------------------

    logger.info("Extracting earnings data from API")

    raw_data = extract_earnings_data(
        sb_client, ['BM'], 
        date(2026,1,1), 
        records_per_page
    )

    if raw_data is None or len(raw_data) == 0:
        logger.error("No data returned from API. Stopping pipeline.")
        return
    
    logger.info(f"Extracted {len(raw_data)} records")

    # -----------------------------------------
    # LOAD BRONZE
    # -----------------------------------------

    bronze_metadata = MetaData(schema=bronze_schema)

    logger.info("Loading data into Bronze layer")

    load_earnings_data(
        df=raw_data,
        client=postgres_client,
        metadata=bronze_metadata
    )

    logger.info("Bronze load complete")

    # -----------------------------------------
    # READ BRONZE
    # -----------------------------------------

    logger.info("Reading Bronze data")

    bronze_df = read_bronze_earnings_data(
        client=postgres_client,
        schema=bronze_schema
    )

    if bronze_df is None or len(bronze_df) == 0:
        logger.error("No data found in Bronze. Stopping pipeline.")
        return

    logger.info(f"Read {len(bronze_df)} records from Bronze")

    # -----------------------------------------
    # TRANSFORM (SILVER)
    # -----------------------------------------

    logger.info("Transforming Bronze data for Silver layer")

    silver_df = transform_earnings_data(bronze_df)

    if silver_df is None or len(silver_df) == 0:
        logger.error("Transformation returned no data.")
        return

    logger.info(f"Transformed {len(silver_df)} records")

    # -----------------------------------------
    # LOAD SILVER
    # -----------------------------------------

    silver_metadata = MetaData(schema=silver_schema)

    logger.info("Loading data into Silver layer")

    load_silver_earnings_data(
        df=silver_df,
        client=postgres_client,
        metadata=silver_metadata
    )

    logger.info("Silver load complete")

    logger.success("Earnings Pipeline finished successfully")