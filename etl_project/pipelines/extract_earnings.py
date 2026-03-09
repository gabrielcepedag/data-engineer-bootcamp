import os
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
    load_silver_earnings_data,
)

from assets.gold_earnings_data import (
    transform_gold_by_entity,
    transform_gold_by_province,
    transform_gold_by_person_type,
    transform_gold_by_currency,
    load_gold_data,
)

from assets.config_data import (
    get_last_period,
    get_records_per_page,
    set_last_period,
)

from assets.pipeline_logging import (
    start_pipeline_run,
    finish_pipeline_run,
    fail_pipeline_run,
)


def _next_month(d: date) -> date:
    """Return the first day of the month following d."""
    if d.month == 12:
        return date(d.year + 1, 1, 1)
    return date(d.year, d.month + 1, 1)


def run_earnings_pipeline(
        server_name,
        database_name,
        username,
        password,
        port,
        api_key,
        config_schema="config",
        bronze_schema="bronze",
        silver_schema="silver",
        gold_schema="gold",
    ):
    """Execute the full earnings ETL pipeline: Bronze → Silver → Gold.

    Pipeline steps:
    1. Read config.params to determine start_date (incremental or full load)
       and records_per_page.
    2. Extract earnings data from the SB API starting from start_date.
    3. Upsert raw records into the bronze schema.
    4. Read the full bronze table back from the database.
    5. Transform bronze data (rename, cast, cleanse) into the silver format.
    6. Upsert transformed records into the silver schema.
    7. Aggregate silver data into four gold tables and write each to both
       PostgreSQL and a local Parquet file.
    8. Update config.params with the latest period successfully loaded.
    9. Write pipeline run metadata (start time, end time, status, record counts)
       to config.pipeline_runs for observability.

    Args:
        server_name: PostgreSQL host (e.g. "postgres" inside Docker).
        database_name: Name of the target database.
        username: Database user.
        password: Database password.
        port: Database port (default 5432).
        api_key: SB API subscription key.
        config_schema: Schema that holds config.params (default "config").
        bronze_schema: Schema for raw ingested data (default "bronze").
        silver_schema: Schema for cleaned/typed data (default "silver").
        gold_schema: Schema for aggregated analytics tables (default "gold").
    """

    logger.info("=" * 60)
    logger.info("  EARNINGS PIPELINE — START")
    logger.info("=" * 60)

    # -----------------------------------------
    # Clients
    # -----------------------------------------

    logger.info("[INIT] Connecting to PostgreSQL...")
    postgres_client = PostgreSqlClient(
        server_name=server_name,
        database_name=database_name,
        username=username,
        password=password,
        port=port
    )
    logger.info(f"[INIT] Connected to {database_name}@{server_name}:{port}")

    logger.info("[INIT] Initializing SB API client...")
    sb_client = SBApiClient(api_key=api_key)
    logger.info("[INIT] Clients ready")

    # -----------------------------------------
    # PIPELINE RUN — open metadata record
    # -----------------------------------------

    config_metadata = MetaData(schema=config_schema)
    run_id = start_pipeline_run(
        client=postgres_client,
        metadata=config_metadata,
        pipeline_name="earnings_pipeline",
    )

    try:
        # -----------------------------------------
        # CONFIG — incremental load params
        # -----------------------------------------

        logger.info("-" * 60)
        logger.info("[CONFIG] Reading pipeline parameters from config.params...")

        records_per_page = get_records_per_page(postgres_client)
        last_period = get_last_period(postgres_client)

        logger.info(f"[CONFIG] records_per_page  = {records_per_page}")
        logger.info(f"[CONFIG] last_pipeline_execution = {last_period or 'NULL (first run)'}")

        if last_period is None:
            default_start = os.getenv("DEFAULT_START_DATE", "2026-01-01")
            start_date = date.fromisoformat(default_start)
            logger.info(f"[CONFIG] Mode: FULL LOAD — start_date = {start_date}")
        else:
            start_date = _next_month(last_period)
            logger.info(f"[CONFIG] Mode: INCREMENTAL — start_date = {start_date}")

        # -----------------------------------------
        # EXTRACT
        # -----------------------------------------

        logger.info("-" * 60)
        logger.info(f"[EXTRACT] Fetching data from SB API (start_date={start_date}, pages of {records_per_page})...")

        raw_data = extract_earnings_data(
            sb_client, ['BM'],
            start_date,
            records_per_page
        )

        if raw_data is None or len(raw_data) == 0:
            logger.error("[EXTRACT] No data returned from API. Stopping pipeline.")
            fail_pipeline_run(postgres_client, config_metadata, run_id, "No data returned from API")
            return

        logger.info(f"[EXTRACT] Done — {len(raw_data)} records fetched")

        # -----------------------------------------
        # LOAD BRONZE
        # -----------------------------------------

        logger.info("-" * 60)
        logger.info(f"[BRONZE] Upserting {len(raw_data)} records into {bronze_schema}.earnings...")

        bronze_metadata = MetaData(schema=bronze_schema)
        load_earnings_data(
            df=raw_data,
            client=postgres_client,
            metadata=bronze_metadata
        )

        logger.info(f"[BRONZE] Upsert complete → {bronze_schema}.earnings")

        # -----------------------------------------
        # READ BRONZE
        # -----------------------------------------

        logger.info(f"[BRONZE] Reading full table {bronze_schema}.earnings for silver transform...")

        bronze_df = read_bronze_earnings_data(
            client=postgres_client,
            schema=bronze_schema
        )

        if bronze_df is None or len(bronze_df) == 0:
            logger.error("[BRONZE] No data found after read. Stopping pipeline.")
            fail_pipeline_run(postgres_client, config_metadata, run_id, "Bronze table empty after upsert")
            return

        logger.info(f"[BRONZE] Read complete — {len(bronze_df)} rows | columns: {list(bronze_df.columns)}")

        # -----------------------------------------
        # TRANSFORM (SILVER)
        # -----------------------------------------

        logger.info("-" * 60)
        logger.info("[SILVER] Transforming bronze data (rename, cast, cleanse)...")

        silver_df = transform_earnings_data(bronze_df)

        if silver_df is None or len(silver_df) == 0:
            logger.error("[SILVER] Transformation returned no data. Stopping pipeline.")
            fail_pipeline_run(postgres_client, config_metadata, run_id, "Silver transform returned empty DataFrame")
            return

        periods = sorted(silver_df["period_date"].unique())
        logger.info(f"[SILVER] Transform complete — {len(silver_df)} rows")
        logger.info(f"[SILVER] Periods in data: {[str(p) for p in periods]}")

        # -----------------------------------------
        # LOAD SILVER
        # -----------------------------------------

        logger.info(f"[SILVER] Upserting into {silver_schema}.earnings...")

        silver_metadata = MetaData(schema=silver_schema)
        load_silver_earnings_data(
            df=silver_df,
            client=postgres_client,
            metadata=silver_metadata
        )

        logger.info(f"[SILVER] Upsert complete → {silver_schema}.earnings")

        # -----------------------------------------
        # TRANSFORM + LOAD GOLD
        # -----------------------------------------

        logger.info("-" * 60)
        logger.info("[GOLD] Building aggregated gold tables...")

        gold_metadata = MetaData(schema=gold_schema)
        parquet_dir = os.getenv("PARQUET_OUTPUT_DIR", "data/gold")

        gold_tables = {
            "earnings_by_entity":      transform_gold_by_entity(silver_df),
            "earnings_by_province":    transform_gold_by_province(silver_df),
            "earnings_by_person_type": transform_gold_by_person_type(silver_df),
            "earnings_by_currency":    transform_gold_by_currency(silver_df),
        }

        for table_name, gold_df in gold_tables.items():
            logger.info(f"[GOLD] {table_name} — {len(gold_df)} rows → upsert + parquet")
            load_gold_data(
                df=gold_df,
                table_name=table_name,
                client=postgres_client,
                metadata=gold_metadata,
                parquet_dir=parquet_dir,
            )

        logger.info(f"[GOLD] All tables loaded. Parquet files saved to: {parquet_dir}/")

        # -----------------------------------------
        # UPDATE CONFIG — mark last loaded period
        # -----------------------------------------

        logger.info("-" * 60)
        logger.info("[CONFIG] Updating last_pipeline_execution...")

        max_period = silver_df["period_date"].max()
        set_last_period(postgres_client, max_period)
        logger.info(f"[CONFIG] last_pipeline_execution = {max_period.strftime('%Y-%m')}")

        # -----------------------------------------
        # PIPELINE RUN — close metadata record
        # -----------------------------------------

        finish_pipeline_run(
            client=postgres_client,
            metadata=config_metadata,
            run_id=run_id,
            records_extracted=len(raw_data),
            records_loaded=len(silver_df),
        )

        logger.info("=" * 60)
        logger.success("  EARNINGS PIPELINE — FINISHED SUCCESSFULLY")
        logger.info("=" * 60)

    except Exception as e:
        logger.exception(f"[PIPELINE] Unhandled error: {e}")
        fail_pipeline_run(postgres_client, config_metadata, run_id, str(e))
        raise
