import pandas as pd
from pathlib import Path
from loguru import logger
from sqlalchemy import MetaData, Table, Column, String, Float, Integer, Date

from connectors.postgresql import PostgreSqlClient


def _aggregate(df: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    """Aggregate silver earnings data by the given group columns.

    Computes three metrics for each unique combination of group_cols:
    - total_balance: sum of all deposit balances in the group.
    - total_instruments: sum of all instrument counts in the group.
    - avg_weighted_rate: true portfolio-weighted average interest rate,
      calculated as SUM(balance × weighted_avg_rate_by_balance) / SUM(balance).
      This avoids the distortion of a simple arithmetic mean when individual
      balances differ significantly.

    Args:
        df: Silver earnings DataFrame produced by transform_earnings_data.
        group_cols: Column names to group by (e.g. ["period_date", "entity"]).

    Returns:
        Aggregated DataFrame with columns: group_cols + total_balance,
        total_instruments, avg_weighted_rate.
    """
    agg_df = (
        df.assign(balance_x_rate=df["balance"] * df["weighted_avg_rate_by_balance"])
        .groupby(group_cols, as_index=False)
        .agg(
            total_balance=("balance", "sum"),
            total_instruments=("instrument_count", "sum"),
            balance_x_rate_sum=("balance_x_rate", "sum"),
        )
    )
    agg_df["avg_weighted_rate"] = agg_df["balance_x_rate_sum"] / agg_df["total_balance"]
    return agg_df.drop(columns="balance_x_rate_sum")


def transform_gold_by_entity(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate deposits by financial entity and period.

    Groups silver data by period_date, entity, and entity_type to produce
    one row per bank per month. Useful for ranking institutions by deposit
    volume and comparing their portfolio-weighted interest rates over time.

    Business question answered: which banks hold the most deposits?

    Args:
        df: Silver earnings DataFrame produced by transform_earnings_data.

    Returns:
        DataFrame grouped by [period_date, entity, entity_type] with columns
        total_balance, total_instruments, and avg_weighted_rate.
    """
    return _aggregate(df, ["period_date", "entity", "entity_type"])


def transform_gold_by_province(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate deposits by geographic province and period.

    Groups silver data by period_date, province, and region to produce
    one row per province per month. Region is carried through as a non-key
    dimension for geographic hierarchy analysis (province → region).

    Business question answered: which provinces concentrate the most savings?

    Args:
        df: Silver earnings DataFrame produced by transform_earnings_data.

    Returns:
        DataFrame grouped by [period_date, province, region] with columns
        total_balance, total_instruments, and avg_weighted_rate.
    """
    return _aggregate(df, ["period_date", "province", "region"])


def transform_gold_by_person_type(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate deposits by depositor type and period.

    Groups silver data by period_date and person_type, collapsing all
    geographic and entity dimensions into two segments: Natural (retail
    individuals) and Juridico (corporate/legal entities). Useful for
    measuring the relative weight of each segment in total deposits.

    Business question answered: how do retail (Natural) vs. corporate
    (Juridico) deposits compare in volume and average rate?

    Args:
        df: Silver earnings DataFrame produced by transform_earnings_data.

    Returns:
        DataFrame grouped by [period_date, person_type] with columns
        total_balance, total_instruments, and avg_weighted_rate.
    """
    return _aggregate(df, ["period_date", "person_type"])


def transform_gold_by_currency(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate deposits by currency denomination and period.

    Groups silver data by period_date and currency, collapsing all other
    dimensions to produce a monthly snapshot of deposit volume split by
    currency (DOP vs. USD). Useful for measuring the dollarization level
    of the financial system and how it evolves over time.

    Business question answered: what percentage of total deposits are held
    in foreign currency (dollarization)?

    Args:
        df: Silver earnings DataFrame produced by transform_earnings_data.

    Returns:
        DataFrame grouped by [period_date, currency] with columns
        total_balance, total_instruments, and avg_weighted_rate.
    """
    return _aggregate(df, ["period_date", "currency"])


# Table schema factories — called each time to produce fresh Column objects
# (SQLAlchemy Column instances can only be assigned to one Table at a time)
_TABLE_SCHEMAS = {
    "earnings_by_entity": lambda: [
        Column("period_date", Date, primary_key=True),
        Column("entity", String, primary_key=True),
        Column("entity_type", String),
        Column("total_balance", Float),
        Column("total_instruments", Integer),
        Column("avg_weighted_rate", Float),
    ],
    "earnings_by_province": lambda: [
        Column("period_date", Date, primary_key=True),
        Column("province", String, primary_key=True),
        Column("region", String),
        Column("total_balance", Float),
        Column("total_instruments", Integer),
        Column("avg_weighted_rate", Float),
    ],
    "earnings_by_person_type": lambda: [
        Column("period_date", Date, primary_key=True),
        Column("person_type", String, primary_key=True),
        Column("total_balance", Float),
        Column("total_instruments", Integer),
        Column("avg_weighted_rate", Float),
    ],
    "earnings_by_currency": lambda: [
        Column("period_date", Date, primary_key=True),
        Column("currency", String, primary_key=True),
        Column("total_balance", Float),
        Column("total_instruments", Integer),
        Column("avg_weighted_rate", Float),
    ],
}


def load_gold_data(
    df: pd.DataFrame,
    table_name: str,
    client: PostgreSqlClient,
    metadata: MetaData,
    parquet_dir: str,
) -> None:
    """Write a gold-layer DataFrame to PostgreSQL and a local Parquet file.

    Persists the aggregated data to two destinations in sequence:
    1. PostgreSQL (gold schema): creates the table if absent, then upserts
       all rows using the table's composite primary key for conflict resolution.
    2. Local Parquet file: writes the DataFrame to parquet_dir/{table_name}.parquet,
       creating parent directories as needed. The flat-file structure is designed
       so that switching to S3 only requires changing parquet_dir to an S3 URI
       and adding the s3fs dependency.

    Args:
        df: Aggregated gold DataFrame produced by one of the transform_gold_* functions.
        table_name: Target table name, must be a key in _TABLE_SCHEMAS
            (e.g. "earnings_by_entity", "earnings_by_currency").
        client: PostgreSQL client used to execute the upsert.
        metadata: SQLAlchemy MetaData instance bound to the gold schema.
        parquet_dir: Local directory path where the Parquet file will be written
            (e.g. "data/gold" or "/app/data/gold" inside Docker).
    """
    # --- PostgreSQL ---
    table = Table(table_name, metadata, *_TABLE_SCHEMAS[table_name]())
    client.upsert(data=df.to_dict("records"), table=table, metadata=metadata)
    logger.info(f"Upserted {len(df)} rows into gold.{table_name}")

    # --- Parquet ---
    parquet_path = Path(parquet_dir) / f"{table_name}.parquet"
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(parquet_path, index=False)
    logger.info(f"Parquet saved to {parquet_path}")
