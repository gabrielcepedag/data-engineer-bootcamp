import pandas as pd
from pathlib import Path
from loguru import logger
from sqlalchemy import MetaData, Table, Column, String, Float, Integer, Date

from connectors.postgresql import PostgreSqlClient


def _aggregate(df: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    """Aggregate silver earnings data by the given group columns.

    Computes total_balance, total_instruments, and a true portfolio
    weighted average rate (weighted by balance) for each group.
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
    """Aggregate deposits by entity and period.
    Answers: which banks hold the most deposits?
    """
    return _aggregate(df, ["period_date", "entity", "entity_type"])


def transform_gold_by_province(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate deposits by province and period.
    Answers: which provinces concentrate the most savings?
    """
    return _aggregate(df, ["period_date", "province", "region"])


def transform_gold_by_person_type(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate deposits by person type and period.
    Answers: how do retail (Natural) vs. corporate (Juridical) deposits compare?
    """
    return _aggregate(df, ["period_date", "person_type"])


def transform_gold_by_currency(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate deposits by currency and period.
    Answers: what is the dollarization level of deposits?
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
    """Write a gold DataFrame to two destinations:
    1. PostgreSQL gold schema (upsert)
    2. Local Parquet file (ready for future S3 migration)
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
