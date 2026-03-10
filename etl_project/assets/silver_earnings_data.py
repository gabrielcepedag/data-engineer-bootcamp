import pandas as pd
from loguru import logger
from sqlalchemy import MetaData, Table, Column, Integer, String, Float, Date

from connectors.postgresql import PostgreSqlClient

BRONZE_TO_SILVER_COLUMN_MAP = {
    "periodo": "period_date",
    "entidad": "entity",
    "provincia": "province",
    "persona": "person_type",
    "divisa": "currency",
    "tipoEntidad": "entity_type",
    "region": "region",
    "codIso": "iso_code",
    "cantidadInstrumento": "instrument_count",
    "balance": "balance",
    "tasaPromedioPonderadoPorBalance": "weighted_avg_rate_by_balance",
    "tasaPromedioPonderado": "weighted_avg_rate",
}

SILVER_PK_COLUMNS = ["period_date", "entity", "province", "person_type", "currency"]
SILVER_RATE_COLUMNS = ["weighted_avg_rate_by_balance", "weighted_avg_rate"]

def transform_earnings_data(data: list[dict]) -> pd.DataFrame:
    """Transform raw bronze earnings data into the silver layer format.

    Applies the following steps in order:
    1. Strip leading/trailing whitespace from all string columns.
    2. Rename columns from Spanish/camelCase (bronze) to English/snake_case (silver).
    3. Cast period_date from "YYYY-MM" string to Python date (first day of month).
    4. Drop rows with null values in any primary key column and log a warning.
    5. Log a warning (without dropping) for rows with negative balance values.
    6. Log a warning (without dropping) for rows with rate values outside [0, 100].

    Args:
        data: List of raw record dicts as returned by the bronze layer.

    Returns:
        Cleaned and typed DataFrame ready for loading into silver.earnings.
    """
    df = pd.DataFrame(data)

    # Strip whitespace from all string columns
    string_cols = df.select_dtypes(include="object").columns
    df[string_cols] = df[string_cols].apply(lambda col: col.str.strip())

    # Rename columns bronze → silver
    df = df.rename(columns=BRONZE_TO_SILVER_COLUMN_MAP)

    # Convert periodo string "YYYY-MM" → date(YYYY, MM, 1)
    df["period_date"] = pd.to_datetime(df["period_date"], format="%Y-%m").dt.date

    # Validate nulls in PK columns — drop and log
    null_mask = df[SILVER_PK_COLUMNS].isnull().any(axis=1)
    if null_mask.sum() > 0:
        logger.warning(f"Dropping {null_mask.sum()} rows with null values in primary key columns.")
        df = df[~null_mask]

    # Validate balance >= 0 — warn, don't drop
    negative_balance = df["balance"] < 0
    if negative_balance.sum() > 0:
        logger.warning(f"{negative_balance.sum()} rows have negative balance values.")

    # Validate rates in [0, 100] — warn, don't drop
    for rate_col in SILVER_RATE_COLUMNS:
        out_of_range = (df[rate_col] < 0) | (df[rate_col] > 100)
        if out_of_range.sum() > 0:
            logger.warning(f"{out_of_range.sum()} rows have out-of-range values in '{rate_col}'.")

    logger.info(f"Transformation complete. {len(df)} rows ready for silver layer.")
    return df

def load_silver_earnings_data(df: pd.DataFrame, client: PostgreSqlClient, metadata: MetaData) -> None:
    """Upsert a silver earnings DataFrame into the PostgreSQL silver schema.

    Creates the silver.earnings table if it does not exist, then performs an
    upsert (INSERT … ON CONFLICT DO UPDATE) keyed on the 5-column composite
    primary key: period_date, entity, province, person_type, currency.

    Args:
        df: Transformed DataFrame produced by transform_earnings_data.
        client: PostgreSQL client used to execute the upsert.
        metadata: SQLAlchemy MetaData instance bound to the silver schema.
    """
    silver_earnings_table = Table(
        "earnings",
        metadata,
        Column("period_date", Date, primary_key=True),
        Column("entity", String, primary_key=True),
        Column("province", String, primary_key=True),
        Column("person_type", String, primary_key=True),
        Column("currency", String, primary_key=True),
        Column("entity_type", String),
        Column("region", String),
        Column("iso_code", String),
        Column("instrument_count", Integer),
        Column("balance", Float),
        Column("weighted_avg_rate_by_balance", Float),
        Column("weighted_avg_rate", Float),
    )
    client.upsert(data=df.to_dict("records"), table=silver_earnings_table, metadata=metadata)
