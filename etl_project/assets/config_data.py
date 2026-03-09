import pandas as pd
from datetime import date
from loguru import logger
from sqlalchemy import text

from connectors.postgresql import PostgreSqlClient


def get_param(client: PostgreSqlClient, param_name: str) -> str | None:
    """Read a single param value from config.params. Returns None if missing or NULL."""
    df = client.read(
        f"SELECT param_value FROM config.params WHERE param_name = '{param_name}'"
    )
    if df.empty or pd.isna(df["param_value"].iloc[0]):
        return None
    return str(df["param_value"].iloc[0])


def set_param(client: PostgreSqlClient, param_name: str, param_value: str) -> None:
    """Update a param value in config.params."""
    with client.engine.connect() as conn:
        conn.execute(
            text("UPDATE config.params SET param_value = :value WHERE param_name = :name"),
            {"value": str(param_value), "name": param_name},
        )
        conn.commit()
    logger.info(f"Config '{param_name}' updated to '{param_value}'")


def get_last_period(client: PostgreSqlClient) -> date | None:
    """Return the last loaded period as a date (first day of that month), or None if not set."""
    value = get_param(client, "last_pipeline_execution")
    if value is None:
        return None
    return date.fromisoformat(value + "-01")  # "2026-01" → date(2026, 1, 1)


def set_last_period(client: PostgreSqlClient, period_date: date) -> None:
    """Persist the last successfully loaded period to config.params."""
    set_param(client, "last_pipeline_execution", period_date.strftime("%Y-%m"))


def get_records_per_page(client: PostgreSqlClient, default: int = 500) -> int:
    """Return records_per_page from config.params, falling back to default."""
    value = get_param(client, "records_per_page")
    return int(value) if value is not None else default
