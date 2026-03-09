import pandas as pd
from datetime import date
from loguru import logger
from sqlalchemy import text

from connectors.postgresql import PostgreSqlClient


def get_param(client: PostgreSqlClient, param_name: str) -> str | None:
    """Read a single parameter value from the config.params table.

    Queries config.params by param_name and returns the stored string value.
    Returns None if the row does not exist or if param_value is SQL NULL,
    allowing callers to apply their own defaults.

    Args:
        client: Active PostgreSQL client used to execute the SELECT query.
        param_name: Primary key of the parameter to retrieve
            (e.g. "last_pipeline_execution", "records_per_page").

    Returns:
        The param_value as a string, or None if not found or NULL.
    """
    df = client.read(
        f"SELECT param_value FROM config.params WHERE param_name = '{param_name}'"
    )
    if df.empty or pd.isna(df["param_value"].iloc[0]):
        return None
    return str(df["param_value"].iloc[0])


def set_param(client: PostgreSqlClient, param_name: str, param_value: str) -> None:
    """Update a parameter value in the config.params table.

    Executes a parameterized UPDATE statement and commits the transaction.
    The row identified by param_name must already exist in config.params —
    this function does not insert new rows.

    Args:
        client: Active PostgreSQL client whose engine is used to open a connection.
        param_name: Primary key of the parameter to update.
        param_value: New value to store. Will be coerced to string before saving.
    """
    with client.engine.connect() as conn:
        conn.execute(
            text("UPDATE config.params SET param_value = :value WHERE param_name = :name"),
            {"value": str(param_value), "name": param_name},
        )
        conn.commit()
    logger.info(f"Config '{param_name}' updated to '{param_value}'")


def get_last_period(client: PostgreSqlClient) -> date | None:
    """Return the last successfully loaded period from config.params.

    Reads the "last_pipeline_execution" parameter, which is stored as "YYYY-MM",
    and converts it to a Python date representing the first day of that month.
    Returns None when the value is NULL, indicating the pipeline has never run
    and a full load should be performed.

    Args:
        client: Active PostgreSQL client used to read from config.params.

    Returns:
        A date object for the first day of the last loaded month (e.g. date(2026, 1, 1)),
        or None if last_pipeline_execution has never been set.
    """
    value = get_param(client, "last_pipeline_execution")
    if value is None:
        return None
    return date.fromisoformat(value + "-01")  # "2026-01" → date(2026, 1, 1)


def set_last_period(client: PostgreSqlClient, period_date: date) -> None:
    """Persist the last successfully loaded period to config.params.

    Formats the given date as "YYYY-MM" and writes it to the
    "last_pipeline_execution" parameter. Called at the end of each successful
    pipeline run so the next execution can determine its incremental start date.

    Args:
        client: Active PostgreSQL client used to write to config.params.
        period_date: The most recent period_date loaded (e.g. date(2026, 1, 1)).
            Only year and month are stored; the day component is ignored.
    """
    set_param(client, "last_pipeline_execution", period_date.strftime("%Y-%m"))


def get_records_per_page(client: PostgreSqlClient, default: int = 500) -> int:
    """Return the API pagination size configured in config.params.

    Reads the "records_per_page" parameter and returns it as an integer.
    Falls back to the provided default when the value is NULL or the row
    does not exist, ensuring the pipeline can always run without manual setup.

    Args:
        client: Active PostgreSQL client used to read from config.params.
        default: Fallback value used when records_per_page is not configured.
            Defaults to 500.

    Returns:
        Integer number of records to request per API page.
    """
    value = get_param(client, "records_per_page")
    return int(value) if value is not None else default
