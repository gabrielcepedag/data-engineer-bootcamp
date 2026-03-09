from loguru import logger
from datetime import date
from sqlalchemy import MetaData, Table, Column, Integer, String, Float

from connectors.sb_api import SBApiClient
from connectors.postgresql import PostgreSqlClient
import pandas as pd

def extract_earnings_data(
        sb_api_clients: SBApiClient,
        entity_types: tuple[str],
        start_date: date,
        records_per_page: int
    ) -> list:
    """Extract earnings data from the SB API for one or more entity types.

    Paginates through the API until a partial page is received, which signals
    the end of available data. If the API returns an empty or error response
    for any page, pagination stops for that entity type and a warning is logged.

    Args:
        sb_api_clients: Initialized SBApiClient instance.
        entity_types: Sequence of entity type codes to query (e.g. ["BM", "BAyC"]).
        start_date: Earliest period to fetch data from (inclusive).
        records_per_page: Number of records to request per API call.

    Returns:
        List of raw record dicts combining all entity types and all pages.
    """
    data = []
    for entity_type in entity_types:
        current_page = 1
        data_is_empty = False
        while not data_is_empty:
            logger.info(f"Extracting page {current_page} for entity_type {entity_type}")

            payload = sb_api_clients.get_earnings_by_location(
                entity_type=entity_type,
                page_number=current_page,
                start_date=start_date,
                total_records=records_per_page
            )

            if not payload:
                logger.warning(f"Empty response for entity_type={entity_type}, page={current_page}. Stopping pagination.")
                data_is_empty = True
                continue

            data.extend(payload)

            if len(payload) < records_per_page:
                data_is_empty = True
            else:
                current_page += 1

    logger.info("API extraction complete.")
    return data


def load_earnings_data(df, client: PostgreSqlClient, metadata: MetaData) -> None:
    """Upsert raw earnings records into the PostgreSQL bronze schema.

    Creates the bronze.earnings table if it does not exist, then performs an
    upsert keyed on the 5-column composite primary key:
    periodo, entidad, provincia, persona, divisa.

    Column names remain in the original Spanish/camelCase format as received
    from the API — no renaming is done at this layer.

    Args:
        df: List of raw record dicts as returned by extract_earnings_data.
        client: PostgreSQL client used to execute the upsert.
        metadata: SQLAlchemy MetaData instance bound to the bronze schema.
    """
    earnings_table = Table(
        "earnings",
        metadata,
        Column("periodo", String, primary_key=True),
        Column("entidad", String, primary_key=True),
        Column("provincia", String, primary_key=True),
        Column("persona", String, primary_key=True),
        Column("divisa", String, primary_key=True),
        Column("tipoEntidad", String),
        Column("region", String),
        Column("codIso", String),
        Column("cantidadInstrumento", Integer),
        Column("balance", Float),
        Column("tasaPromedioPonderadoPorBalance", Float),
        Column("tasaPromedioPonderado", Float),
    )
    client.upsert(data=df, table=earnings_table, metadata=metadata)


def read_bronze_earnings_data(client: PostgreSqlClient, schema: str) -> pd.DataFrame | None:
    """
    Reads earnings data from the bronze layer in PostgreSQL and returns a Pandas DataFrame.
    
    Args:
        client (PostgreSqlClient): The PostgreSQL client instance with engine.
        schema (str): The schema name (e.g., 'bronze').
    
    Returns:
        pd.DataFrame | None: DataFrame with bronze data, or None if error or empty.
    """
    try:
        query = f"SELECT * FROM {schema}.earnings"
        df = client.read(query)
        if df.empty:
            logger.warning(f"No data found in {schema}.earnings.")
            return None
        return df
    except Exception as e:
        logger.error(f"Error reading from bronze: {e}")
        return None