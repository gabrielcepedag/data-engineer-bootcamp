import pandas as pd
from loguru import logger
from datetime import date
from sqlalchemy import MetaData, Table, Column, Integer, String, Float

from connectors.sb_api import SBApiClient
from connectors.postgresql import PostgreSqlClient

def extract_earnings_data(
        sb_api_clients: SBApiClient, 
        entity_types: tuple[str], 
        start_date: date,
        records_per_page: int
    ) -> dict:
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

            data.extend(payload)
            
            if len(payload) < records_per_page:
                data_is_empty = True
            else:
                current_page += 1
    
    logger.info("API extraction complete.")
    return data

def load_earnings_data(df: pd.DataFrame, client: PostgreSqlClient, metadata: MetaData):
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