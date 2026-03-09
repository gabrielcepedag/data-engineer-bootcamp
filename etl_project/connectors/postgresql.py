from sqlalchemy import create_engine, Table, MetaData, Column
from sqlalchemy import text
from sqlalchemy.engine import URL, Engine
from sqlalchemy.dialects import postgresql
from loguru import logger

class PostgreSqlClient:
    '''
    A client for querying a PostgreSQL database.
    '''

    def __init__(
        self,
        server_name: str,
        database_name: str,
        username: str,
        password: str,
        port: int = 5432
    ):
        self.host_name = server_name
        self.database_name = database_name
        self.username = username
        self.password = password
        self.port = port

        connection_url = URL.create(
            drivername="postgresql+pg8000",
            username=username,
            password=password,
            host=server_name,
            port=port,
            database=database_name
        )

        self.engine = create_engine(connection_url)

    def read(self, query: str):
        import pandas as pd
        with self.engine.connect() as conn:
            return pd.read_sql(text(query), conn)
    
    def create_table(table_name: str, metadata: MetaData, engine: Engine):
        try:
            existing_table = metadata.tables[table_name]
            new_metadata = MetaData()
            columns = [
                Column(column.name, column.type, primary_key=column.primary_key)
                for column in existing_table.columns
            ]
            new_table = Table(table_name, new_metadata, *columns)
            new_metadata.create_all(bind=engine)
            return new_metadata
        except Exception as e:
            logger.error(e)

    def upsert(self, data: list[dict], table: Table, metadata: MetaData) -> None:
        try:
            metadata.create_all(self.engine)
            key_columns = [
                pk_column.name for pk_column in table.primary_key.columns.values()
            ]
            insert_statement = postgresql.insert(table).values(data)
            upsert_statement = insert_statement.on_conflict_do_update(
                index_elements=key_columns,
                set_={
                    c.key: c for c in insert_statement.excluded if c.key not in key_columns
                }
            )
            with self.engine.connect() as conn:
                conn.execute(upsert_statement)
                conn.commit()
        except Exception as e:
            logger.error(e)
