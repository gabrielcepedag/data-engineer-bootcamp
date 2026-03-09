import pandas as pd
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

    def read(self, query: str) -> pd.DataFrame:
        """Execute a SQL query and return the results as a Pandas DataFrame.

        Args:
            query: Raw SQL string to execute.

        Returns:
            DataFrame containing all rows returned by the query.
        """
        import pandas as pd
        with self.engine.connect() as conn:
            return pd.read_sql(text(query), conn)
    
    def create_table(table_name: str, metadata: MetaData, engine: Engine):
        """Create a table in the database by cloning its definition from existing metadata.

        Copies column definitions (name, type, primary key) from a table already
        registered in the provided MetaData object and creates it using a fresh
        MetaData instance to avoid schema conflicts.

        Args:
            table_name: Name of the table to create (must exist in metadata.tables).
            metadata: SQLAlchemy MetaData instance that holds the source table definition.
            engine: SQLAlchemy Engine used to execute the CREATE TABLE statement.

        Returns:
            New MetaData instance containing the newly created table, or None on error.
        """
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
        """Insert records into a table, updating existing rows on primary key conflict.

        Creates the target table if it does not exist, then executes a PostgreSQL
        INSERT … ON CONFLICT DO UPDATE statement (upsert) for all provided records.
        Non-primary-key columns are overwritten with the incoming values on conflict.

        Args:
            data: List of dicts where keys match column names in the target table.
            table: SQLAlchemy Table object defining the target schema and primary key.
            metadata: SQLAlchemy MetaData instance used to create the table if absent.
        """
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
