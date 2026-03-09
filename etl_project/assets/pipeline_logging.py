from datetime import datetime, timezone

from loguru import logger
from sqlalchemy import MetaData, Table, Column, Integer, String, Text, DateTime

from connectors.postgresql import PostgreSqlClient


def _get_table(metadata: MetaData) -> Table:
    """Return the SQLAlchemy Table definition for config.pipeline_runs.

    Uses the provided MetaData (bound to the config schema) so the table is
    created in the correct schema without hard-coding a schema prefix.

    Args:
        metadata: SQLAlchemy MetaData bound to the config schema.

    Returns:
        SQLAlchemy Table object for pipeline_runs.
    """
    return Table(
        "pipeline_runs",
        metadata,
        Column("run_id", Integer, primary_key=True, autoincrement=True),
        Column("pipeline_name", String(100), nullable=False),
        Column("start_time", DateTime, nullable=False),
        Column("end_time", DateTime),
        Column("status", String(20)),
        Column("records_extracted", Integer),
        Column("records_loaded", Integer),
        Column("error_message", Text),
        extend_existing=True,
    )


def start_pipeline_run(
    client: PostgreSqlClient,
    metadata: MetaData,
    pipeline_name: str,
) -> int:
    """Insert a new pipeline_runs row and return its run_id.

    Creates the pipeline_runs table if it does not exist, then inserts a row
    with status='running' and the current UTC timestamp. The returned run_id
    must be passed to finish_pipeline_run or fail_pipeline_run at the end of
    the pipeline execution.

    Args:
        client: Active PostgreSQL client used to execute the INSERT.
        metadata: SQLAlchemy MetaData bound to the config schema.
        pipeline_name: Human-readable name for the pipeline
            (e.g. \"earnings_pipeline\").

    Returns:
        Integer run_id of the newly inserted row.
    """
    table = _get_table(metadata)
    metadata.create_all(client.engine)

    row = {
        "pipeline_name": pipeline_name,
        "start_time": datetime.now(timezone.utc).replace(tzinfo=None),
        "status": "running",
    }

    with client.engine.connect() as conn:
        result = conn.execute(table.insert().values(row).returning(table.c.run_id))
        run_id = result.scalar()
        conn.commit()

    logger.info(f"[META] Pipeline run started — run_id={run_id}")
    return run_id


def finish_pipeline_run(
    client: PostgreSqlClient,
    metadata: MetaData,
    run_id: int,
    records_extracted: int,
    records_loaded: int,
) -> None:
    """Mark a pipeline run as successful in config.pipeline_runs.

    Updates the row identified by run_id with status='success', the current
    UTC end_time, and the record counts for the completed execution.

    Args:
        client: Active PostgreSQL client used to execute the UPDATE.
        metadata: SQLAlchemy MetaData bound to the config schema.
        run_id: Primary key of the row created by start_pipeline_run.
        records_extracted: Total records pulled from the source API.
        records_loaded: Total records written to the target (silver layer count).
    """
    table = _get_table(metadata)

    update = {
        "end_time": datetime.now(timezone.utc).replace(tzinfo=None),
        "status": "success",
        "records_extracted": records_extracted,
        "records_loaded": records_loaded,
    }

    with client.engine.connect() as conn:
        conn.execute(table.update().where(table.c.run_id == run_id).values(update))
        conn.commit()

    logger.info(
        f"[META] Run {run_id} finished — status=success, "
        f"extracted={records_extracted}, loaded={records_loaded}"
    )


def fail_pipeline_run(
    client: PostgreSqlClient,
    metadata: MetaData,
    run_id: int,
    error_message: str,
) -> None:
    """Mark a pipeline run as failed in config.pipeline_runs.

    Updates the row identified by run_id with status='failed', the current
    UTC end_time, and the error message captured from the exception. Called
    from the except block of the pipeline's top-level try/except.

    Args:
        client: Active PostgreSQL client used to execute the UPDATE.
        metadata: SQLAlchemy MetaData bound to the config schema.
        run_id: Primary key of the row created by start_pipeline_run.
        error_message: String representation of the exception or failure reason.
    """
    table = _get_table(metadata)

    update = {
        "end_time": datetime.now(timezone.utc).replace(tzinfo=None),
        "status": "failed",
        "error_message": str(error_message)[:2000],
    }

    with client.engine.connect() as conn:
        conn.execute(table.update().where(table.c.run_id == run_id).values(update))
        conn.commit()

    logger.error(f"[META] Run {run_id} failed — error logged to pipeline_runs")
