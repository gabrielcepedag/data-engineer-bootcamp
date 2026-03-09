import pandas as pd
from datetime import date
from unittest.mock import MagicMock

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from assets.earnings_data import extract_earnings_data, load_earnings_data, read_bronze_earnings_data


SAMPLE_RECORD = {
    "periodo": "2026-01",
    "entidad": "Banco Popular",
    "provincia": "Santo Domingo",
    "persona": "Natural",
    "divisa": "DOP",
    "tipoEntidad": "BM",
    "region": "Ozama",
    "codIso": "DO",
    "cantidadInstrumento": 100,
    "balance": 5000.0,
    "tasaPromedioPonderadoPorBalance": 8.5,
    "tasaPromedioPonderado": 9.0,
}


# ---------------------------------------------------------------------------
# extract_earnings_data
# ---------------------------------------------------------------------------

class TestExtractEarningsData:
    def test_single_page_returns_all_records(self):
        # Goal: when the API returns fewer records than records_per_page,
        # all records are included in the result without requesting additional pages.

        # Arrange
        mock_client = MagicMock()
        mock_client.get_earnings_by_location.return_value = [SAMPLE_RECORD, SAMPLE_RECORD]

        # Act
        result = extract_earnings_data(mock_client, ["BM"], date(2026, 1, 1), records_per_page=500)

        # Assert
        assert len(result) == 2

    def test_paginates_until_partial_page(self):
        # Goal: the function keeps requesting pages while receiving full pages,
        # and stops as soon as it receives a partial one.

        # Arrange
        page1 = [SAMPLE_RECORD] * 3
        page2 = [SAMPLE_RECORD]  # partial page -> stop
        mock_client = MagicMock()
        mock_client.get_earnings_by_location.side_effect = [page1, page2]

        # Act
        result = extract_earnings_data(mock_client, ["BM"], date(2026, 1, 1), records_per_page=3)

        # Assert
        assert mock_client.get_earnings_by_location.call_count == 2
        assert len(result) == 4

    def test_empty_api_response_returns_empty_list(self):
        # Goal: when the API returns no data, the function returns an empty list
        # without raising exceptions.

        # Arrange
        mock_client = MagicMock()
        mock_client.get_earnings_by_location.return_value = []

        # Act
        result = extract_earnings_data(mock_client, ["BM"], date(2026, 1, 1), records_per_page=500)

        # Assert
        assert result == []

    def test_multiple_entity_types_combined(self):
        # Goal: when multiple entity types are provided, the function queries the API
        # for each one and combines all records into a single result.

        # Arrange
        mock_client = MagicMock()
        mock_client.get_earnings_by_location.side_effect = [[SAMPLE_RECORD], [SAMPLE_RECORD]]

        # Act
        result = extract_earnings_data(mock_client, ["BM", "BAyC"], date(2026, 1, 1), records_per_page=500)

        # Assert
        assert len(result) == 2
        assert mock_client.get_earnings_by_location.call_count == 2


# ---------------------------------------------------------------------------
# load_earnings_data
# ---------------------------------------------------------------------------

class TestLoadEarningsData:
    def test_table_name_is_earnings(self):
        # Goal: the table passed to upsert must be named "earnings",
        # which is the bronze table where raw API data is stored.

        # Arrange
        mock_client = MagicMock()
        mock_metadata = MagicMock()

        # Act
        load_earnings_data(df=[SAMPLE_RECORD], client=mock_client, metadata=mock_metadata)

        # Assert
        table = mock_client.upsert.call_args.kwargs["table"]
        assert table.name == "earnings"

    def test_primary_key_columns(self):
        # Goal: the table must define the correct 5-column composite primary key
        # used for upsert conflict resolution in the bronze layer.

        # Arrange
        mock_client = MagicMock()
        mock_metadata = MagicMock()

        # Act
        load_earnings_data(df=[SAMPLE_RECORD], client=mock_client, metadata=mock_metadata)

        # Assert
        table = mock_client.upsert.call_args.kwargs["table"]
        pk_columns = {col.name for col in table.primary_key.columns}
        assert pk_columns == {"periodo", "entidad", "provincia", "persona", "divisa"}


# ---------------------------------------------------------------------------
# read_bronze_earnings_data
# ---------------------------------------------------------------------------

class TestReadBronzeEarningsData:
    def test_returns_dataframe_with_data(self):
        # Goal: when the bronze table contains rows, the function returns
        # a DataFrame with all the records read.

        # Arrange
        mock_client = MagicMock()
        mock_client.read.return_value = pd.DataFrame([SAMPLE_RECORD])

        # Act
        result = read_bronze_earnings_data(mock_client, schema="bronze")

        # Assert
        assert result is not None
        assert len(result) == 1

    def test_returns_none_when_empty(self):
        # Goal: when the bronze table has no rows, the function returns None
        # instead of an empty DataFrame so the pipeline can stop gracefully.

        # Arrange
        mock_client = MagicMock()
        mock_client.read.return_value = pd.DataFrame()

        # Act
        result = read_bronze_earnings_data(mock_client, schema="bronze")

        # Assert
        assert result is None

    def test_returns_none_on_exception(self):
        # Goal: if any error occurs while reading (lost connection, missing table, etc.),
        # the function catches it and returns None without propagating the exception.

        # Arrange
        mock_client = MagicMock()
        mock_client.read.side_effect = Exception("Connection error")

        # Act
        result = read_bronze_earnings_data(mock_client, schema="bronze")

        # Assert
        assert result is None
