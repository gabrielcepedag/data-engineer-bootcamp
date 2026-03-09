import pandas as pd
import pytest
from datetime import date
from unittest.mock import MagicMock, patch

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from assets.config_data import (
    get_param,
    set_param,
    get_last_period,
    set_last_period,
    get_records_per_page,
)


# ---------------------------------------------------------------------------
# get_param
# ---------------------------------------------------------------------------

class TestGetParam:
    def test_returns_value_when_present(self):
        # Goal: when the param exists and has a non-null value,
        # the function returns it as a string.

        # Arrange
        mock_client = MagicMock()
        mock_client.read.return_value = pd.DataFrame({"param_value": ["500"]})

        # Act
        result = get_param(mock_client, "records_per_page")

        # Assert
        assert result == "500"

    def test_returns_none_when_null(self):
        # Goal: when the param row exists but param_value is NULL,
        # the function returns None so callers can apply a default.

        # Arrange
        mock_client = MagicMock()
        mock_client.read.return_value = pd.DataFrame({"param_value": [None]})

        # Act
        result = get_param(mock_client, "last_pipeline_execution")

        # Assert
        assert result is None

    def test_returns_none_when_row_missing(self):
        # Goal: when no row matches the param_name, the function returns None
        # instead of raising an IndexError.

        # Arrange
        mock_client = MagicMock()
        mock_client.read.return_value = pd.DataFrame({"param_value": []})

        # Act
        result = get_param(mock_client, "nonexistent_param")

        # Assert
        assert result is None


# ---------------------------------------------------------------------------
# set_param
# ---------------------------------------------------------------------------

class TestSetParam:
    def test_executes_update_and_commits(self):
        # Goal: set_param must run an UPDATE statement and commit the transaction
        # so the new value is persisted to the database.

        # Arrange
        mock_client = MagicMock()
        mock_conn = MagicMock()
        mock_client.engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_client.engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        # Act
        set_param(mock_client, "records_per_page", "1000")

        # Assert
        mock_conn.execute.assert_called_once()
        mock_conn.commit.assert_called_once()


# ---------------------------------------------------------------------------
# get_last_period
# ---------------------------------------------------------------------------

class TestGetLastPeriod:
    def test_returns_date_when_set(self):
        # Goal: when last_pipeline_execution is stored as "YYYY-MM",
        # the function returns the first day of that month as a date object.

        # Arrange
        mock_client = MagicMock()
        mock_client.read.return_value = pd.DataFrame({"param_value": ["2026-01"]})

        # Act
        result = get_last_period(mock_client)

        # Assert
        assert result == date(2026, 1, 1)

    def test_returns_none_when_not_set(self):
        # Goal: when last_pipeline_execution is NULL (first run),
        # the function returns None so the pipeline uses the default start date.

        # Arrange
        mock_client = MagicMock()
        mock_client.read.return_value = pd.DataFrame({"param_value": [None]})

        # Act
        result = get_last_period(mock_client)

        # Assert
        assert result is None


# ---------------------------------------------------------------------------
# set_last_period
# ---------------------------------------------------------------------------

class TestSetLastPeriod:
    def test_stores_period_as_yyyy_mm(self):
        # Goal: set_last_period must format the date as "YYYY-MM" before storing,
        # matching the format expected by get_last_period on the next run.

        # Arrange
        mock_client = MagicMock()
        mock_conn = MagicMock()
        mock_client.engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_client.engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        # Act
        set_last_period(mock_client, date(2026, 3, 1))

        # Assert — the execute call must contain "2026-03" as the value
        call_args = mock_conn.execute.call_args
        assert call_args[0][1]["value"] == "2026-03"


# ---------------------------------------------------------------------------
# get_records_per_page
# ---------------------------------------------------------------------------

class TestGetRecordsPerPage:
    def test_returns_int_from_config(self):
        # Goal: when records_per_page is set in config, it must be returned
        # as an integer so the API client receives the correct type.

        # Arrange
        mock_client = MagicMock()
        mock_client.read.return_value = pd.DataFrame({"param_value": ["200"]})

        # Act
        result = get_records_per_page(mock_client)

        # Assert
        assert result == 200
        assert isinstance(result, int)

    def test_returns_default_when_not_set(self):
        # Goal: when records_per_page is NULL in config, the function returns
        # the specified default so the pipeline can still run.

        # Arrange
        mock_client = MagicMock()
        mock_client.read.return_value = pd.DataFrame({"param_value": [None]})

        # Act
        result = get_records_per_page(mock_client, default=500)

        # Assert
        assert result == 500
