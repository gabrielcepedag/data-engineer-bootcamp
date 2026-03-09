import pandas as pd
from datetime import date
from unittest.mock import patch

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from assets.silver_earnings_data import transform_earnings_data, BRONZE_TO_SILVER_COLUMN_MAP


def make_sample_data(**overrides) -> list[dict]:
    base = {
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
    base.update(overrides)
    return [base]


class TestColumnRenaming:
    def test_all_bronze_columns_renamed(self):
        # Goal: all API column names (Spanish/camelCase) must be replaced by their
        # English/snake_case equivalents. Columns where the name does not change
        # (e.g. "region", "balance") are excluded from the rename check.

        # Arrange
        data = make_sample_data()

        # Act
        df = transform_earnings_data(data)

        # Assert
        for bronze_col, silver_col in BRONZE_TO_SILVER_COLUMN_MAP.items():
            assert silver_col in df.columns, f"Expected silver column '{silver_col}' not found"
            if bronze_col != silver_col:
                assert bronze_col not in df.columns, f"Bronze column '{bronze_col}' should have been renamed"


class TestPeriodDateConversion:
    def test_periodo_string_converts_to_date(self):
        # Goal: the "YYYY-MM" string is converted to the first day of the month
        # as a Python date object.

        # Arrange
        data = make_sample_data(periodo="2026-01")

        # Act
        df = transform_earnings_data(data)

        # Assert
        assert df["period_date"].iloc[0] == date(2026, 1, 1)

    def test_different_month_converts_correctly(self):
        # Goal: the conversion works for any month, not just January.
        # Covers two-digit month values (e.g. December).

        # Arrange
        data = make_sample_data(periodo="2025-12")

        # Act
        df = transform_earnings_data(data)

        # Assert
        assert df["period_date"].iloc[0] == date(2025, 12, 1)


class TestWhitespaceStripping:
    def test_leading_trailing_spaces_stripped(self):
        # Goal: leading and trailing whitespace in text fields is removed to prevent
        # duplicate records in the DB caused by values like "Banco Popular" vs "  Banco Popular  ".

        # Arrange
        data = make_sample_data(
            entidad="  Banco Popular  ",
            provincia="  Santo Domingo  ",
            region="  Ozama  ",
        )

        # Act
        df = transform_earnings_data(data)

        # Assert
        assert df["entity"].iloc[0] == "Banco Popular"
        assert df["province"].iloc[0] == "Santo Domingo"
        assert df["region"].iloc[0] == "Ozama"


class TestNullPkRowsDropped:
    def test_row_with_null_entity_is_dropped(self):
        # Goal: rows with a null entity must be removed because "entity"
        # is part of the silver table's primary key.

        # Arrange
        data = make_sample_data() + make_sample_data(entidad=None)

        # Act
        df = transform_earnings_data(data)

        # Assert
        assert len(df) == 1

    def test_row_with_null_province_is_dropped(self):
        # Goal: rows with a null province must be removed because "province"
        # is part of the silver table's primary key.

        # Arrange
        data = make_sample_data() + make_sample_data(provincia=None)

        # Act
        df = transform_earnings_data(data)

        # Assert
        assert len(df) == 1

    def test_null_drop_is_logged(self):
        # Goal: when rows are dropped due to null PK columns, a warning containing
        # "Dropping" must be emitted so the team is aware of data loss.

        # Arrange
        data = make_sample_data() + make_sample_data(entidad=None)

        # Act & Assert
        with patch("assets.silver_earnings_data.logger") as mock_logger:
            transform_earnings_data(data)
            mock_logger.warning.assert_called()
            warning_calls = [str(c) for c in mock_logger.warning.call_args_list]
            assert any("Dropping" in c for c in warning_calls)


class TestNegativeBalanceLogged:
    def test_negative_balance_row_is_kept(self):
        # Goal: rows with a negative balance must NOT be dropped — they are only
        # flagged. The value may be valid from an accounting perspective.

        # Arrange
        data = make_sample_data(balance=-100.0)

        # Act
        df = transform_earnings_data(data)

        # Assert
        assert len(df) == 1
        assert df["balance"].iloc[0] == -100.0

    def test_negative_balance_triggers_warning(self):
        # Goal: a negative balance must emit a warning to alert the team
        # without stopping the pipeline.

        # Arrange
        data = make_sample_data(balance=-100.0)

        # Act & Assert
        with patch("assets.silver_earnings_data.logger") as mock_logger:
            transform_earnings_data(data)
            warning_calls = [str(c) for c in mock_logger.warning.call_args_list]
            assert any("negative balance" in c for c in warning_calls)


class TestRatesOutOfRangeLogged:
    def test_out_of_range_rate_row_is_kept(self):
        # Goal: rows with rates outside [0, 100] must NOT be dropped — the original
        # value from the SB API is preserved in the silver layer.

        # Arrange
        data = make_sample_data(tasaPromedioPonderado=150.0)

        # Act
        df = transform_earnings_data(data)

        # Assert
        assert len(df) == 1
        assert df["weighted_avg_rate"].iloc[0] == 150.0

    def test_out_of_range_rate_triggers_warning(self):
        # Goal: an out-of-range rate must emit a warning so the team can investigate
        # whether it is an API error or a valid edge case.

        # Arrange
        data = make_sample_data(tasaPromedioPonderado=150.0)

        # Act & Assert
        with patch("assets.silver_earnings_data.logger") as mock_logger:
            transform_earnings_data(data)
            warning_calls = [str(c) for c in mock_logger.warning.call_args_list]
            assert any("out-of-range" in c for c in warning_calls)


class TestOutputDataFrameTypes:
    def test_period_date_is_python_date(self):
        # Goal: period_date must be a Python date object, not a string or pandas Timestamp,
        # to be compatible with the DATE type in the silver PostgreSQL table.

        # Arrange
        data = make_sample_data()

        # Act
        df = transform_earnings_data(data)

        # Assert
        assert isinstance(df["period_date"].iloc[0], date)

    def test_instrument_count_is_numeric(self):
        # Goal: instrument_count must have an integer dtype,
        # consistent with the INTEGER column defined in the silver table.

        # Arrange
        data = make_sample_data()

        # Act
        df = transform_earnings_data(data)

        # Assert
        assert pd.api.types.is_integer_dtype(df["instrument_count"])

    def test_balance_is_float(self):
        # Goal: balance must have a float dtype,
        # consistent with the FLOAT column defined in the silver table.

        # Arrange
        data = make_sample_data()

        # Act
        df = transform_earnings_data(data)

        # Assert
        assert pd.api.types.is_float_dtype(df["balance"])
