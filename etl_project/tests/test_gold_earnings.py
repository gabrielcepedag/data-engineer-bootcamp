import pandas as pd
import pytest
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from assets.gold_earnings_data import (
    transform_gold_by_entity,
    transform_gold_by_province,
    transform_gold_by_person_type,
    transform_gold_by_currency,
    load_gold_data,
)


SILVER_DATA = [
    {
        "period_date": date(2026, 1, 1),
        "entity": "Banco Popular",
        "entity_type": "BM",
        "province": "Santo Domingo",
        "region": "Ozama",
        "person_type": "Natural",
        "currency": "DOP",
        "instrument_count": 100,
        "balance": 5000.0,
        "weighted_avg_rate_by_balance": 8.0,
        "weighted_avg_rate": 8.0,
        "iso_code": "DO",
    },
    {
        "period_date": date(2026, 1, 1),
        "entity": "Banco BHD",
        "entity_type": "BM",
        "province": "Santiago",
        "region": "Cibao Norte",
        "person_type": "Juridico",
        "currency": "USD",
        "instrument_count": 50,
        "balance": 3000.0,
        "weighted_avg_rate_by_balance": 4.0,
        "weighted_avg_rate": 4.0,
        "iso_code": "DO",
    },
    {
        "period_date": date(2026, 1, 1),
        "entity": "Banco Popular",
        "entity_type": "BM",
        "province": "Santo Domingo",
        "region": "Ozama",
        "person_type": "Juridico",
        "currency": "DOP",
        "instrument_count": 200,
        "balance": 7000.0,
        "weighted_avg_rate_by_balance": 6.0,
        "weighted_avg_rate": 6.0,
        "iso_code": "DO",
    },
]


@pytest.fixture
def silver_df():
    return pd.DataFrame(SILVER_DATA)


# ---------------------------------------------------------------------------
# transform_gold_by_entity
# ---------------------------------------------------------------------------

class TestTransformGoldByEntity:
    def test_output_has_entity_group_columns(self, silver_df):
        # Goal: the result must contain the entity-level group columns
        # so analysts can filter and slice by bank.

        # Arrange / Act
        result = transform_gold_by_entity(silver_df)

        # Assert
        assert "period_date" in result.columns
        assert "entity" in result.columns
        assert "entity_type" in result.columns

    def test_total_balance_is_summed_per_entity(self, silver_df):
        # Goal: balances for the same entity across different dimensions
        # (province, person_type, currency) must be summed into one row.

        # Arrange / Act
        result = transform_gold_by_entity(silver_df)

        # Assert
        banco_popular = result[result["entity"] == "Banco Popular"]["total_balance"].iloc[0]
        assert banco_popular == 5000.0 + 7000.0

    def test_avg_weighted_rate_is_balance_weighted(self, silver_df):
        # Goal: avg_weighted_rate must be the true portfolio rate
        # (weighted by balance), not a simple mean.
        # Banco Popular: (5000*8 + 7000*6) / (5000+7000) = 82000/12000 ≈ 6.833

        # Arrange / Act
        result = transform_gold_by_entity(silver_df)

        # Assert
        rate = result[result["entity"] == "Banco Popular"]["avg_weighted_rate"].iloc[0]
        assert abs(rate - (5000 * 8 + 7000 * 6) / 12000) < 1e-6


# ---------------------------------------------------------------------------
# transform_gold_by_province
# ---------------------------------------------------------------------------

class TestTransformGoldByProvince:
    def test_output_has_province_group_columns(self, silver_df):
        # Goal: the result must contain province and region columns
        # so analysts can slice geographically.

        # Arrange / Act
        result = transform_gold_by_province(silver_df)

        # Assert
        assert "period_date" in result.columns
        assert "province" in result.columns
        assert "region" in result.columns

    def test_total_balance_summed_per_province(self, silver_df):
        # Goal: all rows for the same province must be collapsed into one row
        # with the sum of their balances.

        # Arrange / Act
        result = transform_gold_by_province(silver_df)

        # Assert
        sd_balance = result[result["province"] == "Santo Domingo"]["total_balance"].iloc[0]
        assert sd_balance == 5000.0 + 7000.0


# ---------------------------------------------------------------------------
# transform_gold_by_person_type
# ---------------------------------------------------------------------------

class TestTransformGoldByPersonType:
    def test_output_has_person_type_column(self, silver_df):
        # Goal: result must group by person_type so analysts can compare
        # retail (Natural) vs. corporate (Juridico) deposit volumes.

        # Arrange / Act
        result = transform_gold_by_person_type(silver_df)

        # Assert
        assert "person_type" in result.columns

    def test_splits_natural_and_juridico(self, silver_df):
        # Goal: Natural and Juridico person types must appear as separate rows.

        # Arrange / Act
        result = transform_gold_by_person_type(silver_df)

        # Assert
        person_types = set(result["person_type"].tolist())
        assert "Natural" in person_types
        assert "Juridico" in person_types


# ---------------------------------------------------------------------------
# transform_gold_by_currency
# ---------------------------------------------------------------------------

class TestTransformGoldByCurrency:
    def test_output_has_currency_column(self, silver_df):
        # Goal: result must group by currency so analysts can measure
        # deposit dollarization (DOP vs. USD).

        # Arrange / Act
        result = transform_gold_by_currency(silver_df)

        # Assert
        assert "currency" in result.columns

    def test_splits_dop_and_usd(self, silver_df):
        # Goal: DOP and USD must appear as separate rows.

        # Arrange / Act
        result = transform_gold_by_currency(silver_df)

        # Assert
        currencies = set(result["currency"].tolist())
        assert "DOP" in currencies
        assert "USD" in currencies


# ---------------------------------------------------------------------------
# load_gold_data
# ---------------------------------------------------------------------------

class TestLoadGoldData:
    def test_upsert_is_called(self, silver_df, tmp_path):
        # Goal: load_gold_data must delegate persistence to client.upsert,
        # not write SQL directly.

        # Arrange
        df = transform_gold_by_entity(silver_df)
        mock_client = MagicMock()
        mock_metadata = MagicMock()

        # Act
        load_gold_data(df, "earnings_by_entity", mock_client, mock_metadata, str(tmp_path))

        # Assert
        mock_client.upsert.assert_called_once()

    def test_parquet_file_is_created(self, silver_df, tmp_path):
        # Goal: a Parquet file must be written to the specified directory
        # so the data is available for future S3 upload or local consumption.

        # Arrange
        df = transform_gold_by_entity(silver_df)
        mock_client = MagicMock()
        mock_metadata = MagicMock()

        # Act
        load_gold_data(df, "earnings_by_entity", mock_client, mock_metadata, str(tmp_path))

        # Assert
        assert (tmp_path / "earnings_by_entity.parquet").exists()

    def test_parquet_content_matches_dataframe(self, silver_df, tmp_path):
        # Goal: the Parquet file must contain the same rows as the DataFrame
        # passed to load_gold_data — no data loss during serialization.

        # Arrange
        df = transform_gold_by_entity(silver_df)
        mock_client = MagicMock()
        mock_metadata = MagicMock()

        # Act
        load_gold_data(df, "earnings_by_entity", mock_client, mock_metadata, str(tmp_path))

        # Assert
        reloaded = pd.read_parquet(tmp_path / "earnings_by_entity.parquet")
        assert len(reloaded) == len(df)
        assert list(reloaded.columns) == list(df.columns)
