"""Tests for data ingestion, including failure paths and the validation gate."""
import pytest

from iptu_pipeline.pipelines.ingestion import DataIngestion
from iptu_pipeline.utils.data_quality import DataQualityValidator


class TestLoadYearData:
    def test_unknown_year_raises_value_error(self):
        ingestion = DataIngestion(engine="pandas")
        with pytest.raises(ValueError, match="not found in data paths"):
            ingestion.load_year_data(1999)

    def test_missing_file_raises_file_not_found(self):
        # 2023 is configured but its raw file has not been seeded in this test
        ingestion = DataIngestion(engine="pandas")
        from iptu_pipeline.config import DATA_PATHS
        if DATA_PATHS[2023].exists():
            pytest.skip("raw file exists from a prior fixture; cannot test missing path")
        with pytest.raises(FileNotFoundError):
            ingestion.load_year_data(2023)

    def test_load_csv_year(self, seeded_raw_data):
        ingestion = DataIngestion(engine="pandas")
        df = ingestion.load_year_data(2020, validate=False)
        assert len(df) == 8
        assert "bairro" in df.columns
        assert 2020 in ingestion.ingested_years

    def test_load_json_year_2024(self, seeded_raw_data):
        ingestion = DataIngestion(engine="pandas")
        df = ingestion.load_year_data(2024, validate=False)
        assert len(df) == 8
        # 2024 layout: _id present, pavimentos under its 2024 name
        assert "_id" in df.columns
        assert "quantidade de pavimentos" in df.columns


class TestValidationGate:
    def test_failed_validation_does_not_block_ingestion(self, seeded_raw_data):
        """Documents current behaviour: a failed quality check logs a warning
        but ingestion continues. The pipeline has no hard quality gate."""
        strict = DataQualityValidator(
            thresholds={"min_rows_per_year": 10_000, "max_null_percentage": 50.0, "required_columns": []}
        )
        ingestion = DataIngestion(quality_validator=strict, engine="pandas")
        df = ingestion.load_year_data(2020, validate=True)  # must not raise
        assert len(df) == 8
        assert not strict.validation_results[-1]["passed"]


class TestLoadAllYears:
    def test_loads_every_configured_year(self, seeded_raw_data):
        ingestion = DataIngestion(engine="pandas")
        frames = ingestion.load_all_years(validate=False)
        assert sorted(frames.keys()) == [2020, 2021, 2022, 2023, 2024]

    def test_status_tracking(self, seeded_raw_data):
        ingestion = DataIngestion(engine="pandas")
        ingestion.load_year_data(2020, validate=False)
        status = ingestion.get_ingestion_status()
        assert status["ingested_years"] == [2020]
        assert 2021 in status["pending_years"]


class TestLoadIncremental:
    def test_default_path_none_does_not_crash(self, seeded_raw_data):
        # Regression: load_incremental used to dereference None when no
        # existing_data_path was given (the default was commented out).
        ingestion = DataIngestion(engine="pandas")
        frames = ingestion.load_incremental([2020])
        assert 2020 in frames

    def test_existing_years_are_skipped(self, seeded_raw_data, tmp_path):
        import pandas as pd
        existing = pd.DataFrame({"ano do exercício": [2020, 2021]})
        existing_path = tmp_path / "existing.parquet"
        existing.to_parquet(existing_path)
        ingestion = DataIngestion(engine="pandas")
        frames = ingestion.load_incremental([2020, 2022], existing_data_path=existing_path)
        assert 2020 not in frames
        assert 2022 in frames
