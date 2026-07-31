"""Tests for the data-quality validator: these must be able to fail a bad dataset."""
import pandas as pd
import pytest

from iptu_pipeline.config import COMMON_COLUMNS
from iptu_pipeline.utils.data_quality import DataQualityValidator

from conftest import make_iptu_df


def thresholds(min_rows=5, max_null=50.0, required=None):
    return {
        "min_rows_per_year": min_rows,
        "max_null_percentage": max_null,
        "required_columns": required if required is not None else COMMON_COLUMNS,
    }


class TestValidDataset:
    def test_valid_dataset_passes(self):
        validator = DataQualityValidator(thresholds=thresholds())
        result = validator.validate_dataset(make_iptu_df(2020), year=2020)
        assert result["passed"], result["errors"]

    def test_engine_argument_accepted(self):
        # Regression: DataQualityValidator(engine=...) used to raise NameError
        # because get_engine was never imported.
        validator = DataQualityValidator(thresholds=thresholds(), engine="pandas")
        result = validator.validate_dataset(make_iptu_df(2021), year=2021)
        assert result["passed"], result["errors"]


class TestFailurePaths:
    def test_empty_dataset_fails(self):
        validator = DataQualityValidator(thresholds=thresholds())
        result = validator.validate_dataset(pd.DataFrame(), year=2020)
        assert not result["passed"]
        assert any("empty" in e.lower() for e in result["errors"])

    def test_row_count_below_threshold_fails(self):
        validator = DataQualityValidator(thresholds=thresholds(min_rows=1000))
        result = validator.validate_dataset(make_iptu_df(2020, rows=8), year=2020)
        assert not result["passed"]
        assert any("below minimum threshold" in e for e in result["errors"])

    def test_missing_required_columns_fail(self):
        validator = DataQualityValidator(thresholds=thresholds())
        df = make_iptu_df(2020).drop(columns=["bairro", "valor IPTU"])
        result = validator.validate_dataset(df, year=2020)
        assert not result["passed"]
        assert any("Missing required columns" in e for e in result["errors"])

    def test_year_mismatch_fails(self):
        validator = DataQualityValidator(thresholds=thresholds())
        df = make_iptu_df(2020)
        result = validator.validate_dataset(df, year=2023)
        assert not result["passed"]
        assert any("Year mismatch" in e for e in result["errors"])


class TestWarningPaths:
    def test_high_null_percentage_warns(self):
        validator = DataQualityValidator(thresholds=thresholds(max_null=10.0))
        df = make_iptu_df(2020)
        df["complemento"] = None  # 100% null
        result = validator.validate_dataset(df, year=2020)
        assert any("null values" in w for w in result["warnings"])

    def test_duplicates_warn_but_do_not_fail(self):
        validator = DataQualityValidator(thresholds=thresholds())
        df = make_iptu_df(2020)
        df = pd.concat([df, df.iloc[[0]]], ignore_index=True)
        result = validator.validate_dataset(df, year=2020)
        assert result["passed"]
        assert any("duplicate" in w.lower() for w in result["warnings"])

    def test_known_renamed_column_downgraded_to_warning(self):
        # 2024 ships "quantidade de pavimentos"; the validator should flag it as
        # a rename candidate (warning), not a hard missing-column error.
        validator = DataQualityValidator(thresholds=thresholds())
        df = make_iptu_df(2024).rename(columns={"quant pavimentos": "quantidade de pavimentos"})
        result = validator.validate_dataset(df, year=2024)
        assert result["passed"], result["errors"]
        assert any("renamed during transformation" in w for w in result["warnings"])

    def test_unexpected_city_warns(self):
        validator = DataQualityValidator(thresholds=thresholds())
        df = make_iptu_df(2020)
        df["cidade"] = "OLINDA"
        result = validator.validate_dataset(df, year=2020)
        assert any("city" in w.lower() for w in result["warnings"])


class TestReporting:
    def test_report_accumulates_all_runs(self, tmp_path):
        validator = DataQualityValidator(thresholds=thresholds())
        validator.validate_dataset(make_iptu_df(2020), year=2020)
        validator.validate_dataset(pd.DataFrame(), year=2021)
        report = validator.generate_validation_report(output_path=tmp_path / "report.csv")
        assert len(report) == 2
        assert report["passed"].tolist() == [True, False]
        assert (tmp_path / "report.csv").exists()

    def test_errors_table_contains_failures(self):
        validator = DataQualityValidator(thresholds=thresholds())
        validator.validate_dataset(pd.DataFrame(), year=2020)
        errors = validator.get_errors_table()
        assert not errors.empty
        assert (errors["error_type"] == "ERROR").any()

    def test_empty_report_returns_empty_frame(self):
        assert DataQualityValidator(thresholds=thresholds()).generate_validation_report().empty


@pytest.mark.parametrize("bad_year_value", [["2020", "2021"], [1999]])
def test_inconsistent_year_values_fail(bad_year_value):
    validator = DataQualityValidator(thresholds=thresholds())
    df = make_iptu_df(2020, rows=8)
    df["ano do exercício"] = (bad_year_value * 8)[:8]
    result = validator.validate_dataset(df, year=2020)
    assert not result["passed"]
