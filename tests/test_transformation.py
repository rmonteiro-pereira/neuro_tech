"""Tests for schema normalization and cleaning (the bronze-layer transformations)."""
import pandas as pd

from iptu_pipeline.config import COMMON_COLUMNS
from iptu_pipeline.pipelines.transformation import DataTransformer

from conftest import make_iptu_df


def transformer() -> DataTransformer:
    return DataTransformer(engine="pandas")


class TestNormalizeColumnNames:
    def test_2024_pavimentos_renamed(self):
        df = make_iptu_df(2024).rename(columns={"quant pavimentos": "quantidade de pavimentos"})
        out = transformer().normalize_column_names(df, year=2024)
        assert "quant pavimentos" in out.columns
        assert "quantidade de pavimentos" not in out.columns

    def test_other_years_untouched(self):
        df = make_iptu_df(2020)
        out = transformer().normalize_column_names(df, year=2020)
        assert list(out.columns) == list(df.columns)


class TestHandleMissingColumns:
    def test_adds_all_missing_common_columns(self):
        df = make_iptu_df(2020).drop(columns=["complemento", "fração ideal"])
        out, fuzzy = transformer().handle_missing_columns(df, year=2020)
        assert set(COMMON_COLUMNS).issubset(set(out.columns))
        assert fuzzy == {}

    def test_known_rename_applied_instead_of_null_fill(self):
        df = make_iptu_df(2020).rename(columns={"valor IPTU": "valor cobrado de IPTU"})
        out, fuzzy = transformer().handle_missing_columns(df, year=2020)
        assert fuzzy == {"valor cobrado de IPTU": "valor IPTU"}
        # The renamed column keeps its data instead of becoming all-null
        assert out["valor IPTU"].notna().all()

    def test_extra_columns_preserved(self):
        df = make_iptu_df(2024)
        df["_id"] = range(len(df))
        out, _ = transformer().handle_missing_columns(df, year=2024)
        assert "_id" in out.columns


class TestStandardizeDataTypes:
    def test_numeric_strings_coerced(self):
        df = make_iptu_df(2020)
        out = transformer().standardize_data_types(df, year=2020)
        assert pd.api.types.is_numeric_dtype(out["quant pavimentos"])
        assert pd.api.types.is_numeric_dtype(out["CEP"])

    def test_invalid_numeric_becomes_nan(self):
        df = make_iptu_df(2020)
        df.loc[0, "numero"] = "not-a-number"
        out = transformer().standardize_data_types(df, year=2020)
        assert pd.isna(out.loc[0, "numero"])

    def test_date_column_normalized(self):
        df = make_iptu_df(2020)
        out = transformer().standardize_data_types(df, year=2020)
        assert pd.api.types.is_datetime64_any_dtype(out["data do cadastramento"])
        # Time component removed
        parsed = out["data do cadastramento"].dropna()
        assert (parsed.dt.hour == 0).all()


class TestCleanAndOptimize:
    def test_whitespace_trimmed(self):
        df = make_iptu_df(2020)  # logradouro has trailing spaces on purpose
        out = transformer().clean_and_optimize(df)
        assert (out["logradouro"].astype(str) == out["logradouro"].astype(str).str.strip()).all()

    def test_low_cardinality_becomes_categorical(self):
        df = make_iptu_df(2020)
        out = transformer().clean_and_optimize(df)
        assert str(out["cidade"].dtype) == "category"

    def test_row_count_unchanged(self):
        df = make_iptu_df(2020)
        assert len(transformer().clean_and_optimize(df)) == len(df)


class TestTransformYearData:
    def test_full_transform_produces_common_schema(self):
        df = make_iptu_df(2024).rename(columns={"quant pavimentos": "quantidade de pavimentos"})
        out, _ = transformer().transform_year_data(df, year=2024)
        assert set(COMMON_COLUMNS).issubset(set(out.columns))
        assert len(out) == len(df)


class TestConsolidateDatasets:
    def test_multi_year_consolidation_preserves_rows(self):
        frames = {year: make_iptu_df(year, rows=6) for year in (2020, 2021)}
        out = transformer().consolidate_datasets(frames)
        assert len(out) == 12
        assert sorted(out["ano do exercício"].unique().tolist()) == [2020, 2021]
