"""Tests for the analysis layer (Pandas engine): numbers must add up."""
import pandas as pd
import pytest

from iptu_pipeline.pipelines.analysis import IPTUAnalyzer

from conftest import make_iptu_df


@pytest.fixture()
def multi_year_df():
    return pd.concat(
        [make_iptu_df(2020, rows=8), make_iptu_df(2021, rows=6)],
        ignore_index=True,
    )


class TestVolumeAnalysis:
    def test_total_matches_input(self, multi_year_df):
        analyzer = IPTUAnalyzer(multi_year_df, engine="pandas")
        results = analyzer.analyze_volume_total()
        assert results["total_properties"]["value"].iloc[0] == 14

    def test_volume_by_year_counts(self, multi_year_df):
        analyzer = IPTUAnalyzer(multi_year_df, engine="pandas")
        results = analyzer.analyze_volume_total()
        by_year = results["volume_by_year"].set_index("ano")["total_imoveis"]
        assert by_year[2020] == 8
        assert by_year[2021] == 6

    def test_volume_by_type_percentages_sum_to_100(self, multi_year_df):
        analyzer = IPTUAnalyzer(multi_year_df, engine="pandas")
        results = analyzer.analyze_volume_total()
        assert results["volume_by_type"]["percentual"].sum() == pytest.approx(100.0, abs=0.1)

    def test_volume_by_neighborhood_totals(self, multi_year_df):
        analyzer = IPTUAnalyzer(multi_year_df, engine="pandas")
        results = analyzer.analyze_volume_total()
        assert results["volume_by_neighborhood"]["total_imoveis"].sum() == 14


class TestAnalyzerInput:
    def test_rejects_unsupported_input(self):
        with pytest.raises(ValueError):
            IPTUAnalyzer([1, 2, 3], engine="pandas")

    def test_copy_semantics_do_not_mutate_input(self, multi_year_df):
        before = multi_year_df.copy()
        analyzer = IPTUAnalyzer(multi_year_df, engine="pandas")
        analyzer.analyze_volume_total()
        pd.testing.assert_frame_equal(multi_year_df, before)
