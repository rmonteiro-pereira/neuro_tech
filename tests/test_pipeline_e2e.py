"""End-to-end medallion pipeline test on the Pandas engine.

Runs raw -> bronze -> silver -> gold on synthetic data in a temp directory and
verifies each layer boundary: files exist, rows are conserved, layer metadata
is stamped, and the catalog records completion.
"""
import json

import pandas as pd
import pytest

from iptu_pipeline.config import settings
from iptu_pipeline.pipelines.main_pipeline import IPTUPipeline


@pytest.fixture(scope="module")
def pipeline_run(seeded_raw_data):
    pipeline = IPTUPipeline(engine="pandas")
    consolidated = pipeline.run_full_pipeline(
        years=[2020, 2021, 2024],
        run_analysis=False,
        incremental=False,
    )
    return pipeline, consolidated


class TestBronzeLayer:
    def test_bronze_files_written_per_year(self, pipeline_run):
        for year in (2020, 2021, 2024):
            assert (settings.BRONZE_DIR / f"iptu_{year}" / "data.parquet").exists()

    def test_bronze_metadata_stamped(self, pipeline_run):
        df = pd.read_parquet(settings.BRONZE_DIR / "iptu_2020" / "data.parquet")
        assert "_bronze_ingestion_timestamp" in df.columns
        assert (df["_year_partition"] == 2020).all()

    def test_bronze_2024_schema_normalized(self, pipeline_run):
        # The 2024 JSON ships "quantidade de pavimentos"; bronze must expose the
        # common-schema name.
        df = pd.read_parquet(settings.BRONZE_DIR / "iptu_2024" / "data.parquet")
        assert "quant pavimentos" in df.columns


class TestSilverLayer:
    def test_silver_consolidated_written(self, pipeline_run):
        assert (settings.SILVER_DIR / "iptu_silver_consolidated" / "data.parquet").exists()

    def test_rows_conserved_across_consolidation(self, pipeline_run):
        _, consolidated = pipeline_run
        assert len(consolidated) == 24  # 8 rows for each of the 3 years

    def test_all_years_present(self, pipeline_run):
        _, consolidated = pipeline_run
        years = sorted(pd.to_numeric(consolidated["ano do exercício"]).unique().tolist())
        assert years == [2020, 2021, 2024]

    def test_no_duplicate_or_corrupted_columns(self, pipeline_run):
        _, consolidated = pipeline_run
        assert not consolidated.columns.duplicated().any()
        assert not any(str(c).startswith("col-") and len(str(c)) > 40 for c in consolidated.columns)

    def test_key_columns_survive(self, pipeline_run):
        _, consolidated = pipeline_run
        for col in ("valor IPTU", "ano do exercício", "bairro"):
            assert col in consolidated.columns


class TestGoldLayer:
    def test_consolidated_gold_parquet_written(self, pipeline_run):
        assert (settings.gold_parquet_dir / "iptu_consolidated.parquet").exists()

    def test_gold_summary_by_year_type(self, pipeline_run):
        path = settings.gold_parquet_dir / "gold_summary_by_year_type.parquet"
        assert path.exists()
        gold = pd.read_parquet(path)
        # Aggregation must account for every input row
        assert gold["total_imoveis"].sum() == 24


class TestCatalog:
    def test_catalog_marks_years_completed(self, pipeline_run):
        catalog_path = settings.CATALOG_DIR / "data_catalog.json"
        assert catalog_path.exists()
        entries = {e["year"]: e for e in json.loads(catalog_path.read_text(encoding="utf-8"))}
        for year in (2020, 2021, 2024):
            assert entries[year]["processing_status"] == "completed"


class TestIdempotence:
    def test_second_run_produces_same_silver_row_count(self, pipeline_run, seeded_raw_data):
        pipeline = IPTUPipeline(engine="pandas")
        consolidated = pipeline.run_full_pipeline(
            years=[2020, 2021, 2024], run_analysis=False, incremental=False
        )
        assert len(consolidated) == 24
