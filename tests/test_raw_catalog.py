"""Tests for the raw-file catalog (metadata tracking ahead of the bronze layer)."""
import json

from iptu_pipeline.config import settings
from iptu_pipeline.utils.raw_catalog import RawDataCatalog


class TestScan:
    def test_scan_catalogs_seeded_files(self, seeded_raw_data):
        catalog = RawDataCatalog(engine="pandas")
        discovered = catalog.scan_raw_files()
        assert sorted(discovered.keys()) == [2020, 2021, 2022, 2023, 2024]
        entry = discovered[2020]
        assert entry["file_type"] == "csv"
        assert entry["file_size_bytes"] > 0
        assert len(entry["md5_checksum"]) == 32
        assert entry["processing_status"] in ("pending", "processing", "completed")
        # Schema snapshot captured from the file head
        schema = json.loads(entry["schema_snapshot"])
        assert "bairro" in schema["columns"]

    def test_row_count_counts_csv_lines(self, seeded_raw_data):
        catalog = RawDataCatalog(engine="pandas")
        discovered = catalog.scan_raw_files()
        assert discovered[2020]["row_count"] == 8  # header excluded


class TestLifecycle:
    def test_status_transitions(self, seeded_raw_data):
        catalog = RawDataCatalog(engine="pandas")
        catalog.scan_raw_files()

        catalog.mark_as_processing(2020, bronze_path="bronze/iptu_2020")
        assert catalog.get_file_metadata(2020)["processing_status"] == "processing"

        catalog.mark_as_completed(2020)
        assert catalog.get_file_metadata(2020)["processing_status"] == "completed"

        catalog.mark_as_failed(2021, error_message="boom")
        meta = catalog.get_file_metadata(2021)
        assert meta["processing_status"] == "failed"
        assert meta["error_message"] == "boom"

    def test_unknown_year_is_ignored_gracefully(self):
        catalog = RawDataCatalog(engine="pandas")
        catalog.mark_as_completed(1999)  # must not raise

    def test_pending_files_filter(self, seeded_raw_data):
        catalog = RawDataCatalog(engine="pandas")
        catalog.scan_raw_files()
        catalog.mark_as_completed(2020)
        pending = catalog.get_pending_files()
        assert 2020 not in pending


class TestPersistence:
    def test_save_and_reload_json_catalog(self, seeded_raw_data):
        catalog = RawDataCatalog(engine="pandas")
        catalog.scan_raw_files()
        catalog.mark_as_completed(2020, bronze_path="bronze/iptu_2020")
        catalog.save_catalog()

        json_path = settings.CATALOG_DIR / "data_catalog.json"
        assert json_path.exists()
        # The JSON catalog is a list of entries
        data = json.loads(json_path.read_text(encoding="utf-8"))
        assert isinstance(data, list)

        # A new instance loads the persisted state
        reloaded = RawDataCatalog(engine="pandas")
        assert reloaded.get_file_metadata(2020)["processing_status"] == "completed"


class TestSummary:
    def test_summary_counts(self, seeded_raw_data):
        catalog = RawDataCatalog(engine="pandas")
        catalog.scan_raw_files()
        summary = catalog.get_catalog_summary()
        assert summary["total_files"] == 5
        assert summary["by_file_type"] == {"csv": 4, "json": 1}
        assert summary["total_size_bytes"] > 0

    def test_empty_catalog_summary(self, tmp_path, monkeypatch):
        catalog = RawDataCatalog(engine="pandas")
        catalog._catalog = {}
        assert catalog.get_catalog_summary()["total_files"] == 0
