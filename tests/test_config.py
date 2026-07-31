"""Tests for configuration handling (env overrides, path coercion, .env support)."""
from pathlib import Path

from iptu_pipeline.config import COMMON_COLUMNS, QUALITY_THRESHOLDS, Settings, settings


class TestSettingsDefaults:
    def test_directories_are_created_on_instantiation(self, tmp_path):
        custom = Settings(DATA_DIR=str(tmp_path / "d"), _env_file=None)
        assert custom.DATA_DIR.exists()

    def test_string_paths_are_coerced_to_path(self, tmp_path):
        custom = Settings(RAW_DIR=str(tmp_path / "raw"), _env_file=None)
        assert isinstance(custom.RAW_DIR, Path)

    def test_default_years(self):
        assert settings.CSV_YEARS == [2020, 2021, 2022, 2023]
        assert settings.JSON_YEARS == [2024]


class TestEnvOverrides:
    def test_env_prefix_override(self, monkeypatch):
        monkeypatch.setenv("IPTU_MAX_NULL_PERCENTAGE", "12.5")
        assert Settings(_env_file=None).MAX_NULL_PERCENTAGE == 12.5

    def test_engine_override(self, monkeypatch):
        monkeypatch.setenv("IPTU_DATA_ENGINE", "pyspark")
        assert Settings(_env_file=None).DATA_ENGINE == "pyspark"

    def test_conftest_redirection_applied(self):
        # The session conftest points settings at a temp dir via IPTU_* env vars;
        # if this fails, tests are writing inside the repository.
        assert "iptu_test_" in str(settings.DATA_DIR)

    def test_dotenv_file_is_read(self, tmp_path, monkeypatch):
        env_file = tmp_path / ".env"
        env_file.write_text("IPTU_MIN_ROWS_PER_YEAR=42\n", encoding="utf-8")
        monkeypatch.delenv("IPTU_MIN_ROWS_PER_YEAR", raising=False)
        custom = Settings(_env_file=str(env_file))
        assert custom.MIN_ROWS_PER_YEAR == 42


class TestDataPaths:
    def test_fallback_to_direct_location(self, tmp_path):
        custom = Settings(RAW_DIR=str(tmp_path / "raw"), _env_file=None)
        # No files exist: falls back to the direct (non-subdirectory) layout
        assert custom.data_paths[2020] == custom.RAW_DIR / "iptu_2020.csv"

    def test_subdirectory_location_preferred(self, tmp_path):
        raw = tmp_path / "raw"
        subdir_file = raw / "iptu_2021" / "iptu_2021.csv"
        subdir_file.parent.mkdir(parents=True)
        subdir_file.write_text("x", encoding="utf-8")
        custom = Settings(RAW_DIR=str(raw), _env_file=None)
        assert custom.data_paths[2021] == subdir_file

    def test_all_configured_years_have_paths(self):
        expected = set(settings.CSV_YEARS + settings.JSON_YEARS)
        assert set(settings.data_paths.keys()) == expected


class TestQualityThresholds:
    def test_thresholds_reflect_settings(self):
        # conftest sets IPTU_MIN_ROWS_PER_YEAR=5 before import
        assert QUALITY_THRESHOLDS["min_rows_per_year"] == 5
        assert QUALITY_THRESHOLDS["required_columns"] == COMMON_COLUMNS
