"""Tests for the Pandas side of the DataEngine abstraction."""
import json

import pandas as pd
import pytest

from iptu_pipeline.engine import DataEngine, get_engine


@pytest.fixture()
def engine():
    return DataEngine("pandas")


class TestEngineBasics:
    def test_get_engine_returns_pandas(self):
        assert get_engine("pandas").engine_type == "pandas"

    def test_count_and_columns(self, engine):
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        assert engine.get_count(df) == 3
        assert engine.get_columns(df) == ["a", "b"]

    def test_to_pandas_is_identity_for_pandas(self, engine):
        df = pd.DataFrame({"a": [1]})
        assert engine.to_pandas(df) is df

    def test_to_spark_raises_on_pandas_engine(self, engine):
        with pytest.raises(ValueError):
            engine.to_spark(pd.DataFrame({"a": [1]}))


class TestReaders:
    def test_read_csv_semicolon(self, engine, tmp_path):
        p = tmp_path / "x.csv"
        p.write_text("a;b\n1;foo\n2;bar\n", encoding="utf-8")
        df = engine.read_csv(p, sep=";")
        assert list(df.columns) == ["a", "b"]
        assert len(df) == 2

    def test_read_json_records_fields_layout(self, engine, tmp_path):
        # The layout of the 2024 IPTU JSON export
        payload = {
            "fields": [{"id": "col1"}, {"id": "col2"}],
            "records": [[1, "a"], [2, "b"], [3, "c"]],
        }
        p = tmp_path / "x.json"
        p.write_text(json.dumps(payload), encoding="utf-8")
        df = engine.read_json(p)
        assert list(df.columns) == ["col1", "col2"]
        assert len(df) == 3
        assert df["col2"].tolist() == ["a", "b", "c"]

    def test_parquet_roundtrip(self, engine, tmp_path):
        df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
        p = tmp_path / "x.parquet"
        engine.write_parquet(df, p)
        back = engine.read_parquet(p)
        pd.testing.assert_frame_equal(back, df)


class TestWriteParquetGuards:
    def test_rejects_corrupted_uuid_column_names(self, engine, tmp_path):
        corrupted = "col-" + "a1b2c3d4-e5f6-a1b2-c3d4-a1b2c3d4e5f6-extra"
        df = pd.DataFrame({corrupted: [1], "ok": [2]})
        with pytest.raises(ValueError, match="UUID"):
            engine.write_parquet(df, tmp_path / "x.parquet")

    def test_wrong_frame_type_raises(self, engine, tmp_path):
        with pytest.raises(ValueError):
            engine.write_parquet([1, 2, 3], tmp_path / "x.parquet")


class TestConcat:
    def test_aligns_differing_schemas_without_row_loss(self, engine):
        df1 = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
        df2 = pd.DataFrame({"a": [3], "c": [9.0]})
        out = engine.concat([df1, df2])
        assert len(out) == 3
        assert set(out.columns) == {"a", "b", "c"}
        # Columns missing from one frame become NaN, not duplicated columns
        assert out["c"].isna().sum() == 2
        assert not out.columns.duplicated().any()

    def test_single_frame_returns_copy(self, engine):
        df = pd.DataFrame({"a": [1]})
        out = engine.concat([df])
        assert out is not df
        pd.testing.assert_frame_equal(out, df)

    def test_empty_list_raises(self, engine):
        with pytest.raises(ValueError):
            engine.concat([])

    def test_duplicate_columns_rejected(self, engine):
        bad = pd.DataFrame([[1, 2]], columns=["a", "a"])
        good = pd.DataFrame({"a": [3]})
        with pytest.raises(ValueError, match="duplicate"):
            engine.concat([bad, good])
