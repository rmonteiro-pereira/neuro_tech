"""
Pytest configuration for the IPTU pipeline test suite.

IMPORTANT: iptu_pipeline.config instantiates Settings() at import time and
creates its data directories as a side effect. To keep tests hermetic, all
IPTU_* directory settings are pointed at a session-scoped temporary directory
*before* anything under iptu_pipeline is imported.
"""
import json
import os
import sys
import tempfile
from pathlib import Path

# ---------------------------------------------------------------------------
# Environment redirection - MUST happen before importing iptu_pipeline.
# ---------------------------------------------------------------------------
_SESSION_TMP = Path(tempfile.mkdtemp(prefix="iptu_test_"))

os.environ["IPTU_BASE_DIR"] = str(_SESSION_TMP)
os.environ["IPTU_DATA_DIR"] = str(_SESSION_TMP / "data")
os.environ["IPTU_LOG_DIR"] = str(_SESSION_TMP / "logs")
os.environ["IPTU_RAW_DIR"] = str(_SESSION_TMP / "data" / "raw")
os.environ["IPTU_BRONZE_DIR"] = str(_SESSION_TMP / "data" / "bronze")
os.environ["IPTU_SILVER_DIR"] = str(_SESSION_TMP / "data" / "silver")
os.environ["IPTU_GOLD_DIR"] = str(_SESSION_TMP / "data" / "gold")
os.environ["IPTU_CATALOG_DIR"] = str(_SESSION_TMP / "data" / "catalog")
os.environ["IPTU_DATA_ENGINE"] = "pandas"
# Small threshold so tiny synthetic datasets pass row-count validation.
os.environ["IPTU_MIN_ROWS_PER_YEAR"] = "5"

# Make the package importable without installation (mirrors main.py).
_SRC = Path(__file__).parent.parent / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

import pandas as pd  # noqa: E402
import pytest  # noqa: E402

from iptu_pipeline.config import COMMON_COLUMNS, settings  # noqa: E402


def make_iptu_df(year: int, rows: int = 8) -> pd.DataFrame:
    """Build a synthetic IPTU DataFrame with the full common schema."""
    bairros = ["BOA VIAGEM", "MONTEIRO", "GUABIRABA", "JAQUEIRA"]
    tipos = ["Residencial", "Comercial"]
    data = {col: [None] * rows for col in COMMON_COLUMNS}
    data.update(
        {
            # Non-numeric so every reader (CSV inference, JSON) agrees on str dtype.
            # NOTE: purely-numeric IDs expose a real pipeline weakness - CSV years
            # infer int64 while the JSON year yields str, and the mixed column then
            # fails the silver-layer parquet write. See tests/README note in PR.
            "Número do contribuinte": [f"NC{year}-{i:06d}" for i in range(rows)],
            "ano do exercício": [year] * rows,
            "data do cadastramento": [f"{year}/1/{(i % 27) + 1} 10:00:00.000000000" for i in range(rows)],
            "tipo de contribuinte": ["PESSOA FISICA"] * rows,
            "CPF/CNPJ mascarado do contribuinte": ["***123**"] * rows,
            "logradouro": ["RUA DO TESTE  "] * rows,  # trailing spaces on purpose
            "numero": [str(100 + i) for i in range(rows)],
            "bairro": [bairros[i % len(bairros)] for i in range(rows)],
            "cidade": ["RECIFE"] * rows,
            "estado": ["PE"] * rows,
            "AREA TERRENO": [100.0 + i for i in range(rows)],
            "AREA CONSTRUIDA": [80.0 + i for i in range(rows)],
            "ano da construção corrigido": [str(1980 + i) for i in range(rows)],
            "quant pavimentos": [str(1 + (i % 3)) for i in range(rows)],
            "tipo de uso do imóvel": [tipos[i % len(tipos)] for i in range(rows)],
            "valor total do imóvel estimado": [200000.0 + 1000 * i for i in range(rows)],
            "valor IPTU": [1000.0 + 10 * i for i in range(rows)],
            "CEP": ["51020000"] * rows,
            "Código Logradouro": [str(4000 + i) for i in range(rows)],
        }
    )
    return pd.DataFrame(data)


def _write_raw_csv(year: int, rows: int = 8) -> Path:
    """Write a synthetic raw CSV for a year in the layout DATA_PATHS expects."""
    df = make_iptu_df(year, rows)
    path = settings.RAW_DIR / f"iptu_{year}.csv"
    df.to_csv(path, sep=";", index=False, encoding="utf-8")
    return path


def _write_raw_json(year: int, rows: int = 8) -> Path:
    """Write a synthetic raw JSON (records/fields layout used by the 2024 file)."""
    df = make_iptu_df(year, rows)
    # 2024 uses "quantidade de pavimentos" instead of "quant pavimentos" and has _id
    df = df.rename(columns={"quant pavimentos": "quantidade de pavimentos"})
    df.insert(0, "_id", range(1, rows + 1))
    payload = {
        "fields": [{"id": col, "type": "text"} for col in df.columns],
        "records": df.where(pd.notna(df), None).values.tolist(),
    }
    path = settings.RAW_DIR / f"iptu_{year}_json.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, default=str)
    return path


@pytest.fixture(scope="session")
def seeded_raw_data():
    """Create raw source files for all configured years, once per session."""
    paths = {}
    for year in settings.CSV_YEARS:
        paths[year] = _write_raw_csv(year)
    for year in settings.JSON_YEARS:
        paths[year] = _write_raw_json(year)
    return paths


@pytest.fixture()
def sample_df():
    """A single-year synthetic DataFrame."""
    return make_iptu_df(2020, rows=8)
