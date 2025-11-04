"""
Configuration module for IPTU pipeline using Pydantic Settings.
"""
from pathlib import Path
from typing import Dict, List
from pydantic import Field, field_validator

# Handle pydantic_settings import with fallback
try:
    from pydantic_settings import BaseSettings, SettingsConfigDict
except ImportError:
    # Fallback for environments where pydantic-settings isn't installed
    # Try to use pydantic's BaseSettings directly (older versions)
    try:
        from pydantic import BaseSettings, BaseConfig
        # Create SettingsConfigDict as an alias for compatibility
        SettingsConfigDict = BaseConfig
    except ImportError:
        # Last resort: raise with helpful error
        raise ImportError(
            "pydantic-settings is required. Install it with: pip install pydantic-settings"
        )


class Settings(BaseSettings):
    """Configuration settings for IPTU pipeline."""
    
    model_config = SettingsConfigDict(
        env_prefix='IPTU_',
        case_sensitive=False,
        extra='ignore'
    )
    
    # Base directories
    BASE_DIR: Path = Field(
        default_factory=lambda: Path(__file__).parent.parent.parent,
        description="Base directory of the project"
    )
    DATA_DIR: Path = Field(
        default_factory=lambda: Path(__file__).parent.parent.parent / "data",
        description="Directory containing input data files"
    )
    LOG_DIR: Path = Field(
        default_factory=lambda: Path(__file__).parent.parent.parent / "logs",
        description="Directory for log files"
    )
    
    # Medallion architecture directories (all under data/)
    RAW_DIR: Path = Field(
        default_factory=lambda: Path(__file__).parent.parent.parent / "data" / "raw",
        description="Raw layer: source data files, no processing"
    )
    BRONZE_DIR: Path = Field(
        default_factory=lambda: Path(__file__).parent.parent.parent / "data" / "bronze",
        description="Bronze layer: cataloged and cleaned data (Parquet format)"
    )
    SILVER_DIR: Path = Field(
        default_factory=lambda: Path(__file__).parent.parent.parent / "data" / "silver",
        description="Silver layer: transformed and consolidated data"
    )
    GOLD_DIR: Path = Field(
        default_factory=lambda: Path(__file__).parent.parent.parent / "data" / "gold",
        description="Gold layer: refined data ready for consumption, analysis, and plots"
    )
    CATALOG_DIR: Path = Field(
        default_factory=lambda: Path(__file__).parent.parent.parent / "data" / "catalog",
        description="Data catalog: tracks metadata across all medallion layers (raw, bronze, silver, gold)"
    )
    
    # Data years configuration
    CSV_YEARS: List[int] = Field(
        default=[2020, 2021, 2022, 2023],
        description="Years with CSV format data"
    )
    JSON_YEARS: List[int] = Field(
        default=[2024],
        description="Years with JSON format data"
    )
    
    @property
    def gold_delta_dir(self) -> Path:
        """Path to gold layer Delta tables directory."""
        path = self.GOLD_DIR / "delta"
        path.mkdir(parents=True, exist_ok=True)
        return path
    
    @property
    def gold_parquet_dir(self) -> Path:
        """Path to gold layer Parquet files directory."""
        path = self.GOLD_DIR / "parquet"
        path.mkdir(parents=True, exist_ok=True)
        return path
    
    @property
    def gold_csv_dir(self) -> Path:
        """Path to gold layer CSV files directory (analyses)."""
        path = self.GOLD_DIR / "csv"
        path.mkdir(parents=True, exist_ok=True)
        return path
    
    @property
    def gold_plots_dir(self) -> Path:
        """Path to gold layer plots directory."""
        path = self.GOLD_DIR / "plots"
        path.mkdir(parents=True, exist_ok=True)
        return path
    
    @property
    def analysis_output_path(self) -> Path:
        """Path to analysis output directory (in gold layer CSV)."""
        return self.gold_csv_dir
    
    @property
    def plots_output_path(self) -> Path:
        """Path to plots output directory (in gold layer)."""
        return self.gold_plots_dir
    
    # Data quality thresholds
    MIN_ROWS_PER_YEAR: int = Field(
        default=100,
        description="Minimum number of rows required per year"
    )
    MAX_NULL_PERCENTAGE: float = Field(
        default=50.0,
        description="Maximum percentage of null values allowed"
    )
    
    # Data processing engine
    DATA_ENGINE: str = Field(
        default="pandas",
        description="Data processing engine: 'pandas' or 'pyspark'"
    )
    
    @field_validator('BASE_DIR', 'DATA_DIR', 'LOG_DIR', 'RAW_DIR', 'BRONZE_DIR', 'SILVER_DIR', 'GOLD_DIR', 'CATALOG_DIR', mode='before')
    @classmethod
    def ensure_paths(cls, v) -> Path:
        """Ensure path is a Path object."""
        if isinstance(v, str):
            return Path(v)
        return v
    
    @field_validator('DATA_DIR', 'LOG_DIR', 'RAW_DIR', 'BRONZE_DIR', 'SILVER_DIR', 'GOLD_DIR', 'CATALOG_DIR', mode='after')
    @classmethod
    def create_directories(cls, v: Path) -> Path:
        """Create directories if they don't exist."""
        v.mkdir(parents=True, exist_ok=True)
        return v
    
    @property
    def data_paths(self) -> Dict[int, Path]:
        """Generate data paths dictionary based on configured years and directories.
        
        NOTE: Data files are stored in RAW_DIR (data/raw/) as per medallion architecture.
        Structure: data/raw/iptu_2020/iptu_2020.csv, data/raw/iptu_2021/iptu_2021.csv, etc.
        Example: C:\\Users\\Rodrigo\\Documents\\Entrevistas\\neuro_tech\\data\\raw\\iptu_2020\\iptu_2020.csv
        """
        paths = {}
        for year in self.CSV_YEARS:
            # Check subdirectory location first (e.g., data/raw/iptu_2020/iptu_2020.csv)
            file_path = self.RAW_DIR / f"iptu_{year}" / f"iptu_{year}.csv"
            if not file_path.exists():
                # Fallback to direct location (e.g., data/raw/iptu_2020.csv)
                file_path = self.RAW_DIR / f"iptu_{year}.csv"
            paths[year] = file_path
        for year in self.JSON_YEARS:
            # Check subdirectory location first (e.g., data/raw/iptu_2024_json/iptu_2024_json.json)
            file_path = self.RAW_DIR / f"iptu_{year}_json" / f"iptu_{year}_json.json"
            if not file_path.exists():
                # Fallback to direct location (e.g., data/raw/iptu_2024_json.json)
                file_path = self.RAW_DIR / f"iptu_{year}_json.json"
            paths[year] = file_path
        return paths


# Create global settings instance
settings = Settings()

# Schema mapping for 2024 (different column names/types)
SCHEMA_MAPPING_2024 = {
    "_id": "_id",
    "quantidade de pavimentos": "quant pavimentos",
}

# Expected columns (common across all years, excluding _id)
COMMON_COLUMNS = [
    "Número do contribuinte",
    "ano do exercício",
    "data do cadastramento",
    "tipo de contribuinte",
    "CPF/CNPJ mascarado do contribuinte",
    "logradouro",
    "numero",
    "complemento",
    "bairro",
    "cidade",
    "estado",
    "fração ideal",
    "AREA TERRENO",
    "AREA CONSTRUIDA",
    "área ocupada",
    "valor do m2 do terreno",
    "valor do m2 de construção",
    "ano da construção corrigido",
    "quant pavimentos",
    "tipo de uso do imóvel",
    "tipo de padrão da construção",
    "fator de obsolescência",
    "ano e mês de início da contribuição",
    "valor total do imóvel estimado",
    "valor IPTU",
    "CEP",
    "Regime de Tributação do iptu",
    "Regime de Tributação da trsd",
    "Tipo de Construção",
    "Tipo de Empreendimento",
    "Tipo de Estrutura",
    "Código Logradouro",
]

# Data quality thresholds (using settings values)
QUALITY_THRESHOLDS = {
    "min_rows_per_year": settings.MIN_ROWS_PER_YEAR,
    "max_null_percentage": settings.MAX_NULL_PERCENTAGE,
    "required_columns": COMMON_COLUMNS,
}

# Backward compatibility: export paths as constants
BASE_DIR = settings.BASE_DIR
DATA_DIR = settings.DATA_DIR
LOG_DIR = settings.LOG_DIR
RAW_DIR = settings.RAW_DIR
BRONZE_DIR = settings.BRONZE_DIR
SILVER_DIR = settings.SILVER_DIR
GOLD_DIR = settings.GOLD_DIR
CATALOG_DIR = settings.CATALOG_DIR
GOLD_DELTA_DIR = settings.gold_delta_dir
GOLD_PARQUET_DIR = settings.gold_parquet_dir
GOLD_CSV_DIR = settings.gold_csv_dir
GOLD_PLOTS_DIR = settings.gold_plots_dir
CSV_YEARS = settings.CSV_YEARS
JSON_YEARS = settings.JSON_YEARS
DATA_PATHS = settings.data_paths
ANALYSIS_OUTPUT_PATH = settings.analysis_output_path
PLOTS_OUTPUT_PATH = settings.plots_output_path
