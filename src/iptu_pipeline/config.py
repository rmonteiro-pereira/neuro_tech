"""
Configuration module for IPTU pipeline using Pydantic Settings.
"""
from pathlib import Path
from typing import Dict, List
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


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
    OUTPUT_DIR: Path = Field(
        default_factory=lambda: Path(__file__).parent.parent.parent / "outputs",
        description="Directory for output files"
    )
    LOG_DIR: Path = Field(
        default_factory=lambda: Path(__file__).parent.parent.parent / "logs",
        description="Directory for log files"
    )
    
    # Medallion architecture directories
    BRONZE_DIR: Path = Field(
        default_factory=lambda: Path(__file__).parent.parent.parent / "outputs" / "bronze",
        description="Bronze layer: raw ingested data"
    )
    SILVER_DIR: Path = Field(
        default_factory=lambda: Path(__file__).parent.parent.parent / "outputs" / "silver",
        description="Silver layer: cleaned and validated data"
    )
    GOLD_DIR: Path = Field(
        default_factory=lambda: Path(__file__).parent.parent.parent / "outputs" / "gold",
        description="Gold layer: business-ready aggregated data"
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
    
    # File paths (computed from OUTPUT_DIR)
    @property
    def consolidated_data_path(self) -> Path:
        """Path to consolidated data file."""
        return self.OUTPUT_DIR / "iptu_consolidated.parquet"
    
    @property
    def processed_data_path(self) -> Path:
        """Path to processed data file."""
        return self.OUTPUT_DIR / "iptu_processed.parquet"
    
    @property
    def analysis_output_path(self) -> Path:
        """Path to analysis output directory."""
        path = self.OUTPUT_DIR / "analyses"
        path.mkdir(parents=True, exist_ok=True)
        return path
    
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
    
    @field_validator('BASE_DIR', 'DATA_DIR', 'OUTPUT_DIR', 'LOG_DIR', 'BRONZE_DIR', 'SILVER_DIR', 'GOLD_DIR', mode='before')
    @classmethod
    def ensure_paths(cls, v) -> Path:
        """Ensure path is a Path object."""
        if isinstance(v, str):
            return Path(v)
        return v
    
    @field_validator('OUTPUT_DIR', 'LOG_DIR', 'BRONZE_DIR', 'SILVER_DIR', 'GOLD_DIR', mode='after')
    @classmethod
    def create_directories(cls, v: Path) -> Path:
        """Create directories if they don't exist."""
        v.mkdir(parents=True, exist_ok=True)
        return v
    
    @property
    def data_paths(self) -> Dict[int, Path]:
        """Generate data paths dictionary based on configured years and directories."""
        paths = {}
        for year in self.CSV_YEARS:
            paths[year] = self.DATA_DIR / f"iptu_{year}" / f"iptu_{year}.csv"
        for year in self.JSON_YEARS:
            paths[year] = self.DATA_DIR / f"iptu_{year}_json" / f"iptu_{year}_json.json"
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
OUTPUT_DIR = settings.OUTPUT_DIR
LOG_DIR = settings.LOG_DIR
BRONZE_DIR = settings.BRONZE_DIR
SILVER_DIR = settings.SILVER_DIR
GOLD_DIR = settings.GOLD_DIR
CSV_YEARS = settings.CSV_YEARS
JSON_YEARS = settings.JSON_YEARS
DATA_PATHS = settings.data_paths
CONSOLIDATED_DATA_PATH = settings.consolidated_data_path
PROCESSED_DATA_PATH = settings.processed_data_path
ANALYSIS_OUTPUT_PATH = settings.analysis_output_path
