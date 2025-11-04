"""
Data ingestion module for IPTU pipeline.
Handles incremental ingestion of new year data without reprocessing all years.
Supports both Pandas and PySpark engines.
"""
from pathlib import Path
from typing import Dict, List, Optional, Set, Union
from datetime import datetime

try:
    import pandas as pd
except ImportError:
    pd = None

from iptu_pipeline.engine import get_engine
from iptu_pipeline.config import DATA_PATHS, CSV_YEARS, JSON_YEARS
from iptu_pipeline.utils.logger import setup_logger
from iptu_pipeline.utils.data_quality import DataQualityValidator

logger = setup_logger("ingestion")


class DataIngestion:
    """Handles incremental data ingestion from source files."""
    
    def __init__(self, quality_validator: Optional[DataQualityValidator] = None, engine: Optional[str] = None):
        """
        Initialize ingestion pipeline.
        
        Args:
            quality_validator: Optional DataQualityValidator instance
            engine: Optional engine type ('pandas' or 'pyspark'). Uses config default if None.
        """
        self.validator = quality_validator or DataQualityValidator()
        self.ingested_years: Set[int] = set()
        self.engine = get_engine(engine)
        
    def load_csv_file(self, file_path: Path, year: int):
        """
        Load a CSV file for years 2020-2023.
        
        Args:
            file_path: Path to CSV file
            year: Year of the data
        
        Returns:
            DataFrame (Pandas or Spark) with loaded data
        """
        logger.info(f"Loading CSV file for year {year}: {file_path} (engine: {self.engine.engine_type})")
        
        try:
            # Use engine to read CSV
            df = self.engine.read_csv(
                file_path,
                sep=';',
                encoding='utf-8',
                low_memory=False
            )
            
            row_count = self.engine.get_count(df)
            columns = self.engine.get_columns(df)
            logger.info(f"[OK] Loaded {year}: {row_count:,} rows, {len(columns)} columns")
            return df
            
        except Exception as e:
            logger.error(f"[ERROR] Error loading CSV for year {year}: {str(e)}")
            raise
    
    def load_json_file(self, file_path: Path, year: int):
        """
        Load a JSON file for year 2024.
        
        Args:
            file_path: Path to JSON file
            year: Year of the data
        
        Returns:
            DataFrame (Pandas or Spark) with loaded data
        """
        logger.info(f"Loading JSON file for year {year}: {file_path} (engine: {self.engine.engine_type})")
        
        try:
            # Use engine to read JSON
            df = self.engine.read_json(file_path)
            
            row_count = self.engine.get_count(df)
            columns = self.engine.get_columns(df)
            logger.info(f"[OK] Loaded {year}: {row_count:,} rows, {len(columns)} columns")
            return df
            
        except Exception as e:
            logger.error(f"[ERROR] Error loading JSON for year {year}: {str(e)}")
            raise
    
    def load_year_data(self, year: int, validate: bool = True):
        """
        Load data for a specific year.
        
        Args:
            year: Year to load
            validate: Whether to validate data quality
        
        Returns:
            DataFrame with year data
        """
        if year not in DATA_PATHS:
            raise ValueError(f"Year {year} not found in data paths configuration")
        
        file_path = DATA_PATHS[year]
        
        if not file_path.exists():
            raise FileNotFoundError(f"Data file not found: {file_path}")
        
        # Load based on file type
        if year in CSV_YEARS:
            df = self.load_csv_file(file_path, year)
        elif year in JSON_YEARS:
            df = self.load_json_file(file_path, year)
        else:
            raise ValueError(f"Unknown file type for year {year}")
        
        # Validate data quality
        if validate:
            validation_result = self.validator.validate_dataset(df, year, file_path)
            if not validation_result["passed"]:
                logger.warning(
                    f"Data quality validation failed for year {year}, "
                    f"but continuing with ingestion. Review errors in validation report."
                )
        
        self.ingested_years.add(year)
        return df
    
    def load_all_years(self, years: Optional[List[int]] = None, validate: bool = True) -> Dict:
        """
        Load data for all available years or specified years.
        
        Args:
            years: List of years to load. If None, loads all available years.
            validate: Whether to validate data quality
        
        Returns:
            Dictionary mapping year to DataFrame
        """
        if years is None:
            years = sorted(DATA_PATHS.keys())
        
        logger.info(f"Loading data for years: {years}")
        
        dataframes = {}
        for year in sorted(years):
            try:
                df = self.load_year_data(year, validate=validate)
                dataframes[year] = df
            except Exception as e:
                logger.error(f"Failed to load year {year}: {str(e)}")
                raise
        
        logger.info(f"[OK] Successfully loaded {len(dataframes)} year(s)")
        return dataframes
    
    def load_incremental(
        self, 
        new_years: List[int],
        existing_data_path: Optional[Path] = None
    ) -> Dict[int, pd.DataFrame]:
        """
        Load only new years that don't exist in the consolidated dataset.
        This enables incremental ingestion without reprocessing all years.
        
        Args:
            new_years: List of years to load incrementally
            existing_data_path: Path to existing consolidated data. If None, uses default.
        
        Returns:
            Dictionary mapping year to DataFrame for newly loaded years
        """
        # Legacy consolidated path no longer used - data is stored in silver layer
        # existing_data_path = existing_data_path or CONSOLIDATED_DATA_PATH
        
        # Check which years already exist
        existing_years = set()
        if existing_data_path.exists():
            try:
                # Read just the year column to check existing years
                existing_df = pd.read_parquet(existing_data_path)
                if "ano do exercício" in existing_df.columns:
                    existing_years = set(existing_df["ano do exercício"].unique())
                logger.info(f"Found existing data with years: {sorted(existing_years)}")
            except Exception as e:
                logger.warning(f"Could not read existing data: {str(e)}")
        
        # Filter out years that already exist
        years_to_load = [y for y in new_years if y not in existing_years]
        
        if not years_to_load:
            logger.info("All requested years already exist in consolidated data")
            return {}
        
        logger.info(f"Loading incremental years (new only): {years_to_load}")
        
        # Load only new years
        new_dataframes = {}
        for year in years_to_load:
            try:
                df = self.load_year_data(year, validate=True)
                new_dataframes[year] = df
            except Exception as e:
                logger.error(f"Failed to load year {year}: {str(e)}")
                raise
        
        logger.info(f"[OK] Successfully loaded {len(new_dataframes)} new year(s)")
        return new_dataframes
    
    def get_ingestion_status(self) -> Dict:
        """
        Get status of ingested years.
        
        Returns:
            Dictionary with ingestion status
        """
        return {
            "ingested_years": sorted(list(self.ingested_years)),
            "available_years": sorted(list(DATA_PATHS.keys())),
            "pending_years": sorted(list(set(DATA_PATHS.keys()) - self.ingested_years))
        }

