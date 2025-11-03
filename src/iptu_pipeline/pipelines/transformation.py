"""
Data transformation and unification module for IPTU pipeline.
Handles schema differences between years and consolidates datasets.
Supports both Pandas and PySpark engines.
"""
from pathlib import Path
from typing import Dict, List, Optional

try:
    import pandas as pd
    import numpy as np
except ImportError:
    pd = None
    np = None

from iptu_pipeline.engine import get_engine
from iptu_pipeline.config import SCHEMA_MAPPING_2024, COMMON_COLUMNS
from iptu_pipeline.utils.logger import setup_logger
from iptu_pipeline.utils.column_matcher import match_and_map_columns, KNOWN_COLUMN_MAPPINGS

logger = setup_logger("transformation")


class DataTransformer:
    """Transforms and unifies data from different years and schemas."""
    
    def __init__(self, engine: Optional[str] = None):
        """
        Initialize transformer.
        
        Args:
            engine: Optional engine type ('pandas' or 'pyspark'). Uses config default if None.
        """
        self.transformation_history: List[Dict] = []
        self.engine = get_engine(engine)
    
    def normalize_column_names(self, df, year: int):
        """
        Normalize column names to match common schema.
        
        Args:
            df: DataFrame to normalize
            year: Year of the data
        
        Returns:
            DataFrame with normalized column names
        """
        logger.info(f"Normalizing column names for year {year} (engine: {self.engine.engine_type})")
        
        # Handle 2024 specific mappings
        if year == 2024:
            # Map 2024 column names to common schema
            column_mapping = {}
            
            columns = self.engine.get_columns(df)
            
            # Handle specific mappings from config
            for old_col, new_col in SCHEMA_MAPPING_2024.items():
                if old_col in columns:
                    column_mapping[old_col] = new_col
            
            # Handle "quantidade de pavimentos" -> "quant pavimentos"
            if "quantidade de pavimentos" in columns:
                column_mapping["quantidade de pavimentos"] = "quant pavimentos"
            
            if column_mapping:
                if self.engine.engine_type == "pyspark":
                    # PySpark rename
                    for old_col, new_col in column_mapping.items():
                        df = df.withColumnRenamed(old_col, new_col)
                else:
                    # Pandas rename
                    df = df.rename(columns=column_mapping)
                logger.info(f"Renamed columns: {column_mapping}")
        
        return df
    
    def standardize_data_types(self, df, year: int):
        """
        Standardize data types across all years.
        
        Args:
            df: DataFrame to standardize
            year: Year of the data
        
        Returns:
            DataFrame with standardized data types
        """
        logger.info(f"Standardizing data types for year {year}")
        
        columns = self.engine.get_columns(df)
        
        # Standardize numeric columns
        numeric_columns = [
            "ano do exercício",
            "numero",
            "ano da construção corrigido",
            "quant pavimentos",
            "ano e mês de início da contribuição",
            "CEP",
            "Código Logradouro"
        ]
        
        if self.engine.engine_type == "pyspark":
            # PySpark type casting
            try:
                from pyspark.sql.types import IntegerType, DoubleType
                from pyspark.sql import functions as F
                
                for col_name in numeric_columns:
                    if col_name in columns:
                        # Try to cast to numeric, use null on error
                        df = df.withColumn(
                            col_name,
                            F.col(col_name).cast(DoubleType())
                        )
                
                # Date column - convert to timestamp first, then extract date part
                if "data do cadastramento" in columns:
                    # For CSV files, the timestamp includes nanoseconds: "yyyy/M/d HH:mm:ss.SSSSSSSSS"
                    # Parse as timestamp, then convert to date to remove time component
                    df = df.withColumn(
                        "data do cadastramento",
                        F.to_date(F.to_timestamp("data do cadastramento", "yyyy/M/d HH:mm:ss.SSSSSSSSS"))
                    )
            except ImportError:
                logger.warning("PySpark not available for type casting")
        else:
            # Pandas type conversion
            for col in numeric_columns:
                if col in columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Date column - convert to datetime, then normalize to date
            if "data do cadastramento" in columns:
                try:
                    df["data do cadastramento"] = pd.to_datetime(
                        df["data do cadastramento"],
                        errors='coerce'
                    ).dt.normalize()
                except Exception as e:
                    logger.warning(f"Could not convert date column: {str(e)}")
        
        return df
    
    def handle_missing_columns(self, df, year: int):
        """
        Add missing columns with None/NaN values to match common schema.
        
        Args:
            df: DataFrame to enhance
            year: Year of the data
        
        Returns:
            DataFrame with all common columns
        """
        logger.info(f"Handling missing columns for year {year}")
        
        columns = self.engine.get_columns(df)
        
        # First, try matching known similar column names (only specific columns)
        missing_cols = set(COMMON_COLUMNS) - set(columns)
        
        # Only match if we have missing columns
        fuzzy_mappings = {}
        if missing_cols:
            similar_matches = match_and_map_columns(
                list(columns), 
                list(COMMON_COLUMNS), 
                threshold=0.7,
                only_known_mappings=True  # Only match known specific mappings
            )
            
            # Filter to only include columns where target is missing and source is not already in common schema
            for source_col, target_col in similar_matches.items():
                if target_col in missing_cols and source_col not in COMMON_COLUMNS:
                    fuzzy_mappings[source_col] = target_col
        
        # Apply fuzzy matches (rename similar columns)
        if fuzzy_mappings:
            logger.warning(
                f"[WARN] Found {len(fuzzy_mappings)} similar column names. "
                f"Renaming to match common schema: {fuzzy_mappings}"
            )
            
            if self.engine.engine_type == "pyspark":
                for old_col, new_col in fuzzy_mappings.items():
                    df = df.withColumnRenamed(old_col, new_col)
            else:
                df = df.rename(columns=fuzzy_mappings)
            
            # Update columns list and missing_cols
            columns = self.engine.get_columns(df)
            missing_cols = set(COMMON_COLUMNS) - set(columns)
        
        # Add truly missing columns
        if missing_cols:
            logger.info(f"Adding {len(missing_cols)} missing columns: {missing_cols}")
            if self.engine.engine_type == "pyspark":
                try:
                    from pyspark.sql import functions as F
                    from pyspark.sql.types import StringType
                    
                    for col in missing_cols:
                        df = df.withColumn(col, F.lit(None).cast(StringType()))
                except ImportError:
                    logger.warning("PySpark not available for adding columns")
            else:
                for col in missing_cols:
                    df[col] = None
        
        # Don't remove extra columns - support schema evolution
        # All source columns are preserved
        
        return df, fuzzy_mappings
    
    def clean_and_optimize(self, df):
        """
        Clean data and optimize memory usage.
        - Trim whitespace from all string columns
        - Convert low-cardinality string columns to categorical (< 50 unique values)
        
        Args:
            df: DataFrame to clean and optimize
        
        Returns:
            Optimized DataFrame
        """
        logger.info("Cleaning and optimizing DataFrame")
        
        if self.engine.engine_type == "pyspark":
            from pyspark.sql.functions import trim, col
            from pyspark.sql.types import StringType
            
            # Step 1: Trim whitespace from all string columns
            logger.info("Trimming whitespace from string columns")
            string_columns = [field.name for field in df.schema.fields if isinstance(field.dataType, StringType)]
            
            for col_name in string_columns:
                df = df.withColumn(col_name, trim(col(col_name)))
            
            if string_columns:
                logger.info(f"Trimmed {len(string_columns)} string columns: {string_columns[:5]}{'...' if len(string_columns) > 5 else ''}")
            
            # Step 2: Detect categorical columns (for logging/information purposes)
            # Note: PySpark doesn't have categorical dtype, but Parquet will encode repeated strings efficiently
            categorical_columns = []
            if string_columns:
                # Sample data to detect low cardinality (check first 100k rows for performance)
                sample_size = min(100000, df.count())
                sample_df = df.limit(sample_size)
                
                for col_name in string_columns:
                    unique_count = sample_df.select(col_name).distinct().count()
                    if unique_count < 50:
                        categorical_columns.append((col_name, unique_count))
            
            if categorical_columns:
                logger.info(f"Detected {len(categorical_columns)} low-cardinality string columns (categorical candidates): {[c[0] for c in categorical_columns]}")
            
            # PySpark optimizations are handled by Spark Catalyst optimizer
            # Don't cache here - can cause OOM when processing multiple years
            return df
        else:
            # Pandas optimizations
            df_clean = df.copy()
            
            # Step 1: Trim whitespace from all string/object columns
            logger.info("Trimming whitespace from string columns")
            # Get object and categorical columns (both may contain strings)
            object_columns = df_clean.select_dtypes(include=['object', 'category']).columns.tolist()
            
            # Filter to only columns that actually contain string data
            string_columns = []
            for col_name in object_columns:
                try:
                    # Check if column supports string operations
                    test_series = df_clean[col_name]
                    original_dtype = test_series.dtype.name
                    
                    # Convert categorical to string for testing
                    if original_dtype == 'category':
                        test_series = test_series.astype(str)
                    
                    # Test on a sample to avoid processing entire large column
                    sample = test_series.dropna().head(100)
                    if len(sample) > 0:
                        # Try to use .str accessor on a sample
                        test_result = sample.str.strip()
                        string_columns.append((col_name, original_dtype))
                except (AttributeError, TypeError):
                    # Column is not string type, skip it
                    continue
            
            for col_name, original_dtype in string_columns:
                # Handle categorical columns: convert to string, trim, then convert back
                if original_dtype == 'category':
                    # Convert categorical to string, trim, then back to category
                    df_clean[col_name] = df_clean[col_name].astype(str).str.strip().astype('category')
                else:
                    # For object columns, ensure they're string-like before trimming
                    # Use try-except to handle edge cases with mixed types
                    try:
                        df_clean[col_name] = df_clean[col_name].str.strip()
                    except (AttributeError, TypeError):
                        # Column contains non-string data, skip trimming but log it
                        logger.debug(f"Skipping whitespace trimming for column {col_name} (non-string data detected)")
                        continue
            
            if string_columns:
                logger.info(f"Trimmed {len(string_columns)} string columns")
            
            # Step 2: Convert low-cardinality string columns to category
            # Changed from < 50% ratio to < 50 absolute unique values
            categorical_columns = []
            for col_info in string_columns:
                # Extract column name (string_columns may be tuples (col_name, dtype) or just strings)
                if isinstance(col_info, tuple):
                    col = col_info[0]
                else:
                    col = col_info
                
                unique_count = df_clean[col].nunique()
                if unique_count < 50:
                    df_clean[col] = df_clean[col].astype('category')
                    categorical_columns.append((col, unique_count))
            
            if categorical_columns:
                logger.info(f"Converted {len(categorical_columns)} columns to categorical: {[c[0] for c in categorical_columns]}")
            
            # Optimize numeric types
            for col in df_clean.select_dtypes(include=[np.integer]).columns:
                df_clean[col] = pd.to_numeric(df_clean[col], downcast='integer')
            
            for col in df_clean.select_dtypes(include=[np.floating]).columns:
                df_clean[col] = pd.to_numeric(df_clean[col], downcast='float')
            
            logger.info(
                f"Memory optimization: "
                f"{df.memory_usage(deep=True).sum() / 1024**2:.2f} MB -> "
                f"{df_clean.memory_usage(deep=True).sum() / 1024**2:.2f} MB"
            )
            
            return df_clean
    
    def transform_year_data(self, df, year: int):
        """
        Apply all transformations to a single year's data.
        
        Args:
            df: DataFrame to transform
            year: Year of the data
        
        Returns:
            Transformed DataFrame
        """
        logger.info(f"Transforming data for year {year}")
        
        # Step 1: Normalize column names
        df = self.normalize_column_names(df, year)
        
        # Step 2: Handle missing columns
        df, fuzzy_mappings = self.handle_missing_columns(df, year)
        
        # Step 3: Standardize data types
        df = self.standardize_data_types(df, year)
        
        # Step 4: Clean and optimize
        df = self.clean_and_optimize(df)
        
        # Record transformation
        self.transformation_history.append({
            "year": year,
            "rows_before": self.engine.get_count(df),
            "columns_before": len(self.engine.get_columns(df)),
            "rows_after": self.engine.get_count(df),
            "columns_after": len(self.engine.get_columns(df)),
            "transformations_applied": [
                "normalize_column_names",
                "handle_missing_columns",
                "standardize_data_types",
                "clean_and_optimize"
            ]
        })
        
        logger.info(f"Transformation complete for year {year}")
        return df, fuzzy_mappings
    
    def consolidate_datasets(
        self, 
        dataframes: Dict[int, pd.DataFrame],
        append_to_existing: Optional[pd.DataFrame] = None
    ) -> pd.DataFrame:
        """
        Consolidate multiple year datasets into a single unified DataFrame.
        
        Args:
            dataframes: Dictionary mapping year to DataFrame
            append_to_existing: Optional existing consolidated DataFrame to append to
        
        Returns:
            Consolidated DataFrame
        """
        logger.info(f"Consolidating {len(dataframes)} year(s) of data")
        
        # Transform all datasets first
        transformed_dfs = []
        all_fuzzy_mappings = {}
        for year, df in dataframes.items():
            df_transformed, fuzzy_mappings = self.transform_year_data(df, year)
            transformed_dfs.append(df_transformed)
            if fuzzy_mappings:
                all_fuzzy_mappings[year] = fuzzy_mappings
        
        # Combine all DataFrames using engine
        if append_to_existing is not None:
            logger.info("Appending to existing consolidated data")
            transformed_dfs.insert(0, append_to_existing)
        
        consolidated_df = self.engine.concat(transformed_dfs)
        
        row_count = self.engine.get_count(consolidated_df)
        columns = self.engine.get_columns(consolidated_df)
        logger.info(
            f"[OK] Consolidated dataset: {row_count:,} rows, "
            f"{len(columns)} columns (engine: {self.engine.engine_type})"
        )
        
        # Log fuzzy mappings summary
        if all_fuzzy_mappings:
            logger.warning(
                f"[WARN] Column name fuzzy matching applied for {len(all_fuzzy_mappings)} year(s):"
            )
            for year, mappings in all_fuzzy_mappings.items():
                logger.warning(f"  Year {year}: {mappings}")
        
        return consolidated_df
    
    def get_transformation_summary(self) -> pd.DataFrame:
        """
        Get summary of all transformations applied.
        
        Returns:
            DataFrame with transformation summary
        """
        if not self.transformation_history:
            return pd.DataFrame()
        
        return pd.DataFrame(self.transformation_history)

