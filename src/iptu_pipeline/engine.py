"""
Engine abstraction layer for Pandas or PySpark.
Allows switching between Pandas and PySpark without changing pipeline code.
"""
from typing import Literal, Optional, Union
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

# Try to import PySpark
try:
    from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False
    SparkSession = None
    SparkDataFrame = None
    F = None

# Pandas is always available
import pandas as pd
import numpy as np

from iptu_pipeline.config import settings
from iptu_pipeline.utils.logger import setup_logger

logger = setup_logger("engine")


class DataEngine:
    """Abstract engine interface for Pandas or PySpark."""
    
    def __init__(self, engine: Literal["pandas", "pyspark"] = "pandas"):
        """
        Initialize data engine.
        
        Args:
            engine: "pandas" or "pyspark"
        """
        self.engine_type = engine.lower()
        
        if self.engine_type == "pyspark":
            if not PYSPARK_AVAILABLE:
                logger.warning("PySpark not available, falling back to Pandas")
                self.engine_type = "pandas"
            else:
                self.spark = self._get_or_create_spark_session()
        
        logger.info(f"Using {self.engine_type.upper()} engine")
    
    def _get_or_create_spark_session(self) -> SparkSession:
        """Get or create Spark session with Delta Lake support."""
        if not PYSPARK_AVAILABLE:
            raise RuntimeError("PySpark is not available")
        
        import os
        
        # Check if running in Docker/cluster mode
        spark_master = os.getenv("SPARK_MASTER", "local[*]")
        
        builder = SparkSession.builder \
            .appName("IPTU_Pipeline") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .config("spark.sql.shuffle.partitions", "200")
        
        # Configure Delta Lake if available (must be done on builder, not session)
        try:
            from delta import configure_spark_with_delta_pip
            builder = configure_spark_with_delta_pip(builder)
            logger.info("Delta Lake configured for Spark session")
        except ImportError:
            logger.debug("Delta Lake not available, will use Parquet format")
        
        # If running in Docker with cluster, use master URL
        if spark_master.startswith("spark://"):
            builder = builder.master(spark_master)
            logger.info(f"Connecting to Spark cluster at {spark_master}")
        else:
            builder = builder.master(spark_master)
            logger.info(f"Using Spark in {spark_master} mode")
        
        spark = builder.getOrCreate()
        logger.info(f"Spark session created - Version: {spark.version}")
        return spark
    
    def read_csv(self, file_path: Path, **kwargs) -> Union[pd.DataFrame, SparkDataFrame]:
        """Read CSV file using the selected engine."""
        if self.engine_type == "pyspark":
            if not PYSPARK_AVAILABLE:
                raise RuntimeError("PySpark is not available")
            
            # PySpark CSV reading
            sep = kwargs.get('sep', ';')
            spark_df = self.spark.read \
                .option("header", "true") \
                .option("delimiter", sep) \
                .option("inferSchema", "true") \
                .option("encoding", kwargs.get('encoding', 'utf-8')) \
                .csv(str(file_path))
            return spark_df
        else:
            # Pandas CSV reading
            return pd.read_csv(file_path, **kwargs)
    
    def read_json(self, file_path: Path, **kwargs) -> Union[pd.DataFrame, SparkDataFrame]:
        """Read JSON file using the selected engine."""
        if self.engine_type == "pyspark":
            if not PYSPARK_AVAILABLE:
                raise RuntimeError("PySpark is not available")
            
            # PySpark JSON reading
            # Read JSON - PySpark can handle nested structures
            spark_df = self.spark.read \
                .option("multiline", "true") \
                .json(str(file_path))
            
            # Check if we have a nested structure with 'records'
            cols = spark_df.columns
            if "records" in cols and "fields" in cols:
                # Get field names from the fields array
                fields_info = spark_df.select("fields").first()
                if fields_info:
                    # fields_info is a Row with a list of Rows
                    fields_data = fields_info[0]  # This is the list of Row objects
                    column_names = [field.id for field in fields_data]
                    
                    # Explode the records array (which is an array of arrays)
                    spark_df = spark_df.select(F.explode("records").alias("record_values"))
                    
                    # We need to create columns from the array elements
                    # The array has values in order matching the fields
                    num_cols = len(column_names)
                    select_expr = [spark_df["record_values"][i].alias(column_names[i]) for i in range(num_cols)]
                    spark_df = spark_df.select(*select_expr)
            elif "records" in cols:
                # If records exists but not fields, select it
                spark_df = spark_df.select("records.*")
            
            return spark_df
        else:
            # Pandas JSON reading
            import json
            with open(file_path, 'r', encoding='utf-8') as f:
                json_data = json.load(f)
            
            if 'records' in json_data and 'fields' in json_data:
                column_names = [field['id'] for field in json_data['fields']]
                df = pd.DataFrame(json_data['records'], columns=column_names)
            else:
                df = pd.read_json(file_path, **kwargs)
            
            return df
    
    def read_parquet(self, file_path: Path, **kwargs) -> Union[pd.DataFrame, SparkDataFrame]:
        """Read Parquet file using the selected engine."""
        if self.engine_type == "pyspark":
            if not PYSPARK_AVAILABLE:
                raise RuntimeError("PySpark is not available")
            return self.spark.read.parquet(str(file_path))
        else:
            return pd.read_parquet(file_path, **kwargs)
    
    def write_parquet(self, df: Union[pd.DataFrame, SparkDataFrame], file_path: Path, **kwargs):
        """Write DataFrame to Parquet using the selected engine."""
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        if self.engine_type == "pyspark":
            if not isinstance(df, SparkDataFrame):
                raise ValueError("DataFrame must be Spark DataFrame for PySpark engine")
            df.write.mode("overwrite").parquet(str(file_path))
        else:
            if not isinstance(df, pd.DataFrame):
                raise ValueError("DataFrame must be Pandas DataFrame for Pandas engine")
            df.to_parquet(file_path, **kwargs)
    
    def to_pandas(self, df: Union[pd.DataFrame, SparkDataFrame]) -> pd.DataFrame:
        """Convert to Pandas DataFrame."""
        if self.engine_type == "pyspark":
            if isinstance(df, SparkDataFrame):
                return df.toPandas()
            return df
        else:
            return df
    
    def to_spark(self, df: Union[pd.DataFrame, SparkDataFrame]) -> SparkDataFrame:
        """Convert to Spark DataFrame."""
        if self.engine_type == "pyspark":
            if isinstance(df, pd.DataFrame):
                return self.spark.createDataFrame(df)
            return df
        else:
            raise ValueError("Cannot convert to Spark DataFrame when using Pandas engine")
    
    def get_count(self, df: Union[pd.DataFrame, SparkDataFrame]) -> int:
        """Get row count."""
        if self.engine_type == "pyspark":
            if isinstance(df, SparkDataFrame):
                return df.count()
            return len(df)
        else:
            return len(df)
    
    def get_columns(self, df: Union[pd.DataFrame, SparkDataFrame]) -> list:
        """Get column names."""
        if self.engine_type == "pyspark":
            if isinstance(df, SparkDataFrame):
                return df.columns
            return df.columns.tolist()
        else:
            return df.columns.tolist()
    
    def groupby(self, df: Union[pd.DataFrame, SparkDataFrame], by: Union[str, list]):
        """Create groupby object."""
        if self.engine_type == "pyspark":
            if isinstance(df, SparkDataFrame):
                return df.groupBy(by if isinstance(by, list) else [by])
            return df.groupby(by)
        else:
            return df.groupby(by)
    
    def concat(self, dataframes: list) -> Union[pd.DataFrame, SparkDataFrame]:
        """Concatenate multiple DataFrames with schema alignment."""
        if self.engine_type == "pyspark":
            if not PYSPARK_AVAILABLE:
                raise RuntimeError("PySpark is not available")
            
            # Convert all to Spark if needed
            spark_dfs = []
            for df in dataframes:
                if isinstance(df, pd.DataFrame):
                    spark_dfs.append(self.spark.createDataFrame(df))
                elif isinstance(df, SparkDataFrame):
                    spark_dfs.append(df)
                else:
                    spark_dfs.append(self.spark.createDataFrame(df))
            
            # Use unionByName with allowMissingColumns=True for schema evolution
            # This handles different schemas across years without manual alignment
            result = spark_dfs[0]
            for df in spark_dfs[1:]:
                result = result.unionByName(df, allowMissingColumns=True)
            
            return result
        else:
            return pd.concat(dataframes, ignore_index=True)
    
    def stop_spark(self):
        """Stop Spark session if using PySpark."""
        if self.engine_type == "pyspark" and PYSPARK_AVAILABLE and hasattr(self, 'spark'):
            self.spark.stop()
            logger.info("Spark session stopped")


# Global engine instance (will be initialized from config)
_engine_instance: Optional[DataEngine] = None


def get_engine(engine: Optional[Literal["pandas", "pyspark"]] = None) -> DataEngine:
    """
    Get or create global engine instance.
    
    Args:
        engine: Engine type. If None, uses config setting.
    
    Returns:
        DataEngine instance
    """
    global _engine_instance
    
    if engine is None:
        # Get from config
        engine = getattr(settings, 'DATA_ENGINE', 'pandas').lower()
    
    if _engine_instance is None or _engine_instance.engine_type != engine:
        _engine_instance = DataEngine(engine)
    
    return _engine_instance


def set_engine(engine: Literal["pandas", "pyspark"]):
    """Set global engine."""
    global _engine_instance
    _engine_instance = DataEngine(engine)
    logger.info(f"Global engine set to {engine.upper()}")

