"""
Engine abstraction layer for Pandas or PySpark.
Allows switching between Pandas and PySpark without changing pipeline code.
"""
from typing import Literal, Optional, Union
from pathlib import Path
import warnings
import os
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
        import sys
        
        # Set default SPARK_VERSION for PyDeequ (will be refined after session creation)
        # PyDeequ needs this before importing to determine which JARs to use
        if "SPARK_VERSION" not in os.environ:
            os.environ["SPARK_VERSION"] = "3.5"  # Default for Spark 3.5.x, will refine if needed
        
        # Check if running in Docker/cluster mode
        # Use SPARK_MASTER_URL if set (from docker-compose), otherwise check SPARK_MASTER, fallback to local
        spark_master = os.getenv("SPARK_MASTER_URL") or os.getenv("SPARK_MASTER", "local[*]")
        
        # In Docker, prefer connecting to Spark cluster
        if "SPARK_MASTER_URL" in os.environ:
            logger.info(f"Connecting to Spark cluster: {spark_master}")
        
        # Configure Python path for Spark workers (use venv Python if available)
        python_executable = sys.executable
        if not os.getenv("PYSPARK_PYTHON"):
            os.environ["PYSPARK_PYTHON"] = python_executable
        if not os.getenv("PYSPARK_DRIVER_PYTHON"):
            os.environ["PYSPARK_DRIVER_PYTHON"] = python_executable
        
        builder = SparkSession.builder \
            .appName("IPTU_Pipeline") \
            .master(spark_master) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .config("spark.sql.shuffle.partitions", "200") \
            .config("spark.sql.warehouse.dir", str(Path(__file__).parent.parent.parent / "spark-warehouse")) \
            .config("spark.pyspark.python", python_executable) \
            .config("spark.pyspark.driver.python", python_executable) \
            .config("spark.network.timeout", "600s") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
            .config("spark.sql.debug.maxToStringFields", "200")
        
        # Only set driver host/bind for local mode
        if spark_master.startswith("local"):
            builder = builder \
                .config("spark.driver.host", "localhost") \
                .config("spark.driver.bindAddress", "127.0.0.1")
        
        # Set default SPARK_VERSION for PyDeequ (will be refined after session creation)
        # PyDeequ needs this before importing to determine which JARs to use
        if "SPARK_VERSION" not in os.environ:
            os.environ["SPARK_VERSION"] = "3.5"  # Default for Spark 3.5.x, will refine if needed
        
        # Configure Delta Lake and PyDeequ JARs together
        # IMPORTANT: configure_spark_with_delta_pip sets spark.jars.packages internally.
        # If we also need PyDeequ, we need to manually combine packages to avoid overwriting.
        pydeequ_available = False
        pydeequ_packages = None
        pydeequ_excludes = None
        
        # Check if PyDeequ is available (needed to decide whether to use configure_spark_with_delta_pip)
        try:
            import pydeequ
            pydeequ_available = True
            pydeequ_packages = pydeequ.deequ_maven_coord
            pydeequ_excludes = pydeequ.f2j_maven_coord
            logger.debug(f"PyDeequ/Deequ detected: {pydeequ.deequ_maven_coord}")
        except ImportError:
            logger.debug("PyDeequ not available, data quality checks will use basic validation")
        except Exception as e:
            logger.debug(f"Could not detect PyDeequ: {str(e)}, basic validation will be used")
        
        # Configure Delta Lake
        # If PyDeequ is also needed, we'll manually set packages instead of using configure_spark_with_delta_pip
        # to avoid package overwrite issues
        if pydeequ_available:
            # Both Delta and PyDeequ needed: manually configure Delta packages along with PyDeequ
            # Delta Lake package format: io.delta:delta-spark_2.12:VERSION
            # We'll use a version that matches delta-spark>=3.3.2 from requirements
            try:
                # Try to import delta to check if it's available
                import delta
                # Manually construct Delta package (matches what configure_spark_with_delta_pip would set)
                # Using Spark 2.12 scala version (standard for Spark 3.x)
                delta_packages = "io.delta:delta-spark_2.12:3.3.2"
                
                # Combine Delta and PyDeequ packages
                combined_packages = f"{delta_packages},{pydeequ_packages}"
                builder = builder.config("spark.jars.packages", combined_packages)
                if pydeequ_excludes:
                    builder = builder.config("spark.jars.excludes", pydeequ_excludes)
                
                # Manually add Delta extensions and catalog (required for Delta to work)
                builder = builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                                .config("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "false") \
                                .config("spark.databricks.delta.columnMapping.enabled", "true") \
                                .config("spark.databricks.delta.columnMapping.mode", "name")
                logger.info("Delta Lake + PyDeequ configured: JARs loaded + extensions set (manual package combination)")
            except ImportError:
                logger.debug("Delta Lake not available (optional)")
            except Exception as e:
                logger.warning(f"Could not configure Delta Lake manually: {str(e)}")
        else:
            # Only Delta needed: use configure_spark_with_delta_pip (standard approach)
            try:
                from delta import configure_spark_with_delta_pip
                builder = configure_spark_with_delta_pip(builder)
                
                # Manually add extensions and catalog (required for Delta to work)
                builder = builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                                .config("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "false") \
                                .config("spark.databricks.delta.columnMapping.enabled", "true") \
                                .config("spark.databricks.delta.columnMapping.mode", "name")
                logger.info("Delta Lake configured: JARs loaded + extensions set")
            except ImportError:
                logger.debug("Delta Lake not available (optional)")
            except Exception as e:
                logger.warning(f"Could not configure Delta Lake: {str(e)}")
        
        # If running in Docker with cluster, use master URL
        if spark_master.startswith("spark://"):
            builder = builder.master(spark_master)
            logger.info(f"Connecting to Spark cluster at {spark_master}")
        else:
            builder = builder.master(spark_master)
            logger.info(f"Using Spark in {spark_master} mode")
        
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")  # Reduce Spark log verbosity
        
        # Refine SPARK_VERSION based on actual Spark version
        spark_version = spark.version
        spark_version_major_minor = '.'.join(spark_version.split('.')[:2])  # e.g., "3.5.5" -> "3.5"
        os.environ["SPARK_VERSION"] = spark_version_major_minor
        
        logger.info(f"Spark session created - Version: {spark.version}")
        logger.debug(f"SPARK_VERSION set to: {spark_version_major_minor} (for PyDeequ)")
        
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
        """
        Convert to Pandas DataFrame.
        
        Note: PySpark DateType cannot be directly converted to Pandas.
        DateType columns are automatically converted to TimestampType before conversion.
        """
        if self.engine_type == "pyspark":
            if isinstance(df, SparkDataFrame):
                # PySpark DateType cannot be directly converted to Pandas
                # Convert DateType columns to TimestampType before toPandas()
                try:
                    from pyspark.sql.types import DateType
                    from pyspark.sql import functions as F
                    
                    # Check for DateType columns and convert them to TimestampType
                    date_columns = [
                        field.name for field in df.schema.fields 
                        if isinstance(field.dataType, DateType)
                    ]
                    
                    if date_columns:
                        logger.debug(f"Converting DateType columns to TimestampType for Pandas conversion: {date_columns}")
                        for col_name in date_columns:
                            # Convert DateType to TimestampType (date becomes timestamp at midnight)
                            df = df.withColumn(col_name, F.to_timestamp(F.col(col_name)))
                    
                    return df.toPandas()
                except Exception as e:
                    logger.warning(f"Error converting DateType columns: {e}, attempting direct conversion")
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
        """
        Concatenate multiple DataFrames with schema alignment.
        
        For Pandas: Uses pd.concat with proper column alignment to prevent duplication.
        For PySpark: Uses unionByName for schema evolution.
        """
        if self.engine_type == "pyspark":
            if not PYSPARK_AVAILABLE:
                raise RuntimeError("PySpark is not available")
            
            # Convert all to Spark if needed
            spark_dfs = []
            total_rows = 0
            for i, df in enumerate(dataframes):
                if isinstance(df, pd.DataFrame):
                    rows = len(df)
                    spark_df = self.spark.createDataFrame(df)
                    spark_dfs.append(spark_df)
                elif isinstance(df, SparkDataFrame):
                    rows = df.count()
                    spark_dfs.append(df)
                else:
                    rows = len(df) if hasattr(df, '__len__') else 0
                    spark_df = self.spark.createDataFrame(df)
                    spark_dfs.append(spark_df)
                
                total_rows += rows
                logger.debug(f"DataFrame {i}: {rows:,} rows, {len(df.columns) if hasattr(df, 'columns') else 'N/A'} columns")
            
            logger.debug(f"Total rows before PySpark concat: {total_rows:,}")
            
            # Use unionByName with allowMissingColumns=True for schema evolution
            # This handles different schemas across years without manual alignment
            result = spark_dfs[0]
            for df in spark_dfs[1:]:
                result = result.unionByName(df, allowMissingColumns=True)
            
            result_rows = result.count()
            if result_rows != total_rows:
                logger.warning(f"Row count changed during PySpark concat: {total_rows:,} -> {result_rows:,}")
            
            return result
        else:
            # Pandas concat with proper schema alignment
            if not dataframes:
                raise ValueError("Cannot concatenate empty list of DataFrames")
            
            if len(dataframes) == 1:
                return dataframes[0].copy()
            
            # Log row counts before concatenation
            total_rows = sum(len(df) for df in dataframes)
            logger.debug(f"Total rows before Pandas concat: {total_rows:,} from {len(dataframes)} DataFrames")
            
            # Get all unique columns across all DataFrames
            all_columns = set()
            for df in dataframes:
                all_columns.update(df.columns.tolist())
            all_columns = sorted(list(all_columns))
            
            logger.debug(f"Total unique columns to align: {len(all_columns)}")
            
            # Ensure each DataFrame has all columns (missing columns become NaN)
            # This prevents pandas from creating duplicate columns
            aligned_dfs = []
            for i, df in enumerate(dataframes):
                # Check for duplicate columns BEFORE alignment
                if df.columns.duplicated().any():
                    dup = df.columns[df.columns.duplicated()].unique()
                    raise ValueError(f"DataFrame {i} has duplicate columns before concat: {dup.tolist()}")
                
                # Reindex to ensure all columns exist (missing become NaN)
                df_aligned = df.reindex(columns=all_columns, copy=False)
                aligned_dfs.append(df_aligned)
                
                logger.debug(f"DataFrame {i}: {len(df):,} rows, {len(df.columns)} -> {len(df_aligned.columns)} columns after alignment")
            
            # Concatenate with ignore_index to create new sequential index
            # sort=False to maintain column order (faster)
            result = pd.concat(aligned_dfs, ignore_index=True, sort=False)
            
            # Validate result
            result_rows = len(result)
            if result_rows != total_rows:
                ratio = result_rows / total_rows if total_rows > 0 else 0
                logger.error(f"⚠️  Row count mismatch after concat: expected {total_rows:,}, got {result_rows:,} ({ratio:.2f}x)")
                logger.error(f"   This suggests duplication during concatenation!")
                logger.error(f"   Check for duplicate columns or alignment issues")
            
            # Verify no duplicate columns in result
            if result.columns.duplicated().any():
                dup = result.columns[result.columns.duplicated()].unique()
                raise ValueError(f"Duplicate columns in concatenated result: {dup.tolist()}")
            
            logger.debug(f"Concatenation complete: {result_rows:,} rows, {len(result.columns)} columns")
            return result
    
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

