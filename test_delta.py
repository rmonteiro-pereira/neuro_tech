"""
Standalone Delta Lake test script - completely independent from pipeline code.
Tests basic Delta Lake functionality with sample data to identify required JARs.
"""
import sys
from pathlib import Path
import logging

# Setup basic logging
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s - %(message)s'
)
logger = logging.getLogger("delta_test")


def create_spark_session():
    """Create a standalone Spark session with Delta Lake configured."""
    try:
        from pyspark.sql import SparkSession
        import os
        import sys
        
        # Configure Python path for Spark workers
        python_executable = sys.executable
        if not os.getenv("PYSPARK_PYTHON"):
            os.environ["PYSPARK_PYTHON"] = python_executable
        if not os.getenv("PYSPARK_DRIVER_PYTHON"):
            os.environ["PYSPARK_DRIVER_PYTHON"] = python_executable
        
        builder = SparkSession.builder \
            .appName("Delta_Test") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .config("spark.sql.shuffle.partitions", "4") \
            .config("spark.driver.host", "localhost") \
            .config("spark.driver.bindAddress", "127.0.0.1") \
            .config("spark.network.timeout", "600s") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
            .config("spark.sql.debug.maxToStringFields", "200") \
            .master("local[*]")
        
        # Configure Delta Lake if available
        try:
            from delta import configure_spark_with_delta_pip
            builder = configure_spark_with_delta_pip(builder)
            
            # IMPORTANT: configure_spark_with_delta_pip loads JARs but doesn't set extensions!
            # We need to manually add the extensions and catalog
            builder = builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            
            logger.info("Delta Lake configured: JARs loaded + extensions set")
        except ImportError:
            logger.error("Delta Lake not available. Please install: pip install delta-spark")
            return None
        except Exception as e:
            logger.error(f"Failed to configure Delta Lake: {str(e)}")
            return None
        
        # Create Spark session
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")  # Reduce Spark log verbosity
        
        logger.info(f"Spark session created - Version: {spark.version}")
        return spark
        
    except ImportError:
        logger.error("PySpark not available. Please install: pip install pyspark")
        return None
    except Exception as e:
        logger.error(f"Failed to create Spark session: {str(e)}")
        return None


def test_delta_basic():
    """Test basic Delta Lake functionality."""
    logger.info("=" * 80)
    logger.info("Testing Delta Lake Basic Functionality")
    logger.info("=" * 80)
    
    # Create Spark session
    spark = create_spark_session()
    if not spark:
        return False
    
    try:
        # Create sample data
        logger.info("\nCreating sample data...")
        sample_data = [
            ("A001", "RESIDENCIAL", 2020, 1500.0, 200000.0),
            ("A002", "COMERCIAL", 2021, 2000.0, 300000.0),
            ("A003", "RESIDENCIAL", 2022, 1800.0, 250000.0),
            ("A004", "COMERCIAL", 2023, 2500.0, 400000.0),
            ("A005", "RESIDENCIAL", 2024, 1200.0, 180000.0),
        ]
        
        df = spark.createDataFrame(
            sample_data,
            ["id", "tipo_uso", "ano", "area_terreno", "valor_iptu"]
        )
        
        row_count = df.count()
        col_count = len(df.columns)
        logger.info(f"Sample DataFrame created: {row_count} rows, {col_count} columns")
        logger.info("\nSample data:")
        df.show(truncate=False)
        
        # Test 1: Write to Delta format
        logger.info("\n" + "-" * 80)
        logger.info("Test 1: Write to Delta Format")
        logger.info("-" * 80)
        
        test_delta_path = Path("test_delta_table")
        
        try:
            # Try to write as Delta
            df.write.format("delta").mode("overwrite").save(str(test_delta_path))
            logger.info(f"[OK] Successfully wrote to Delta format: {test_delta_path}")
            
            # Test 2: Read from Delta format
            logger.info("\n" + "-" * 80)
            logger.info("Test 2: Read from Delta Format")
            logger.info("-" * 80)
            
            read_df = spark.read.format("delta").load(str(test_delta_path))
            read_count = read_df.count()
            logger.info(f"[OK] Successfully read from Delta format: {read_count} rows")
            read_df.show(truncate=False)
            
            # Test 3: Use DeltaTable API
            logger.info("\n" + "-" * 80)
            logger.info("Test 3: DeltaTable API")
            logger.info("-" * 80)
            
            try:
                from delta.tables import DeltaTable
                delta_table = DeltaTable.forPath(spark, str(test_delta_path))
                delta_df = delta_table.toDF()
                delta_count = delta_df.count()
                logger.info(f"[OK] Successfully used DeltaTable API: {delta_count} rows")
                
                # Get history
                history = delta_table.history()
                logger.info("\nDelta table history:")
                history.show(truncate=False)
                
            except Exception as e:
                logger.warning(f"DeltaTable API test failed: {str(e)}")
                logger.info("This might indicate missing Delta classes, but basic read/write works")
            
            # Cleanup
            import shutil
            if test_delta_path.exists():
                shutil.rmtree(test_delta_path)
                logger.info(f"Cleaned up test directory: {test_delta_path}")
            
            logger.info("\n" + "=" * 80)
            logger.info("[OK] Delta Lake Basic Tests Complete!")
            logger.info("=" * 80)
            
            spark.stop()
            return True
            
        except Exception as e:
            logger.error(f"Delta write/read test failed: {str(e)}")
            import traceback
            traceback.print_exc()
            
            # Check what packages are configured
            logger.info("\nChecking Spark configuration...")
            try:
                packages = spark.conf.get("spark.jars.packages", "Not set")
                logger.info(f"spark.jars.packages: {packages}")
                
                extensions = spark.conf.get("spark.sql.extensions", "Not set")
                logger.info(f"spark.sql.extensions: {extensions}")
                
                catalog = spark.conf.get("spark.sql.catalog.spark_catalog", "Not set")
                logger.info(f"spark.sql.catalog.spark_catalog: {catalog}")
            except:
                pass
            
            spark.stop()
            return False
            
    except Exception as e:
        logger.error(f"Delta test failed: {str(e)}")
        import traceback
        traceback.print_exc()
        if spark:
            spark.stop()
        return False


if __name__ == "__main__":
    success = test_delta_basic()
    sys.exit(0 if success else 1)

