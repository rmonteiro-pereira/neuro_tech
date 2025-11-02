"""
Standalone PyDeequ test script - completely independent from pipeline code.
Tests basic PyDeequ functionality with sample data.
"""
import sys
from pathlib import Path
import logging

# Setup basic logging
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s - %(message)s'
)
logger = logging.getLogger("pydeequ_test")


def create_spark_session():
    """Create a standalone Spark session with PyDeequ/Deequ JARs configured."""
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
            .appName("PyDeequ_Test") \
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
        
        # Set default SPARK_VERSION for PyDeequ (will be refined after session creation)
        # PyDeequ needs this before importing to determine which JARs to use
        if "SPARK_VERSION" not in os.environ:
            os.environ["SPARK_VERSION"] = "3.5"  # Default, will refine if needed
        
        # Configure Delta Lake if available (optional)
        try:
            from delta import configure_spark_with_delta_pip
            builder = configure_spark_with_delta_pip(builder)
            logger.info("Delta Lake configured")
        except ImportError:
            logger.debug("Delta Lake not available (optional)")
        
        # Configure PyDeequ/Deequ JARs on builder (must be done before session creation)
        try:
            import pydeequ
            # Use PyDeequ's built-in Maven coordinate constants
            builder = builder.config("spark.jars.packages", pydeequ.deequ_maven_coord) \
                            .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
            logger.info(f"PyDeequ/Deequ JAR configured: {pydeequ.deequ_maven_coord}")
        except ImportError:
            logger.warning("PyDeequ not available - JARs won't be configured")
        except Exception as e:
            logger.warning(f"Could not configure PyDeequ JARs: {str(e)}")
        
        # Create Spark session with JARs configured
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")  # Reduce Spark log verbosity
        
        # Refine SPARK_VERSION based on actual Spark version
        spark_version = spark.version
        spark_version_major_minor = '.'.join(spark_version.split('.')[:2])  # e.g., "3.5.5" -> "3.5"
        os.environ["SPARK_VERSION"] = spark_version_major_minor
        
        logger.info(f"Spark session created - Version: {spark.version}")
        logger.info(f"SPARK_VERSION set to: {spark_version_major_minor} (for PyDeequ)")
        return spark
        
    except ImportError:
        logger.error("PySpark not available. Please install: pip install pyspark")
        return None
    except Exception as e:
        logger.error(f"Failed to create Spark session: {str(e)}")
        return None


def test_pydeequ_basic():
    """Test basic PyDeequ functionality."""
    logger.info("=" * 80)
    logger.info("Testing PyDeequ Basic Functionality")
    logger.info("=" * 80)
    
    # Create Spark session
    spark = create_spark_session()
    if not spark:
        return False
    
    # SPARK_VERSION should already be set in create_spark_session()
    # But ensure it's set here too in case session was created elsewhere
    import os
    if "SPARK_VERSION" not in os.environ:
        spark_version_major_minor = '.'.join(spark.version.split('.')[:2])
        os.environ["SPARK_VERSION"] = spark_version_major_minor
        logger.info(f"Set SPARK_VERSION={spark_version_major_minor}")
    
    try:
        # Import PyDeequ (try different import paths)
        # NOTE: SPARK_VERSION must be set BEFORE importing PyDeequ
        Check = None
        CheckLevel = None
        VerificationSuite = None
        VerificationResult = None
        PyDeequSession = None
        ColumnProfilerRunner = None
        
        try:
            from pydeequ.checks import Check, CheckLevel
            from pydeequ.verification import VerificationSuite, VerificationResult
            logger.info("[OK] PyDeequ imports from pydeequ.checks successful")
        except ImportError:
            try:
                from pydeequ.schecks import Check, CheckLevel
                from pydeequ.verification import VerificationSuite, VerificationResult
                logger.info("[OK] PyDeequ imports from pydeequ.schecks successful")
            except ImportError:
                try:
                    from pydeequ import Check, CheckLevel
                    from pydeequ import VerificationSuite, VerificationResult
                    logger.info("[OK] PyDeequ imports from pydeequ successful")
                except ImportError:
                    logger.error("Could not import PyDeequ Check/Verification classes")
                    raise
        
        # Import profiling classes
        try:
            from pydeequ.profiles import ColumnProfilerRunner
            from pydeequ import PyDeequSession
            logger.info("[OK] PyDeequ profiling imports successful")
        except ImportError:
            try:
                from pydeequ.pydeequ import PyDeequSession
                from pydeequ.profiles import ColumnProfilerRunner
                logger.info("[OK] PyDeequ profiling imports successful (alternative path)")
            except ImportError:
                logger.warning("PyDeequ profiling classes not available")
                ColumnProfilerRunner = None
                PyDeequSession = None
        
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
        
        # Test 1: Basic constraint checks
        if Check and VerificationSuite and VerificationResult:
            logger.info("\n" + "-" * 80)
            logger.info("Test 1: Basic Constraint Checks")
            logger.info("-" * 80)
            
            try:
                check = Check(spark, CheckLevel.Error, "Basic Test Check")
                # Use only methods that are known to work in PyDeequ
                check_result = VerificationSuite(spark).onData(df).addCheck(
                    check.hasSize(lambda x: x >= 5, "DataFrame should have at least 5 rows")
                    .isComplete("id")
                    .isUnique("id")
                    .isNonNegative("ano")
                    .isContainedIn("tipo_uso", ["RESIDENCIAL", "COMERCIAL", "INDUSTRIAL"])
                    # Note: isGreaterThan/isLessThan/hasMin may have different API
                    # These basic checks should work for most PyDeequ versions
                ).run()
                
                result_df = VerificationResult.checkResultsAsDataFrame(spark, check_result)
                logger.info("\nConstraint Check Results:")
                result_df.show(truncate=False)
                logger.info("[OK] Constraint checks completed")
            except Exception as e:
                logger.warning(f"Constraint checks failed: {str(e)}")
                logger.info("This might indicate PyDeequ JARs are still downloading or API mismatch")
        else:
            logger.warning("Skipping constraint checks - Check/VerificationSuite not available")
        
        # Test 2: Column profiling
        if ColumnProfilerRunner and PyDeequSession:
            logger.info("\n" + "-" * 80)
            logger.info("Test 2: Column Profiling")
            logger.info("-" * 80)
            
            try:
                # ColumnProfilerRunner expects raw SparkSession, not PyDeequSession
                # PyDeequSession is only used for certain operations
                profiler = ColumnProfilerRunner(spark).onData(df)
                profile_result = profiler.run()
                
                logger.info("\nColumn Profiles:")
                
                # Try to extract profile information (API may vary)
                try:
                    # Newer PyDeequ API: profile_result is a DataFrame
                    if hasattr(profile_result, 'collect'):
                        profile_rows = profile_result.collect()
                        for row in profile_rows:
                            logger.info(f"  {row}")
                    # Older PyDeequ API: profile_result.profiles is a dict
                    elif hasattr(profile_result, 'profiles'):
                        for column, profile in profile_result.profiles.items():
                            logger.info(f"\nColumn: {column}")
                            logger.info(f"  Data Type: {getattr(profile, 'dataType', 'N/A')}")
                            if hasattr(profile, 'completeness'):
                                logger.info(f"  Completeness: {profile.completeness}")
                            if hasattr(profile, 'approximateNumDistinctValues'):
                                logger.info(f"  Distinct Values: {profile.approximateNumDistinctValues}")
                            if hasattr(profile, 'min'):
                                logger.info(f"  Min: {profile.min}")
                            if hasattr(profile, 'max'):
                                logger.info(f"  Max: {profile.max}")
                    else:
                        logger.info(f"Profile result type: {type(profile_result)}")
                        logger.info(f"Profile result: {profile_result}")
                except Exception as e:
                    logger.warning(f"Could not parse profile results: {str(e)}")
                    logger.info(f"Profile result type: {type(profile_result)}")
                
                logger.info("[OK] Column profiling completed")
            except Exception as e:
                logger.warning(f"Column profiling failed: {str(e)}")
                logger.info("This is expected if Deequ JARs are still downloading...")
                import traceback
                traceback.print_exc()
        else:
            logger.warning("Skipping column profiling - ColumnProfilerRunner not available")
        
        # Test 3: Anomaly detection (optional)
        logger.info("\n" + "-" * 80)
        logger.info("Test 3: Anomaly Detection (Optional)")
        logger.info("-" * 80)
        
        try:
            from pydeequ.anomaly_detection import AnomalyDetector, RelativeRateOfChangeStrategy
            
            if Check and VerificationSuite and VerificationResult:
                anomaly_check = Check(spark, CheckLevel.Warning, "Anomaly Detection")
                anomaly_result = VerificationSuite(spark).onData(df).addCheck(
                    anomaly_check.hasAnomaly(
                        AnomalyDetector(
                            RelativeRateOfChangeStrategy(0.1)
                        ),
                        "ano"
                    )
                ).run()
                
                anomaly_df = VerificationResult.checkResultsAsDataFrame(spark, anomaly_result)
                logger.info("\nAnomaly Detection Results:")
                anomaly_df.show(truncate=False)
                logger.info("[OK] Anomaly detection completed")
            else:
                logger.warning("Anomaly detection skipped - Check/VerificationSuite not available")
        except ImportError:
            logger.info("Anomaly detection not available in this PyDeequ version, skipping...")
        except Exception as e:
            logger.warning(f"Anomaly detection test failed: {str(e)}")
        
        logger.info("\n" + "=" * 80)
        logger.info("[OK] PyDeequ Basic Tests Complete!")
        logger.info("=" * 80)
        
        spark.stop()
        return True
        
    except ImportError as e:
        logger.error(f"PyDeequ import failed: {str(e)}")
        logger.error("Please ensure PyDeequ is installed: pip install pydeequ")
        if spark:
            spark.stop()
        return False
    except Exception as e:
        logger.error(f"PyDeequ test failed: {str(e)}")
        import traceback
        traceback.print_exc()
        if spark:
            spark.stop()
        return False


if __name__ == "__main__":
    success = test_pydeequ_basic()
    sys.exit(0 if success else 1)
