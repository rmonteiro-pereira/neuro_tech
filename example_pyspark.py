"""
Example of using PySpark engine with IPTU pipeline.
"""
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from iptu_pipeline.pipelines.main_pipeline import IPTUPipeline
from iptu_pipeline.utils.logger import setup_logger

logger = setup_logger("example_pyspark")


def main():
    """Run pipeline with PySpark engine."""
    logger.info("="*80)
    logger.info("IPTU Pipeline - PySpark Engine Example")
    logger.info("="*80)
    
    # Check if PySpark is available
    try:
        from pyspark.sql import SparkSession
        logger.info("✓ PySpark is available")
    except ImportError:
        logger.error("✗ PySpark not installed. Install with: pip install pyspark")
        logger.info("Falling back to Pandas engine...")
        engine = "pandas"
    else:
        engine = "pyspark"
    
    # Create pipeline with PySpark engine
    pipeline = IPTUPipeline(engine=engine)
    
    # Run pipeline
    consolidated_df = pipeline.run_full_pipeline(
        years=None,  # All years
        incremental=False,
        run_analysis=True
    )
    
    logger.info("\n" + "="*80)
    logger.info("Pipeline Complete!")
    logger.info("="*80)
    
    # Stop Spark if using PySpark
    if engine == "pyspark":
        pipeline.engine.stop_spark()
    
    return consolidated_df


if __name__ == "__main__":
    main()

