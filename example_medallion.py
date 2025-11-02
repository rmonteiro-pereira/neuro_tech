"""
Example script to run the IPTU Data Pipeline using Medallion Architecture.
Demonstrates Bronze -> Silver -> Gold data processing pattern.
"""
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root / "src"))

from iptu_pipeline.orchestration import run_medallion_pipeline
from iptu_pipeline.utils.logger import setup_logger

# Setup logging
logger = setup_logger("main")


def main():
    """Main function to run the medallion pipeline."""
    logger.info("Starting IPTU Medallion Architecture Pipeline")
    
    try:
        # Run the medallion pipeline
        # This will process data through Raw -> Bronze -> Silver -> Gold layers
        results = run_medallion_pipeline(
            years=None,  # Process all years
            incremental=False,  # Set to True for incremental processing
            engine='pyspark'  # Medallion architecture optimized for PySpark
        )
        
        logger.info("Medallion pipeline completed successfully!")
        
        # Get summary statistics
        from iptu_pipeline.engine import get_engine
        from iptu_pipeline.config import settings
        
        engine = get_engine('pyspark')
        
        # Silver layer (consolidated)
        silver_df = results['silver']
        silver_row_count = engine.get_count(silver_df)
        silver_col_count = len(engine.get_columns(silver_df))
        logger.info(f"\nSilver layer (consolidated): {silver_row_count:,} rows, {silver_col_count} columns")
        
        # Gold layer outputs
        gold_outputs = results['gold']
        logger.info(f"\nGold layer outputs created: {len(gold_outputs)}")
        for output_name, gold_df in gold_outputs.items():
            gold_row_count = engine.get_count(gold_df)
            gold_col_count = len(engine.get_columns(gold_df))
            logger.info(f"  - {output_name}: {gold_row_count:,} rows, {gold_col_count} columns")
        
        logger.info("\nMedallion Architecture Layers:")
        logger.info(f"  - Raw: {settings.DATA_DIR} (source files)")
        logger.info(f"  - Bronze: {settings.BRONZE_DIR} (cleaned & cataloged)")
        logger.info(f"  - Silver: {settings.SILVER_DIR} (consolidated)")
        logger.info(f"  - Gold: {settings.GOLD_DIR} (refined outputs)")
        
        logger.info("\nValidation report saved to: outputs/medallion_validation_report.json")
        
        return results
        
    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    main()


