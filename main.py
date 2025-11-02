"""
Main entry point for IPTU Data Pipeline.
"""
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from iptu_pipeline.orchestration import run_orchestrated_pipeline
from iptu_pipeline.utils.logger import setup_logger

logger = setup_logger("main")


def main():
    """Main function to run the IPTU data pipeline."""
    logger.info("Starting IPTU Data Pipeline")
    
    # Run the orchestrated pipeline
    # Set years=None to process all available years
    # Set incremental=True to only process new years
    consolidated_df = run_orchestrated_pipeline(
        years=None,  # Process all years, or specify [2020, 2021, 2022, 2023, 2024]
        incremental=False,  # Set to True for incremental processing
        run_analysis=True
    )
    
    logger.info(f"Pipeline completed successfully!")
    # Use engine to get counts (works for both pandas and pyspark)
    from iptu_pipeline.engine import get_engine
    from iptu_pipeline.config import settings
    engine = get_engine(getattr(settings, 'DATA_ENGINE', 'pandas'))
    row_count = engine.get_count(consolidated_df)
    col_count = len(engine.get_columns(consolidated_df))
    logger.info(f"Consolidated dataset: {row_count:,} rows, {col_count} columns")
    
    # Generate plots from analysis results
    logger.info("\nGenerating visualizations from analysis results...")
    try:
        from iptu_pipeline.visualizations import generate_plots_from_analysis_results
        plot_files = generate_plots_from_analysis_results()
        logger.info(f"[OK] Generated {len([f for f in plot_files.keys() if f != 'html_report'])} plots")
        logger.info(f"[OK] HTML report: {plot_files.get('html_report', 'N/A')}")
    except Exception as e:
        logger.warning(f"Could not generate plots: {str(e)}")
    
    return consolidated_df


if __name__ == "__main__":
    main()
