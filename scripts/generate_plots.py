"""
Script to generate plots from IPTU analysis results.
"""
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from iptu_pipeline.visualizations import generate_plots_from_analysis_results
from iptu_pipeline.utils.logger import setup_logger

logger = setup_logger("generate_plots")


def main():
    """Generate all plots from analysis results."""
    logger.info("Starting plot generation...")
    
    try:
        plot_files = generate_plots_from_analysis_results()
        
        logger.info("="*80)
        logger.info("Plot Generation Complete!")
        logger.info("="*80)
        logger.info(f"\nGenerated {len([f for f in plot_files.keys() if f != 'html_report'])} plots")
        logger.info(f"\nPlots saved to: {plot_files.get('html_report', {}).parent}")
        logger.info(f"\nHTML Report: {plot_files.get('html_report', 'N/A')}")
        logger.info("\nGenerated plots:")
        for plot_name, plot_path in plot_files.items():
            if plot_name != "html_report":
                logger.info(f"  - {plot_name}: {plot_path}")
        
    except Exception as e:
        logger.error(f"Error generating plots: {str(e)}", exc_info=True)
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())

