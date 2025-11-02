"""
Standalone script to export the most refined version of the IPTU dataset.

Reads from the Silver layer (consolidated, transformed data) and exports
to a single Parquet file for sharing.

Usage:
    python export_refined_dataset.py [--output OUTPUT_FILE] [--engine pandas|pyspark]

Example:
    python export_refined_dataset.py --output iptu_refined.parquet --engine pyspark
"""
import sys
import argparse
from pathlib import Path
from typing import Optional

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

from iptu_pipeline.config import settings, SILVER_DIR
from iptu_pipeline.engine import get_engine
from iptu_pipeline.utils.logger import setup_logger

logger = setup_logger("export_refined_dataset")


def export_refined_dataset(
    output_path: Optional[Path] = None,
    engine: Optional[str] = None,
    year: Optional[int] = None
) -> Path:
    """
    Export the most refined version of the dataset to a single Parquet file.
    
    Args:
        output_path: Output file path. Defaults to 'iptu_refined.parquet' in project root.
        engine: Engine type ('pandas' or 'pyspark'). Uses config default if None.
        year: Optional year filter. If None, exports all years (consolidated).
    
    Returns:
        Path to the exported file
    """
    # Determine output path
    if output_path is None:
        output_path = Path(__file__).parent / "iptu_refined.parquet"
    else:
        output_path = Path(output_path)
    
    # Get engine
    data_engine = get_engine(engine)
    logger.info(f"Using engine: {data_engine.engine_type}")
    
    # Determine source path (Silver layer)
    if year is not None:
        # Single year from bronze layer
        from iptu_pipeline.config import BRONZE_DIR
        source_path = BRONZE_DIR / f"iptu_{year}"
        logger.info(f"Loading year {year} from bronze layer: {source_path}")
    else:
        # Consolidated data from silver layer
        source_path = SILVER_DIR / "iptu_silver_consolidated"
        logger.info(f"Loading consolidated data from silver layer: {source_path}")
    
    # Check if source exists
    if not source_path.exists():
        # Try alternative paths
        if data_engine.engine_type == "pyspark":
            # Check for Delta table or partitioned Parquet
            import glob
            parquet_files = list(glob.glob(str(source_path / "**/*.parquet"), recursive=True))
            if not parquet_files:
                raise FileNotFoundError(
                    f"Source data not found at {source_path}. "
                    f"Run the pipeline first to generate silver layer data."
                )
        else:
            # Check for single parquet file
            single_file = source_path / "data.parquet"
            if not single_file.exists():
                raise FileNotFoundError(
                    f"Source data not found at {source_path}. "
                    f"Run the pipeline first to generate silver layer data."
                )
    
    # Load data
    logger.info("Loading data...")
    try:
        # For export, always use Pandas to read (handles nanosecond timestamps and mixed types)
        # This is more reliable than trying to use Spark which has compatibility issues
        logger.info("Reading data with Pandas (handles all timestamp formats)...")
        import pandas as pd
        
        # Read data - prioritize Delta format to get correct column names (handles column mapping)
        pandas_df = None
        
        # First, try Delta format (handles column mapping correctly)
        if data_engine.engine_type == "pyspark":
            try:
                logger.info("Attempting to read from Delta table (preserves column names)...")
                delta_df = data_engine.spark.read.format("delta").load(str(source_path))
                
                # Get row count before converting to Pandas
                row_count_spark = data_engine.get_count(delta_df)
                logger.info(f"Delta table loaded: {row_count_spark:,} rows")
                
                pandas_df = delta_df.toPandas()
                logger.info("Converted Delta table to Pandas with correct column names")
            except Exception as delta_error:
                logger.warning(f"Delta read failed: {delta_error}, trying Parquet...")
                # Fall back to Parquet reading
                pass
        
        # Fall back to Parquet if Delta failed or engine is pandas
        if pandas_df is None:
            if (source_path / "data.parquet").exists():
                pandas_df = pd.read_parquet(source_path / "data.parquet")
                logger.info("Loaded from single Parquet file")
            else:
                # Try to read all parquet files in directory
                import glob
                parquet_files = glob.glob(str(source_path / "**/*.parquet"), recursive=True)
                if parquet_files:
                    logger.info(f"Reading {len(parquet_files)} Parquet files...")
                    logger.warning("NOTE: Reading Parquet directly may show UUID column names if Delta column mapping was used")
                    pandas_df = pd.concat([pd.read_parquet(f) for f in parquet_files], ignore_index=True)
                else:
                    raise FileNotFoundError(f"No Parquet or Delta files found in {source_path}")
        
        if pandas_df is None:
            raise FileNotFoundError(f"Could not load data from {source_path}")
        
        # Check for duplicate rows and remove them
        initial_row_count = len(pandas_df)
        duplicate_count = pandas_df.duplicated().sum()
        if duplicate_count > 0:
            logger.warning(f"Found {duplicate_count:,} duplicate rows ({duplicate_count/initial_row_count*100:.1f}%), removing...")
            pandas_df = pandas_df.drop_duplicates()
            logger.info(f"Removed duplicates: {initial_row_count:,} -> {len(pandas_df):,} rows")
        
        # Convert timestamp columns to date (remove time component for sharing)
        logger.info("Converting timestamps to dates...")
        for col_name in pandas_df.columns:
            if pandas_df[col_name].dtype == 'datetime64[ns]' or str(pandas_df[col_name].dtype).startswith('datetime'):
                # Convert to date (removes time component)
                pandas_df[col_name] = pd.to_datetime(pandas_df[col_name]).dt.date
        
        # Filter out internal metadata columns (for sharing)
        metadata_columns = [col for col in pandas_df.columns if col.startswith('_') and col not in ['_id']]
        
        if metadata_columns:
            logger.info(f"Removing {len(metadata_columns)} internal metadata columns: {metadata_columns}")
            pandas_df = pandas_df.drop(columns=metadata_columns)
        
        row_count = len(pandas_df)
        col_count = len(pandas_df.columns)
        
        logger.info(f"Loaded {row_count:,} rows, {col_count} columns")
        
        # Save as single Parquet file
        logger.info(f"Exporting to Parquet file: {output_path}")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Save with compatible timestamp format (use microsecond precision for better compatibility)
        pandas_df.to_parquet(output_path, index=False, compression='snappy', coerce_timestamps='us')
        logger.info(f"[OK] Exported to: {output_path}")
        
        logger.info("\n" + "="*80)
        logger.info("Export Summary:")
        logger.info(f"  Source: {source_path}")
        logger.info(f"  Output: {output_path}")
        logger.info(f"  Rows: {row_count:,}")
        logger.info(f"  Columns: {col_count}")
        logger.info(f"  Engine: {data_engine.engine_type}")
        logger.info("="*80)
        
        return output_path
        
    except Exception as e:
        logger.error(f"Failed to export dataset: {str(e)}")
        import traceback
        traceback.print_exc()
        raise


def main():
    """Main entry point for the export script."""
    parser = argparse.ArgumentParser(
        description="Export refined IPTU dataset to a single Parquet file"
    )
    parser.add_argument(
        "--output", "-o",
        type=str,
        default=None,
        help="Output file path (default: iptu_refined.parquet in project root)"
    )
    parser.add_argument(
        "--engine",
        type=str,
        choices=["pandas", "pyspark"],
        default=None,
        help="Engine type to use (default: from config)"
    )
    parser.add_argument(
        "--year",
        type=int,
        default=None,
        help="Optional: Export specific year from bronze layer instead of consolidated data"
    )
    
    args = parser.parse_args()
    
    try:
        output_path = export_refined_dataset(
            output_path=args.output,
            engine=args.engine,
            year=args.year
        )
        print(f"\n[OK] Successfully exported dataset to: {output_path}")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Export failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()

