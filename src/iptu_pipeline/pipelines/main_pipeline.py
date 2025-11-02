"""
Main pipeline orchestrating all data processing steps.
"""
import pandas as pd
from pathlib import Path
from typing import Optional, List

from iptu_pipeline.config import CONSOLIDATED_DATA_PATH, PROCESSED_DATA_PATH, settings
from iptu_pipeline.utils.logger import setup_logger
from iptu_pipeline.utils.data_quality import DataQualityValidator
from iptu_pipeline.pipelines.ingestion import DataIngestion
from iptu_pipeline.pipelines.transformation import DataTransformer
from iptu_pipeline.pipelines.analysis import IPTUAnalyzer
from iptu_pipeline.engine import get_engine

logger = setup_logger("main_pipeline")


class IPTUPipeline:
    """Main pipeline for IPTU data processing."""
    
    def __init__(self, engine: Optional[str] = None):
        """
        Initialize pipeline with all components.
        
        Args:
            engine: Optional engine type ('pandas' or 'pyspark'). Uses config default if None.
        """
        # Get engine from config if not provided
        if engine is None:
            engine = getattr(settings, 'DATA_ENGINE', 'pandas')
        
        self.engine = get_engine(engine)
        self.validator = DataQualityValidator()
        self.ingestion = DataIngestion(quality_validator=self.validator, engine=engine)
        self.transformer = DataTransformer(engine=engine)
        self.analyzer: Optional[IPTUAnalyzer] = None
    
    def run_full_pipeline(
        self,
        years: Optional[List[int]] = None,
        save_consolidated: bool = True,
        run_analysis: bool = True,
        incremental: bool = False
    ) -> pd.DataFrame:
        """
        Run the complete data pipeline.
        
        Args:
            years: Years to process. If None, processes all available years.
            save_consolidated: Whether to save consolidated data
            run_analysis: Whether to run analyses
            incremental: Whether to load only new years (incremental mode)
        
        Returns:
            Consolidated DataFrame
        """
        logger.info("="*80)
        logger.info("Starting IPTU Data Pipeline")
        logger.info("="*80)
        
        # Step 1: Data Ingestion
        logger.info("\n[STEP 1] Data Ingestion")
        logger.info("-" * 80)
        
        if incremental:
            # Incremental ingestion: load only new years
            new_dataframes = self.ingestion.load_incremental(
                new_years=years or [],
                existing_data_path=CONSOLIDATED_DATA_PATH
            )
            
            if not new_dataframes:
                logger.info("No new data to process. Loading existing consolidated data.")
                if CONSOLIDATED_DATA_PATH.exists():
                    consolidated_df = self.engine.read_parquet(CONSOLIDATED_DATA_PATH)
                    row_count = self.engine.get_count(consolidated_df)
                    logger.info(f"Loaded existing data: {row_count:,} rows")
                else:
                    logger.warning("No existing data found. Running full pipeline.")
                    new_dataframes = self.ingestion.load_all_years(years=years)
                    consolidated_df = None
            else:
                # Load existing data to append to
                if CONSOLIDATED_DATA_PATH.exists():
                    existing_df = self.engine.read_parquet(CONSOLIDATED_DATA_PATH)
                    row_count = self.engine.get_count(existing_df)
                    logger.info(f"Loaded existing data: {row_count:,} rows")
                else:
                    existing_df = None
                
                consolidated_df = self.transformer.consolidate_datasets(
                    new_dataframes,
                    append_to_existing=existing_df
                )
        else:
            # Full ingestion: load all specified years
            dataframes = self.ingestion.load_all_years(years=years)
            
            # Step 2: Data Transformation and Unification
            logger.info("\n[STEP 2] Data Transformation and Unification")
            logger.info("-" * 80)
            consolidated_df = self.transformer.consolidate_datasets(dataframes)
        
        # Step 3: Save consolidated data
        if save_consolidated:
            logger.info("\n[STEP 3] Saving Consolidated Data")
            logger.info("-" * 80)
            self.engine.write_parquet(consolidated_df, CONSOLIDATED_DATA_PATH, compression='snappy')
            logger.info(f"[OK] Saved consolidated data to {CONSOLIDATED_DATA_PATH}")
            
            # Also save as processed (optimized) version
            self.engine.write_parquet(consolidated_df, PROCESSED_DATA_PATH, compression='snappy')
            logger.info(f"[OK] Saved processed data to {PROCESSED_DATA_PATH}")
        
        # Step 4: Data Analysis
        if run_analysis:
            logger.info("\n[STEP 4] Data Analysis")
            logger.info("-" * 80)
            # Analysis currently only supports Pandas
            if self.engine.engine_type == "pyspark":
                logger.warning("Analysis currently only supports Pandas. Skipping analysis for PySpark.")
            else:
                self.analyzer = IPTUAnalyzer(consolidated_df)
                all_analyses = self.analyzer.generate_all_analyses()
                self.analyzer.save_analyses()
                logger.info("[OK] Analysis complete")
        
        # Step 5: Generate validation report
        logger.info("\n[STEP 5] Validation Report")
        logger.info("-" * 80)
        validation_report = self.validator.generate_validation_report(
            output_path=Path("outputs") / "validation_report.csv"
        )
        if not validation_report.empty:
            logger.info(f"[OK] Validation report generated: {len(validation_report)} records")
        
        # Generate errors table
        errors_table = self.validator.get_errors_table()
        if not errors_table.empty:
            errors_path = Path("outputs") / "validation_errors.csv"
            errors_table.to_csv(errors_path, index=False)
            logger.info(f"[OK] Validation errors table saved: {errors_path}")
        
        logger.info("\n" + "="*80)
        logger.info("Pipeline Complete!")
        logger.info("="*80)
        
        return consolidated_df
    
    def get_pipeline_summary(self) -> dict:
        """
        Get summary of pipeline execution.
        
        Returns:
            Dictionary with pipeline summary
        """
        summary = {
            "ingestion_status": self.ingestion.get_ingestion_status(),
            "validation_summary": None,
            "transformation_summary": None,
            "analysis_summary": None
        }
        
        if self.validator.validation_results:
            validation_df = self.validator.generate_validation_report()
            summary["validation_summary"] = {
                "years_validated": len(validation_df),
                "passed": validation_df["passed"].sum() if "passed" in validation_df.columns else 0,
                "failed": (~validation_df["passed"]).sum() if "passed" in validation_df.columns else 0
            }
        
        if self.transformer.transformation_history:
            summary["transformation_summary"] = {
                "years_transformed": len(self.transformer.transformation_history)
            }
        
        if self.analyzer and self.analyzer.analyses_results:
            summary["analysis_summary"] = {
                "analyses_completed": list(self.analyzer.analyses_results.keys())
            }
        
        return summary

