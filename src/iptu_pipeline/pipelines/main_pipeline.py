"""
Main pipeline orchestrating all data processing steps using Medallion Architecture.
Implements Bronze (cleaned/cataloged) -> Silver (consolidated) -> Gold (refined) layers.
"""
import pandas as pd
from pathlib import Path
from typing import Optional, List, Dict, Any

from iptu_pipeline.config import (
    CONSOLIDATED_DATA_PATH, PROCESSED_DATA_PATH, settings,
    RAW_DIR, BRONZE_DIR, SILVER_DIR, GOLD_DIR, CATALOG_DIR, DATA_DIR
)
from iptu_pipeline.utils.logger import setup_logger
from iptu_pipeline.utils.data_quality import DataQualityValidator
from iptu_pipeline.utils.medallion_quality import MedallionDataQuality
from iptu_pipeline.utils.raw_catalog import RawDataCatalog
from iptu_pipeline.pipelines.ingestion import DataIngestion
from iptu_pipeline.pipelines.transformation import DataTransformer
from iptu_pipeline.pipelines.analysis import IPTUAnalyzer
from iptu_pipeline.engine import get_engine

logger = setup_logger("main_pipeline")


class IPTUPipeline:
    """
    Main pipeline for IPTU data processing using Medallion Architecture.
    
    Architecture:
    - Raw: Source files (referenced, not stored in medallion)
    - Bronze: Cleaned and cataloged data per year (Delta/Parquet format)
    - Silver: Consolidated dataset (all years merged, standardized)
    - Gold: Refined business-ready outputs (aggregations, summaries)
    """
    
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
        
        # Legacy validator (for backward compatibility)
        self.validator = DataQualityValidator()
        
        # Medallion architecture quality checker (PyDeequ for PySpark)
        self.medallion_quality = None
        if self.engine.engine_type == "pyspark" and hasattr(self.engine, 'spark'):
            try:
                self.medallion_quality = MedallionDataQuality(self.engine.spark)
                logger.info("PyDeequ data quality enabled for medallion architecture")
            except Exception as e:
                logger.warning(f"Could not initialize PyDeequ: {str(e)}")
        
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
        Run the complete data pipeline using Medallion Architecture.
        
        Pipeline stages:
        1. Bronze Layer: Clean and catalog raw data (per year)
        2. Silver Layer: Consolidate all years into unified dataset
        3. Gold Layer: Create refined business-ready outputs
        
        Args:
            years: Years to process. If None, processes all available years.
            save_consolidated: Whether to save consolidated data (legacy path for backward compatibility)
            run_analysis: Whether to run analyses
            incremental: Whether to load only new years (incremental mode)
        
        Returns:
            Consolidated DataFrame from Silver layer
        """
        logger.info("="*80)
        logger.info("Starting IPTU Data Pipeline (Medallion Architecture)")
        logger.info("="*80)
        logger.info(f"Raw layer (source files): {RAW_DIR}")
        logger.info(f"Bronze layer (cleaned): {BRONZE_DIR}")
        logger.info(f"Silver layer (consolidated): {SILVER_DIR}")
        logger.info(f"Gold layer (refined/analysis/plots): {GOLD_DIR}")
        logger.info(f"Data catalog (metadata): {settings.CATALOG_DIR}")
        
        # ===== RAW LAYER: Catalog Source Files =====
        logger.info("\n" + "="*80)
        logger.info("[RAW LAYER] Cataloging Source Data Files")
        logger.info("="*80)
        
        # Initialize raw data catalog
        raw_catalog = RawDataCatalog(engine=self.engine.engine_type)
        
        # Scan and catalog all raw files
        cataloged_files = raw_catalog.scan_raw_files()
        catalog_summary = raw_catalog.get_catalog_summary()
        logger.info(f"Catalog summary: {catalog_summary}")
        
        # Save initial catalog state
        raw_catalog.save_catalog()
        logger.info(f"[OK] Raw files cataloged: {len(cataloged_files)} files")
        
        # ===== BRONZE LAYER: Clean and Catalog Raw Data =====
        logger.info("\n" + "="*80)
        logger.info("[BRONZE LAYER] Cleaning and Cataloging Raw Data")
        logger.info("="*80)
        
        # Load raw data from source files
        if years is None:
            years = sorted(settings.CSV_YEARS + settings.JSON_YEARS)
        
        bronze_dataframes = {}
        for year in years:
            # Check if already in bronze layer (incremental mode)
            if incremental and self._year_exists_in_bronze(year):
                logger.info(f"Year {year} already in bronze layer, skipping")
                continue
            
            logger.info(f"\nProcessing year {year} to bronze layer")
            
            # Mark file as processing in catalog
            bronze_path = self._get_bronze_path(year)
            raw_catalog.mark_as_processing(year, bronze_path=str(bronze_path))
            raw_catalog.save_catalog()
            
            # Load raw data
            try:
                df = self.ingestion.load_year_data(year, validate=True)
            except Exception as e:
                logger.error(f"Failed to load year {year}: {str(e)}")
                raw_catalog.mark_as_failed(year, error_message=str(e))
                raw_catalog.save_catalog()
                continue
            
            # Bronze layer cleaning: normalize, handle missing columns, standardize types, trim strings
            df = self.transformer.normalize_column_names(df, year)
            df, _ = self.transformer.handle_missing_columns(df, year)
            df = self.transformer.standardize_data_types(df, year)
            # Trim whitespace from strings and optimize categorical columns (must happen before validation)
            df = self.transformer.clean_and_optimize(df)
            
            # Add bronze layer metadata
            if self.engine.engine_type == "pyspark":
                from pyspark.sql.functions import current_timestamp, lit
                file_path = settings.data_paths.get(year)
                df = df.withColumn("_bronze_ingestion_timestamp", current_timestamp())
                df = df.withColumn("_source_file", lit(str(file_path) if file_path else ""))
                df = df.withColumn("_year_partition", lit(year))
                df = df.withColumn("_data_layer", lit("bronze"))
            else:
                import pandas as pd
                file_path = settings.data_paths.get(year)
                df["_bronze_ingestion_timestamp"] = pd.Timestamp.now()
                df["_source_file"] = str(file_path) if file_path else ""
                df["_year_partition"] = year
                df["_data_layer"] = "bronze"
            
            # Save to bronze layer
            bronze_path = BRONZE_DIR / f"iptu_{year}"
            self._save_to_bronze(df, bronze_path)
            logger.info(f"[OK] Year {year} saved to bronze layer: {bronze_path}")
            
            # Mark file as completed in catalog
            raw_catalog.mark_as_completed(year, bronze_path=str(bronze_path))
            raw_catalog.save_catalog()
            
            # Validate bronze layer with PyDeequ (if available)
            if self.medallion_quality:
                self.medallion_quality.validate_bronze_layer(df, year, settings.data_paths.get(year))
            
            bronze_dataframes[year] = df
        
        logger.info(f"\n[OK] Bronze layer complete: {len(bronze_dataframes)} years cataloged")
        
        # ===== SILVER LAYER: Consolidate All Years =====
        logger.info("\n" + "="*80)
        logger.info("[SILVER LAYER] Consolidating Data (All Years)")
        logger.info("="*80)
        
        # Load all years from bronze layer
        silver_dataframes = []
        for year in years:
            bronze_path = BRONZE_DIR / f"iptu_{year}"
            if not self._path_exists(bronze_path):
                logger.warning(f"Year {year} not found in bronze layer, skipping")
                continue
            
            df = self._load_from_bronze(bronze_path)
            
            # Apply additional cleaning for silver layer
            df = self.transformer.clean_and_optimize(df)
            
            # Add silver layer metadata
            if self.engine.engine_type == "pyspark":
                from pyspark.sql.functions import current_timestamp, lit
                df = df.withColumn("_silver_timestamp", current_timestamp())
                df = df.withColumn("_data_layer", lit("silver"))
            else:
                import pandas as pd
                df["_silver_timestamp"] = pd.Timestamp.now()
                df["_data_layer"] = "silver"
            
            silver_dataframes.append(df)
            logger.info(f"[OK] Year {year} processed for silver layer")
        
        if not silver_dataframes:
            raise RuntimeError("No data found in bronze layer to consolidate")
            
        # Consolidate all years into single dataset (schema evolution handled by unionByName)
        logger.info(f"\nConsolidating {len(silver_dataframes)} years into silver layer...")
        consolidated_df = self.engine.concat(silver_dataframes)
        
        # Get consolidated stats
        row_count = self.engine.get_count(consolidated_df)
        col_count = len(self.engine.get_columns(consolidated_df))
        logger.info(f"Silver layer consolidated: {row_count:,} rows, {col_count} columns")
        
        # Save consolidated silver layer
        silver_path = SILVER_DIR / "iptu_silver_consolidated"
        self._save_to_silver(consolidated_df, silver_path)
        logger.info(f"[OK] Silver layer saved to: {silver_path}")
        
        # Validate silver layer with PyDeequ (if available)
        if self.medallion_quality:
            self.medallion_quality.validate_silver_layer(consolidated_df, None)
        
        # Legacy: Save to old paths for backward compatibility
        if save_consolidated:
            logger.info("\n[Saving legacy paths for backward compatibility]")
            self.engine.write_parquet(consolidated_df, CONSOLIDATED_DATA_PATH, compression='snappy')
            logger.info(f"[OK] Saved to legacy path: {CONSOLIDATED_DATA_PATH}")
            self.engine.write_parquet(consolidated_df, PROCESSED_DATA_PATH, compression='snappy')
            logger.info(f"[OK] Saved to legacy path: {PROCESSED_DATA_PATH}")
        
        # ===== GOLD LAYER: Create Refined Outputs =====
        logger.info("\n" + "="*80)
        logger.info("[GOLD LAYER] Creating Refined Business-Ready Outputs")
        logger.info("="*80)
        
        gold_outputs = self._create_gold_outputs(consolidated_df)
        
        # Validate gold outputs
        if self.medallion_quality:
            for output_name, gold_df in gold_outputs.items():
                self.medallion_quality.validate_gold_layer(gold_df, f"gold_{output_name}")
        
        # ===== Analysis (if requested) =====
        if run_analysis:
            logger.info("\n[ANALYSIS] Running Data Analysis")
            logger.info("-" * 80)
            # Analysis now supports both Pandas and PySpark
            self.analyzer = IPTUAnalyzer(consolidated_df, engine=self.engine.engine_type)
            all_analyses = self.analyzer.generate_all_analyses()
            # Save analyses to gold layer (data/gold/analyses)
            self.analyzer.save_analyses(output_dir=settings.analysis_output_path)
            logger.info(f"[OK] Analysis complete - saved to {settings.analysis_output_path}")
        
        # ===== Generate Validation Reports =====
        logger.info("\n[VALIDATION] Generating Validation Reports")
        logger.info("-" * 80)
        
        # Legacy validation report
        validation_report = self.validator.generate_validation_report(
            output_path=Path("outputs") / "validation_report.csv"
        )
        if not validation_report.empty:
            logger.info(f"[OK] Legacy validation report: {len(validation_report)} records")
        
        errors_table = self.validator.get_errors_table()
        if not errors_table.empty:
            errors_path = Path("outputs") / "validation_errors.csv"
            errors_table.to_csv(errors_path, index=False)
            logger.info(f"[OK] Validation errors table saved: {errors_path}")
        
        # Medallion validation report (PyDeequ)
        if self.medallion_quality:
            medallion_report_path = settings.OUTPUT_DIR / "medallion_validation_report.json"
            self.medallion_quality.save_validation_report(medallion_report_path)
            summary = self.medallion_quality.get_summary()
            logger.info(f"[OK] Medallion validation summary: {summary['passed']}/{summary['total_validations']} passed")
        
        logger.info("\n" + "="*80)
        logger.info("Pipeline Complete!")
        logger.info("="*80)
        
        return consolidated_df
    
    def _create_gold_outputs(self, silver_df: Any) -> Dict[str, Any]:
        """
        Create refined business-ready outputs in gold layer.
        
        Args:
            silver_df: Consolidated silver DataFrame
        
        Returns:
            Dictionary of gold layer DataFrames
        """
        gold_outputs = {}
        
        if self.engine.engine_type == "pyspark":
            from pyspark.sql.functions import col, count, sum, avg, min, max, lit
            from datetime import datetime
            
            # Get columns in engine-agnostic way
            columns = self.engine.get_columns(silver_df)
            
            # Gold 1: Summary by Year and Property Type
            logger.info("Creating gold output: Summary by Year and Property Type")
            if "ano do exercício" in columns and "tipo de uso do imóvel" in columns:
                gold_summary = silver_df.groupBy("ano do exercício", "tipo de uso do imóvel") \
                    .agg(
                        count("*").alias("total_imoveis"),
                        sum("valor IPTU").alias("total_iptu_value"),
                        avg("valor IPTU").alias("avg_iptu_value"),
                        min("valor IPTU").alias("min_iptu_value"),
                        max("valor IPTU").alias("max_iptu_value")
                    ) \
                    .orderBy("ano do exercício", col("total_imoveis").desc())
                
                gold_summary = gold_summary.withColumn("_gold_timestamp", lit(datetime.now()))
                gold_summary = gold_summary.withColumn("_output_type", lit("summary_by_year_type"))
                
                gold_path_summary = GOLD_DIR / "gold_summary_by_year_type"
                self._save_to_gold(gold_summary, gold_path_summary)
                gold_outputs["summary_by_year_type"] = gold_summary
                logger.info(f"[OK] Saved to: {gold_path_summary}")
            
            # Gold 2: Summary by Neighborhood
            logger.info("Creating gold output: Summary by Neighborhood")
            if "bairro" in columns:
                gold_neighborhood = silver_df.groupBy("bairro", "ano do exercício") \
                    .agg(
                        count("*").alias("total_imoveis"),
                        sum("valor IPTU").alias("total_iptu_value"),
                        avg("AREA TERRENO").alias("avg_terrain_area"),
                        avg("AREA CONSTRUIDA").alias("avg_built_area")
                    ) \
                    .orderBy("ano do exercício", col("total_imoveis").desc())
                
                gold_neighborhood = gold_neighborhood.withColumn("_gold_timestamp", lit(datetime.now()))
                gold_neighborhood = gold_neighborhood.withColumn("_output_type", lit("summary_by_neighborhood"))
                
                gold_path_neighborhood = GOLD_DIR / "gold_summary_by_neighborhood"
                self._save_to_gold(gold_neighborhood, gold_path_neighborhood)
                gold_outputs["summary_by_neighborhood"] = gold_neighborhood
                logger.info(f"[OK] Saved to: {gold_path_neighborhood}")
            
            # Gold 3: Year-over-Year Trends
            logger.info("Creating gold output: Year-over-Year Trends")
            if "ano do exercício" in columns:
                gold_trends = silver_df.groupBy("ano do exercício") \
                    .agg(
                        count("*").alias("total_imoveis"),
                        sum("valor IPTU").alias("total_iptu_value"),
                        avg("valor IPTU").alias("avg_iptu_value"),
                        sum("AREA TERRENO").alias("total_terrain_area"),
                        sum("AREA CONSTRUIDA").alias("total_built_area")
                    ) \
                    .orderBy("ano do exercício")
                
                gold_trends = gold_trends.withColumn("_gold_timestamp", lit(datetime.now()))
                gold_trends = gold_trends.withColumn("_output_type", lit("year_over_year_trends"))
                
                gold_path_trends = GOLD_DIR / "gold_year_over_year_trends"
                self._save_to_gold(gold_trends, gold_path_trends)
                gold_outputs["year_over_year_trends"] = gold_trends
                logger.info(f"[OK] Saved to: {gold_path_trends}")
            
        else:
            # Pandas implementation
            import pandas as pd
            
            # Get columns
            columns = self.engine.get_columns(silver_df)
            
            # Gold 1: Summary by Year and Property Type
            logger.info("Creating gold output: Summary by Year and Property Type")
            if "ano do exercício" in columns and "tipo de uso do imóvel" in columns:
                gold_summary = silver_df.groupby(["ano do exercício", "tipo de uso do imóvel"]).agg({
                    "valor IPTU": ["count", "sum", "mean", "min", "max"]
                }).reset_index()
                gold_summary.columns = ["ano do exercício", "tipo de uso do imóvel", 
                                       "total_imoveis", "total_iptu_value", "avg_iptu_value",
                                       "min_iptu_value", "max_iptu_value"]
                gold_summary = gold_summary.sort_values(["ano do exercício", "total_imoveis"], ascending=[True, False])
                gold_summary["_gold_timestamp"] = pd.Timestamp.now()
                gold_summary["_output_type"] = "summary_by_year_type"
                
                gold_path_summary = GOLD_DIR / "gold_summary_by_year_type.parquet"
                self._save_to_gold(gold_summary, gold_path_summary)
                gold_outputs["summary_by_year_type"] = gold_summary
                logger.info(f"[OK] Saved to: {gold_path_summary}")
            
            logger.warning("Pandas gold layer aggregation partially implemented. Consider using PySpark for full features.")
        
        return gold_outputs
    
    def _get_bronze_path(self, year: int) -> Path:
        """Get bronze layer path for a specific year."""
        return BRONZE_DIR / f"iptu_{year}"
    
    def _year_exists_in_bronze(self, year: int) -> bool:
        """Check if year exists in bronze layer."""
        bronze_path = self._get_bronze_path(year)
        return self._path_exists(bronze_path)
    
    def _path_exists(self, path: Path) -> bool:
        """Check if a data path exists (Delta table or Parquet directory)."""
        if self.engine.engine_type == "pyspark":
            # For Delta, check for _delta_log directory
            delta_log = path / "_delta_log"
            if delta_log.exists():
                return True
            # For Parquet, check for directory and parquet files (including partitioned subdirectories)
            if not path.exists():
                return False
            # Check for parquet files directly or in subdirectories (partitioned)
            parquet_files = list(path.glob("**/*.parquet"))
            if parquet_files:
                return True
            # Also check for subdirectories (partitioned data)
            subdirs = [d for d in path.iterdir() if d.is_dir()]
            return len(subdirs) > 0
        else:
            # For Parquet, check for directory or parquet files
            if path.suffix == ".parquet":
                return path.exists()
            if not path.exists():
                return False
            return any(path.glob("*.parquet")) or any(path.glob("**/*.parquet"))
    
    def _save_to_bronze(self, df, path: Path):
        """Save DataFrame to bronze layer."""
        if self.engine.engine_type == "pyspark":
            try:
                # Try to save as Delta table with column mapping enabled for special characters
                # Setting delta.columnMapping.mode to 'name' enables column mapping automatically
                df.write.format("delta") \
                    .mode("overwrite") \
                    .option("delta.columnMapping.mode", "name") \
                    .partitionBy("_year_partition") \
                    .save(str(path))
                logger.debug(f"Saved as Delta table: {path}")
            except Exception as e:
                logger.warning(f"Delta write failed, using Parquet: {str(e)}")
                df.write.format("parquet").mode("overwrite").partitionBy("_year_partition").save(str(path))
        else:
            # Pandas: save as Parquet
            path.mkdir(parents=True, exist_ok=True)
            self.engine.write_parquet(df, path / "data.parquet")
    
    def _save_to_silver(self, df, path: Path):
        """Save DataFrame to silver layer."""
        if self.engine.engine_type == "pyspark":
            try:
                # Try to save as Delta table with column mapping enabled for special characters
                # Setting delta.columnMapping.mode to 'name' enables column mapping automatically
                df.write.format("delta") \
                    .mode("overwrite") \
                    .option("delta.columnMapping.mode", "name") \
                    .save(str(path))
                logger.debug(f"Saved as Delta table: {path}")
            except Exception as e:
                logger.warning(f"Delta write failed, using Parquet: {str(e)}")
                df.write.format("parquet").mode("overwrite").save(str(path))
        else:
            # Pandas: save as Parquet
            path.parent.mkdir(parents=True, exist_ok=True)
            if path.suffix == ".parquet":
                self.engine.write_parquet(df, path)
            else:
                path.mkdir(parents=True, exist_ok=True)
                self.engine.write_parquet(df, path / "data.parquet")
    
    def _save_to_gold(self, df, path: Path):
        """Save DataFrame to gold layer."""
        if self.engine.engine_type == "pyspark":
            # Gold layer uses Parquet for better compatibility
            df.write.format("parquet").mode("overwrite").save(str(path))
        else:
            # Pandas: save as Parquet
            path.parent.mkdir(parents=True, exist_ok=True)
            if path.suffix == ".parquet":
                self.engine.write_parquet(df, path)
            else:
                path.mkdir(parents=True, exist_ok=True)
                self.engine.write_parquet(df, path / "data.parquet")
    
    def _load_from_bronze(self, path: Path):
        """Load DataFrame from bronze layer."""
        if self.engine.engine_type == "pyspark":
            try:
                # Try to load as Delta table
                return self.engine.spark.read.format("delta").load(str(path))
            except Exception:
                # Fall back to Parquet
                return self.engine.read_parquet(path)
        else:
            return self.engine.read_parquet(path)
    
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
            "analysis_summary": None,
            "medallion_summary": None
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
        
        if self.medallion_quality:
            medallion_summary = self.medallion_quality.get_summary()
            summary["medallion_summary"] = medallion_summary
        
        return summary

