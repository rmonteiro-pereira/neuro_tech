"""
Medallion architecture pipeline implementation.
Organizes data into Raw (source) -> Bronze (cleaned/cataloged) -> Silver (consolidated) -> Gold (refined) layers.
"""
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime

from iptu_pipeline.engine import DataEngine
from iptu_pipeline.config import settings, BRONZE_DIR, SILVER_DIR, GOLD_DIR, DATA_DIR
from iptu_pipeline.utils.logger import setup_logger
from iptu_pipeline.utils.medallion_quality import MedallionDataQuality
from iptu_pipeline.pipelines.transformation import DataTransformer

logger = setup_logger("medallion_pipeline")


class MedallionPipeline:
    """
    Implements medallion architecture for IPTU data processing.
    
    Architecture:
    - Raw: Source files (referenced, not stored in medallion)
    - Bronze: Cleaned and cataloged data per year (Delta/Parquet format)
    - Silver: Consolidated dataset (all years merged, standardized)
    - Gold: Refined business-ready outputs (aggregations, summaries, analytics)
    """
    
    def __init__(self, engine: Optional[DataEngine] = None):
        """
        Initialize medallion pipeline.
        
        Args:
            engine: Optional data engine instance (Pandas or PySpark)
        """
        if engine is None:
            engine = DataEngine(engine_type=getattr(settings, 'DATA_ENGINE', 'pandas'))
        
        self.engine = engine
        
        # PySpark-specific setup
        self.spark_session = None
        if self.engine.engine_type == "pyspark":
            self.spark_session = self.engine.spark
            logger.info("Delta Lake support enabled (if configured)")
        
        # Initialize quality checker
        if self.spark_session:
            self.quality_checker = MedallionDataQuality(self.spark_session)
        else:
            self.quality_checker = None
        
        # Initialize transformer
        self.transformer = DataTransformer(engine=self.engine)
    
    def ingest_to_bronze(
        self,
        years: Optional[List[int]] = None,
        incremental: bool = False
    ) -> Dict[int, Any]:
        """
        Ingest and clean raw data to bronze layer (cleaned and cataloged per year).
        
        Bronze layer: Minimal cleaning, normalization, and cataloging.
        - Normalize column names
        - Handle missing columns (schema evolution support)
        - Standardize data types
        - Add metadata (ingestion timestamp, source file, year)
        - Catalog structure
        
        Args:
            years: List of years to ingest. If None, ingests all years.
            incremental: If True, only ingest new years not in bronze layer.
        
        Returns:
            Dictionary mapping years to bronze DataFrames
        """
        logger.info("\n" + "=" * 80)
        logger.info("[BRONZE LAYER] Cleaning and Cataloging Raw Data")
        logger.info("=" * 80)
        
        if years is None:
            years = sorted(settings.CSV_YEARS + settings.JSON_YEARS)
        
        ingested_dfs = {}
        
        for year in years:
            # Check if already in bronze layer (incremental mode)
            if incremental and self._year_exists_in_bronze(year):
                logger.info(f"Year {year} already in bronze layer, skipping")
                continue
            
            logger.info(f"\nProcessing year {year} to bronze layer")
            
            # Load raw data from source files
            file_path = settings.data_paths.get(year)
            if not file_path or not file_path.exists():
                logger.error(f"Raw data file not found for year {year}: {file_path}")
                continue
            
            logger.info(f"Reading raw data from: {file_path}")
            
            # Load data based on file type
            if year in settings.CSV_YEARS:
                df = self.engine.read_csv(file_path, sep=';', encoding='utf-8', low_memory=False)
            elif year in settings.JSON_YEARS:
                df = self.engine.read_json(file_path)
            else:
                logger.error(f"Unknown file type for year {year}")
                continue
            
            # Bronze layer cleaning: normalize column names, handle missing columns, standardize types
            logger.info(f"Cleaning and cataloging data for year {year}")
            df = self.transformer.normalize_column_names(df, year)
            df, _ = self.transformer.handle_missing_columns(df, year)
            df = self.transformer.standardize_data_types(df, year)
            
            # Add bronze layer metadata
            if self.engine.engine_type == "pyspark":
                from pyspark.sql.functions import current_timestamp, lit
                df = df.withColumn("_bronze_ingestion_timestamp", current_timestamp())
                df = df.withColumn("_source_file", lit(str(file_path)))
                df = df.withColumn("_year_partition", lit(year))
                df = df.withColumn("_data_layer", lit("bronze"))
            else:
                import pandas as pd
                df["_bronze_ingestion_timestamp"] = pd.Timestamp.now()
                df["_source_file"] = str(file_path)
                df["_year_partition"] = year
                df["_data_layer"] = "bronze"
            
            # Save to bronze layer
            bronze_path = self._get_bronze_path(year)
            self._save_to_bronze(df, bronze_path)
            logger.info(f"[OK] Year {year} saved to bronze layer: {bronze_path}")
            
            # Validate bronze layer
            if self.quality_checker:
                self.quality_checker.validate_bronze_layer(df, year, file_path)
            
            ingested_dfs[year] = df
        
        logger.info(f"\n[OK] Bronze layer complete: {len(ingested_dfs)} years cataloged")
        return ingested_dfs
    
    def process_to_silver(
        self,
        years: Optional[List[int]] = None,
        incremental: bool = False
    ) -> Any:
        """
        Consolidate bronze layer data into silver layer (single unified dataset).
        
        Silver layer: Consolidated, standardized, and validated data.
        - Load all years from bronze
        - Merge into single consolidated dataset
        - Apply final standardization
        - Validate data quality
        
        Args:
            years: List of years to process. If None, processes all years from bronze.
            incremental: If True, only process new years not in silver layer.
        
        Returns:
            Consolidated DataFrame with all years
        """
        logger.info("\n" + "=" * 80)
        logger.info("[SILVER LAYER] Consolidating Data (All Years)")
        logger.info("=" * 80)
        
        if years is None:
            years = sorted(settings.CSV_YEARS + settings.JSON_YEARS)
        
        silver_dataframes = []
        
        for year in years:
            # Check if already processed (incremental mode)
            if incremental and self._year_exists_in_silver(year):
                logger.info(f"Year {year} already in silver layer, skipping")
                continue
            
            # Load from bronze layer
            bronze_path = self._get_bronze_path(year)
            if not self._path_exists(bronze_path):
                logger.warning(f"Year {year} not found in bronze layer, skipping silver processing")
                continue
            
            logger.info(f"Loading year {year} from bronze layer")
            df = self._load_from_bronze(bronze_path)
            
            # Apply any additional cleaning for silver layer
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
            logger.error("No data found in bronze layer to consolidate")
            raise RuntimeError("No data to consolidate in silver layer")
        
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
        
        # Validate silver layer
        if self.quality_checker:
            self.quality_checker.validate_silver_layer(consolidated_df, None)  # No specific year for consolidated
        
        logger.info("\n[OK] Silver layer complete (consolidated dataset)")
        return consolidated_df
    
    def aggregate_to_gold(
        self,
        silver_df: Optional[Any] = None
    ) -> Dict[str, Any]:
        """
        Create refined business-ready outputs in gold layer.
        
        Gold layer: Business-ready aggregations and analytics.
        - Aggregations by year, type, location
        - Summary statistics
        - Business metrics
        - Multiple output files for different use cases
        
        Args:
            silver_df: Optional consolidated silver DataFrame. If None, loads from silver layer.
        
        Returns:
            Dictionary of gold layer DataFrames (different outputs)
        """
        logger.info("\n" + "=" * 80)
        logger.info("[GOLD LAYER] Creating Refined Business-Ready Outputs")
        logger.info("=" * 80)
        
        # Load from silver if not provided
        if silver_df is None:
            silver_path = SILVER_DIR / "iptu_silver_consolidated"
            if not self._path_exists(silver_path):
                logger.error("Silver layer not found. Run silver layer first.")
                raise RuntimeError("Silver layer data not found")
            
            logger.info("Loading consolidated data from silver layer")
            silver_df = self._load_from_silver(silver_path)
        
        gold_outputs = {}
        
        if self.engine.engine_type == "pyspark":
            from pyspark.sql.functions import col, count, sum, avg, min, max, lit
            
            # Gold 1: Summary by Year and Property Type
            logger.info("Creating gold output: Summary by Year and Property Type")
            if "ano do exercício" in silver_df.columns and "tipo de uso do imóvel" in silver_df.columns:
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
            if "bairro" in silver_df.columns:
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
            if "ano do exercício" in silver_df.columns:
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
            
            # Gold 4: Full consolidated dataset (for advanced analytics)
            logger.info("Creating gold output: Full Consolidated Dataset")
            gold_full = silver_df.withColumn("_gold_timestamp", lit(datetime.now()))
            gold_full = gold_full.withColumn("_output_type", lit("full_consolidated"))
            
            gold_path_full = GOLD_DIR / "gold_full_consolidated"
            self._save_to_gold(gold_full, gold_path_full)
            gold_outputs["full_consolidated"] = gold_full
            logger.info(f"[OK] Saved to: {gold_path_full}")
            
        else:
            # Pandas implementation
            import pandas as pd
            
            # Gold 1: Summary by Year and Property Type
            logger.info("Creating gold output: Summary by Year and Property Type")
            if "ano do exercício" in silver_df.columns and "tipo de uso do imóvel" in silver_df.columns:
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
            
            # Similar implementations for other gold outputs...
            # (Pandas version simplified for brevity)
            logger.warning("Pandas gold layer aggregation partially implemented. Consider using PySpark for full features.")
        
        # Validate each gold output
        if self.quality_checker:
            for output_name, gold_df in gold_outputs.items():
                self.quality_checker.validate_gold_layer(gold_df, f"gold_{output_name}")
        
        logger.info(f"\n[OK] Gold layer complete: {len(gold_outputs)} refined outputs created")
        return gold_outputs
    
    def run_full_medallion_pipeline(
        self,
        years: Optional[List[int]] = None,
        incremental: bool = False
    ) -> Dict[str, Any]:
        """
        Run the complete medallion pipeline: Raw -> Bronze -> Silver -> Gold.
        
        Args:
            years: List of years to process. If None, processes all years.
            incremental: If True, only process new years.
        
        Returns:
            Dictionary with:
            - 'silver': Consolidated silver DataFrame
            - 'gold': Dictionary of gold layer outputs
        """
        logger.info("=" * 80)
        logger.info("Starting Medallion Architecture Pipeline")
        logger.info("=" * 80)
        logger.info(f"Raw data source: {DATA_DIR}")
        logger.info(f"Bronze layer: {BRONZE_DIR}")
        logger.info(f"Silver layer: {SILVER_DIR}")
        logger.info(f"Gold layer: {GOLD_DIR}")
        
        # Bronze layer: Clean and catalog raw data
        bronze_dfs = self.ingest_to_bronze(years=years, incremental=incremental)
        
        # Silver layer: Consolidate all years
        silver_df = self.process_to_silver(years=years, incremental=incremental)
        
        # Gold layer: Create refined outputs
        gold_outputs = self.aggregate_to_gold(silver_df=silver_df)
        
        # Generate validation report
        if self.quality_checker:
            validation_report_path = settings.OUTPUT_DIR / "medallion_validation_report.json"
            self.quality_checker.save_validation_report(validation_report_path)
            
            summary = self.quality_checker.get_summary()
            logger.info(f"\nValidation Summary: {summary['passed']}/{summary['total_validations']} passed")
        
        logger.info("=" * 80)
        logger.info("Medallion Architecture Pipeline Complete")
        logger.info("=" * 80)
        
        return {
            "silver": silver_df,
            "gold": gold_outputs
        }
    
    # Helper methods for path and file operations
    def _get_bronze_path(self, year: int) -> Path:
        """Get bronze layer path for a specific year."""
        return BRONZE_DIR / f"iptu_{year}"
    
    def _get_silver_path(self, year: int) -> Path:
        """Get silver layer path for a specific year (legacy, now consolidated)."""
        return SILVER_DIR / f"iptu_{year}"
    
    def _year_exists_in_bronze(self, year: int) -> bool:
        """Check if year exists in bronze layer."""
        bronze_path = self._get_bronze_path(year)
        return self._path_exists(bronze_path)
    
    def _year_exists_in_silver(self, year: int) -> bool:
        """Check if year exists in silver layer (legacy check)."""
        silver_path = SILVER_DIR / "iptu_silver_consolidated"
        return self._path_exists(silver_path)
    
    def _path_exists(self, path: Path) -> bool:
        """Check if a data path exists (Delta table or Parquet directory)."""
        if self.engine.engine_type == "pyspark":
            # For Delta, check for _delta_log directory
            delta_log = path / "_delta_log"
            if delta_log.exists():
                return True
            # For Parquet, check for directory or parquet files
            return path.exists() and any(path.glob("*.parquet"))
        else:
            # For Parquet, check for directory or parquet files
            if path.suffix == ".parquet":
                return path.exists()
            return path.exists() and any(path.glob("*.parquet"))
    
    def _save_to_bronze(self, df, path: Path):
        """Save DataFrame to bronze layer."""
        if self.engine.engine_type == "pyspark":
            try:
                # Try to save as Delta table
                df.write.format("delta").mode("overwrite").partitionBy("_year_partition").save(str(path))
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
                # Try to save as Delta table
                df.write.format("delta").mode("overwrite").save(str(path))
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
                return self.spark_session.read.format("delta").load(str(path))
            except Exception:
                # Fall back to Parquet
                return self.engine.read_parquet(path)
        else:
            return self.engine.read_parquet(path)
    
    def _load_from_silver(self, path: Path):
        """Load DataFrame from silver layer."""
        return self._load_from_bronze(path)  # Same loading logic
