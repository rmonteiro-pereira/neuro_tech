"""
Medallion architecture data quality module using PyDeequ for Spark-based validation.
"""
from typing import Dict, List, Optional, Any
from pathlib import Path
from datetime import datetime
import json

from iptu_pipeline.utils.logger import setup_logger

logger = setup_logger("medallion_quality")


class MedallionDataQuality:
    """
    Data quality checker for medallion architecture using PyDeequ.
    Compatible with Apache Spark DataFrames.
    """
    
    def __init__(self, spark_session):
        """
        Initialize MedallionDataQuality with a Spark session.
        
        Args:
            spark_session: Active SparkSession instance
        """
        self.spark = spark_session
        self.validation_results: List[Dict[str, Any]] = []
        
    def validate_bronze_layer(
        self,
        df,
        year: int,
        source_path: Optional[Path] = None
    ) -> Dict[str, Any]:
        """
        Validate bronze layer (raw ingested data).
        
        Args:
            df: Spark DataFrame
            year: Year of the dataset
            source_path: Source file path (for logging)
        
        Returns:
            Dictionary with validation results
        """
        logger.info(f"Validating bronze layer for year {year}")
        
        results = {
            "layer": "bronze",
            "year": year,
            "source_path": str(source_path) if source_path else None,
            "timestamp": datetime.now().isoformat(),
            "passed": True,
            "errors": [],
            "warnings": [],
            "metrics": {}
        }
        
        try:
            # Basic structure checks
            row_count = df.count()
            col_count = len(df.columns)
            results["metrics"]["row_count"] = row_count
            results["metrics"]["column_count"] = col_count
            
            # Check if empty
            if row_count == 0:
                results["errors"].append("Dataset is empty (0 rows)")
                results["passed"] = False
            elif row_count < 100:
                results["warnings"].append(f"Low row count: {row_count} (expected minimum 100)")
            
            # Check for null counts using PyDeequ
            try:
                from pydeequ import PyDeequSession
                from pydeequ.profiles import ColumnProfilerRunner
                
                spark_session = PyDeequSession(self.spark)
                runner = ColumnProfilerRunner(spark_session).onData(df)
                profile_df = runner.run()
                
                # Extract profile information
                profile_rows = profile_df.collect()
                total_nulls = 0
                
                for row in profile_rows:
                    column_name = row["column"]
                    null_count = row.get("completeness", 0)
                    if null_count > 0:
                        null_pct = (null_count / row_count) * 100
                        if null_pct > 50:
                            results["warnings"].append(
                                f"Column '{column_name}' has {null_pct:.1f}% null values"
                            )
                        total_nulls += null_count
                
                results["metrics"]["total_null_count"] = total_nulls
                results["metrics"]["null_percentage"] = (total_nulls / (row_count * col_count)) * 100
                
            except Exception as e:
                logger.warning(f"PyDeequ profiling failed, using basic checks: {str(e)}")
                # Fallback to basic null count
                from pyspark.sql.functions import col, isnan, isnull, when, count
                null_counts = df.select(
                    [count(when(col(c).isNull() | isnan(c), c)).alias(c) for c in df.columns]
                ).collect()[0]
                total_nulls = sum(null_counts)
                results["metrics"]["total_null_count"] = total_nulls
                results["metrics"]["null_percentage"] = (total_nulls / (row_count * col_count)) * 100
                
                # Check individual columns
                for col_name, null_count in null_counts.asDict().items():
                    if null_count > 0:
                        null_pct = (null_count / row_count) * 100
                        if null_pct > 50:
                            results["warnings"].append(
                                f"Column '{col_name}' has {null_pct:.1f}% null values"
                            )
            
        except Exception as e:
            logger.error(f"Bronze layer validation failed: {str(e)}")
            results["errors"].append(f"Validation error: {str(e)}")
            results["passed"] = False
        
        self.validation_results.append(results)
        return results
    
    def validate_silver_layer(
        self,
        df,
        year: int,
        additional_checks: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Validate silver layer (cleaned and transformed data).
        More comprehensive checks including business rules.
        
        Args:
            df: Spark DataFrame
            year: Year of the dataset
            additional_checks: Optional dictionary with custom validation rules
        
        Returns:
            Dictionary with validation results
        """
        logger.info(f"Validating silver layer for year {year}")
        
        results = {
            "layer": "silver",
            "year": year,
            "timestamp": datetime.now().isoformat(),
            "passed": True,
            "errors": [],
            "warnings": [],
            "metrics": {}
        }
        
        try:
            # Basic structure checks
            row_count = df.count()
            col_count = len(df.columns)
            results["metrics"]["row_count"] = row_count
            results["metrics"]["column_count"] = col_count
            
            # Check for duplicates
            from pyspark.sql.functions import col, count
            key_cols = ["Número do contribuinte", "ano do exercício"]
            existing_cols = [c for c in key_cols if c in df.columns]
            
            if existing_cols:
                duplicate_count = df.groupBy(existing_cols).count().filter("count > 1").count()
                duplicate_pct = (duplicate_count / row_count * 100) if row_count > 0 else 0
                results["metrics"]["duplicate_count"] = duplicate_count
                results["metrics"]["duplicate_percentage"] = duplicate_pct
                
                if duplicate_count > 0:
                    results["warnings"].append(
                        f"Found {duplicate_count} duplicate rows ({duplicate_pct:.1f}%)"
                    )
            
            # Validate year consistency
            if "ano do exercício" in df.columns:
                from pyspark.sql.functions import collect_set
                years = df.select(collect_set("ano do exercício")).collect()[0][0]
                if len(years) > 1 or (years and years[0] != year):
                    results["errors"].append(
                        f"Year mismatch: expected {year}, found {years}"
                    )
                    results["passed"] = False
            
            # Validate CEP format (should be numeric, 8 digits)
            if "CEP" in df.columns:
                from pyspark.sql.functions import when, length, col, regexp_replace, isnan, isnull
                
                # Convert to string and check length
                invalid_cep = df.filter(
                    ~((col("CEP").cast("string").rlike("^[0-9]{8}$") | col("CEP").isNull()))
                ).count()
                
                results["metrics"]["invalid_cep_count"] = invalid_cep
                if invalid_cep > 0:
                    results["warnings"].append(f"Found {invalid_cep} invalid CEP values")
            
            # Validate city (should be Recife)
            if "cidade" in df.columns:
                from pyspark.sql.functions import collect_set, upper
                cities = df.select(collect_set(upper("cidade"))).collect()[0][0]
                if "RECIFE" not in cities:
                    results["warnings"].append(f"Unexpected city values: {cities}")
            
            # Validate estado (should be PE)
            if "estado" in df.columns:
                from pyspark.sql.functions import collect_set
                estados = df.select(collect_set("estado")).collect()[0][0]
                if "PE" not in estados:
                    results["warnings"].append(f"Unexpected estado values: {estados}")
            
            # Custom business rules
            if additional_checks:
                for check_name, check_func in additional_checks.items():
                    try:
                        check_result = check_func(df)
                        results["metrics"][check_name] = check_result
                    except Exception as e:
                        results["warnings"].append(f"Custom check '{check_name}' failed: {str(e)}")
            
        except Exception as e:
            logger.error(f"Silver layer validation failed: {str(e)}")
            results["errors"].append(f"Validation error: {str(e)}")
            results["passed"] = False
        
        self.validation_results.append(results)
        return results
    
    def validate_gold_layer(
        self,
        df,
        layer_name: str = "gold"
    ) -> Dict[str, Any]:
        """
        Validate gold layer (business-ready aggregated data).
        
        Args:
            df: Spark DataFrame
            layer_name: Name of the gold layer (e.g., "gold", "analysis")
        
        Returns:
            Dictionary with validation results
        """
        logger.info(f"Validating {layer_name} layer")
        
        results = {
            "layer": layer_name,
            "timestamp": datetime.now().isoformat(),
            "passed": True,
            "errors": [],
            "warnings": [],
            "metrics": {}
        }
        
        try:
            # Basic structure checks
            row_count = df.count()
            col_count = len(df.columns)
            results["metrics"]["row_count"] = row_count
            results["metrics"]["column_count"] = col_count
            
            # Check for null values in key columns
            from pyspark.sql.functions import col, count, when, isnan, isnull
            
            null_counts = df.select(
                [count(when(col(c).isNull() | isnan(c), c)).alias(c) for c in df.columns]
            ).collect()[0]
            
            for col_name, null_count in null_counts.asDict().items():
                if null_count > 0:
                    null_pct = (null_count / row_count) * 100
                    results["metrics"][f"{col_name}_null_pct"] = null_pct
                    if null_pct > 20:  # Stricter threshold for gold layer
                        results["warnings"].append(
                            f"Column '{col_name}' has {null_pct:.1f}% null values"
                        )
            
            # Validate no duplicates in aggregated data
            duplicate_count = df.groupBy(df.columns).count().filter("count > 1").count()
            results["metrics"]["duplicate_count"] = duplicate_count
            
            if duplicate_count > 0:
                results["errors"].append(f"Found {duplicate_count} duplicate rows in aggregated data")
                results["passed"] = False
            
        except Exception as e:
            logger.error(f"{layer_name} layer validation failed: {str(e)}")
            results["errors"].append(f"Validation error: {str(e)}")
            results["passed"] = False
        
        self.validation_results.append(results)
        return results
    
    def save_validation_report(self, output_path: Path):
        """
        Save validation results to JSON file.
        
        Args:
            output_path: Path to save the report
        """
        if not self.validation_results:
            logger.warning("No validation results to save")
            return
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(self.validation_results, f, indent=2, default=str)
        
        logger.info(f"Validation report saved to {output_path}")
    
    def get_summary(self) -> Dict[str, Any]:
        """
        Get summary of all validation results.
        
        Returns:
            Dictionary with validation summary
        """
        if not self.validation_results:
            return {"total_validations": 0, "passed": 0, "failed": 0}
        
        passed = sum(1 for r in self.validation_results if r["passed"])
        failed = len(self.validation_results) - passed
        
        return {
            "total_validations": len(self.validation_results),
            "passed": passed,
            "failed": failed,
            "success_rate": (passed / len(self.validation_results) * 100) if self.validation_results else 0
        }


