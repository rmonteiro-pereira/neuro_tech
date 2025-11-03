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
        logger.info(f"[PyDeequ] Starting data quality validation for year {year} - Bronze Layer")
        
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
                from pydeequ.profiles import ColumnProfilerRunner
                
                # ColumnProfilerRunner expects raw SparkSession, not PyDeequSession
                # PyDeequSession is only needed for certain operations, not profiling
                runner = ColumnProfilerRunner(self.spark).onData(df)
                profile_result = runner.run()
                
                # Extract profile information (API may return dict or DataFrame)
                total_nulls = 0
                
                if hasattr(profile_result, 'profiles'):
                    # Older PyDeequ API: profile_result.profiles is a dict
                    for column_name, profile in profile_result.profiles.items():
                        # Completeness is 0.0-1.0, where 1.0 means no nulls
                        completeness = getattr(profile, 'completeness', 1.0)
                        null_count = int((1.0 - completeness) * row_count) if completeness < 1.0 else 0
                        
                        if null_count > 0:
                            null_pct = (null_count / row_count) * 100
                            if null_pct > 50:
                                results["warnings"].append(
                                    f"Column '{column_name}' has {null_pct:.1f}% null values"
                                )
                            total_nulls += null_count
                elif hasattr(profile_result, 'collect'):
                    # Newer PyDeequ API: profile_result is a DataFrame
                    profile_rows = profile_result.collect()
                    for row in profile_rows:
                        column_name = row.get("column", row.get("Column", "unknown"))
                        # Handle different column names in the profile DataFrame
                        completeness = row.get("completeness", row.get("Completeness", 1.0))
                        if isinstance(completeness, (int, float)):
                            null_count = int((1.0 - completeness) * row_count) if completeness < 1.0 else 0
                        else:
                            # If completeness is already a count
                            null_count = row_count - int(completeness) if isinstance(completeness, (int, float)) else 0
                        
                        if null_count > 0:
                            null_pct = (null_count / row_count) * 100
                            if null_pct > 50:
                                results["warnings"].append(
                                    f"Column '{column_name}' has {null_pct:.1f}% null values"
                                )
                            total_nulls += null_count
                else:
                    logger.warning(f"Unknown profile result format: {type(profile_result)}")
                
                results["metrics"]["total_null_count"] = total_nulls
                results["metrics"]["null_percentage"] = (total_nulls / (row_count * col_count)) * 100 if (row_count * col_count) > 0 else 0
                
                # Log PyDeequ profiling success with metrics
                null_pct = results["metrics"]["null_percentage"]
                logger.info(
                    f"[PyDeequ] Profiling completed successfully for year {year} - Bronze Layer "
                    f"(Rows: {row_count:,}, Columns: {col_count}, Null%: {null_pct:.2f}%)"
                )
                
            except Exception as e:
                logger.warning(f"PyDeequ profiling failed, using basic checks: {str(e)}")
                # Fallback to basic null count (skip isnan for non-numeric types)
                from pyspark.sql.functions import col, isnan, isnull, when, count
                from pyspark.sql.types import DateType, TimestampType, StringType
                
                # Get column types
                schema = df.schema
                column_checks = []
                for field in schema:
                    col_name = field.name
                    col_type = field.dataType
                    # Only use isnan for numeric types, not for DATE, TIMESTAMP, or STRING
                    if isinstance(col_type, (DateType, TimestampType, StringType)):
                        column_checks.append(count(when(col(col_name).isNull(), col(col_name))).alias(col_name))
                    else:
                        # For numeric types, check both null and nan
                        column_checks.append(count(when(col(col_name).isNull() | isnan(col(col_name)), col(col_name))).alias(col_name))
                
                null_counts = df.select(column_checks).collect()[0]
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
            logger.error(f"[FAIL] PyDeequ Data Quality Validation FAILED for year {year} - Bronze Layer: {str(e)}")
            results["errors"].append(f"Validation error: {str(e)}")
            results["passed"] = False
        
        # Final pass/fail message before returning
        if results["passed"]:
            row_count = results["metrics"].get("row_count", 0)
            col_count = results["metrics"].get("column_count", 0)
            null_pct = results["metrics"].get("null_percentage", 0)
            error_count = len(results["errors"])
            logger.info(
                f"[PASS] PyDeequ Data Quality Validation PASSED for year {year} - Bronze Layer "
                f"(Rows: {row_count:,}, Columns: {col_count}, Null%: {null_pct:.2f}%, Errors: {error_count})"
            )
        else:
            error_count = len(results["errors"])
            logger.error(
                f"[FAIL] PyDeequ Data Quality Validation FAILED for year {year} - Bronze Layer: {error_count} errors"
            )
        
        self.validation_results.append(results)
        return results
    
    def validate_silver_layer(
        self,
        df,
        year: Optional[int] = None,
        additional_checks: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Validate silver layer (cleaned and transformed data).
        More comprehensive checks including business rules.
        
        Args:
            df: Spark DataFrame
            year: Optional year of the dataset. If None, validates consolidated (multi-year) data.
            additional_checks: Optional dictionary with custom validation rules
        
        Returns:
            Dictionary with validation results
        """
        if year is None:
            logger.info("[PyDeequ] Starting data quality validation - Silver Layer (Consolidated)")
        else:
            logger.info(f"[PyDeequ] Starting data quality validation for year {year} - Silver Layer")
        
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
            
            # Validate year consistency (only if a specific year is provided)
            if "ano do exercício" in df.columns:
                from pyspark.sql.functions import collect_set
                years = df.select(collect_set("ano do exercício")).collect()[0][0]
                
                if year is not None:
                    # Single-year validation: should match the specified year
                    if len(years) > 1 or (years and years[0] != year):
                        results["errors"].append(
                            f"Year mismatch: expected {year}, found {years}"
                        )
                        results["passed"] = False
                else:
                    # Consolidated data validation: should have multiple years
                    if years:
                        results["metrics"]["years_in_data"] = sorted(years)
                        results["metrics"]["year_count"] = len(years)
                        logger.info(f"Silver layer contains data from {len(years)} year(s): {sorted(years)}")
                    else:
                        results["warnings"].append("No year data found in 'ano do exercício' column")
            
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
            if year is None:
                logger.error(f"[FAIL] PyDeequ Data Quality Validation FAILED - Silver Layer (Consolidated): {str(e)}")
            else:
                logger.error(f"[FAIL] PyDeequ Data Quality Validation FAILED for year {year} - Silver Layer: {str(e)}")
            results["errors"].append(f"Validation error: {str(e)}")
            results["passed"] = False
        
        # Final pass/fail message before returning
        if results["passed"]:
            row_count = results["metrics"].get("row_count", 0)
            col_count = results["metrics"].get("column_count", 0)
            duplicate_pct = results["metrics"].get("duplicate_percentage", 0)
            error_count = len(results["errors"])
            
            if year is None:
                logger.info(
                    f"[PASS] PyDeequ Data Quality Validation PASSED - Silver Layer (Consolidated) "
                    f"(Rows: {row_count:,}, Columns: {col_count}, Duplicate%: {duplicate_pct:.2f}%, Errors: {error_count})"
                )
            else:
                logger.info(
                    f"[PASS] PyDeequ Data Quality Validation PASSED for year {year} - Silver Layer "
                    f"(Rows: {row_count:,}, Columns: {col_count}, Duplicate%: {duplicate_pct:.2f}%, Errors: {error_count})"
                )
        else:
            error_count = len(results["errors"])
            if year is None:
                logger.error(f"[FAIL] PyDeequ Data Quality Validation FAILED - Silver Layer (Consolidated): {error_count} errors")
            else:
                logger.error(f"[FAIL] PyDeequ Data Quality Validation FAILED for year {year} - Silver Layer: {error_count} errors")
        
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
        logger.info(f"[PyDeequ] Starting data quality validation - Gold Layer ({layer_name})")
        
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
            
            # Check for null values in key columns (skip isnan for non-numeric types)
            from pyspark.sql.functions import col, count, when, isnan, isnull
            from pyspark.sql.types import DateType, TimestampType, StringType
            
            # Get column types
            schema = df.schema
            column_checks = []
            for field in schema:
                col_name = field.name
                col_type = field.dataType
                # Only use isnan for numeric types, not for DATE, TIMESTAMP, or STRING
                if isinstance(col_type, (DateType, TimestampType, StringType)):
                    column_checks.append(count(when(col(col_name).isNull(), col(col_name))).alias(col_name))
                else:
                    # For numeric types, check both null and nan
                    column_checks.append(count(when(col(col_name).isNull() | isnan(col(col_name)), col(col_name))).alias(col_name))
            
            null_counts = df.select(column_checks).collect()[0]
            
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
            logger.error(f"[FAIL] PyDeequ Data Quality Validation FAILED - Gold Layer ({layer_name}): {str(e)}")
            results["errors"].append(f"Validation error: {str(e)}")
            results["passed"] = False
        
        # Final pass/fail message before returning
        if results["passed"]:
            row_count = results["metrics"].get("row_count", 0)
            col_count = results["metrics"].get("column_count", 0)
            duplicate_count = results["metrics"].get("duplicate_count", 0)
            error_count = len(results["errors"])
            logger.info(
                f"[PASS] PyDeequ Data Quality Validation PASSED - Gold Layer ({layer_name}) "
                f"(Rows: {row_count:,}, Columns: {col_count}, Duplicates: {duplicate_count}, Errors: {error_count})"
            )
        else:
            error_count = len(results["errors"])
            logger.error(f"[FAIL] PyDeequ Data Quality Validation FAILED - Gold Layer ({layer_name}): {error_count} errors")
        
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


