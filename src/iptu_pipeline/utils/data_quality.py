"""
Data quality validation module for IPTU pipeline.
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from pathlib import Path
from datetime import datetime

from iptu_pipeline.config import QUALITY_THRESHOLDS
from iptu_pipeline.utils.logger import setup_logger

logger = setup_logger("data_quality")


class DataQualityValidator:
    """Validates data quality according to defined criteria."""
    
    def __init__(self, thresholds: Optional[Dict] = None, engine: Optional[str] = None):
        """
        Initialize validator with quality thresholds.
        
        Args:
            thresholds: Dictionary with quality thresholds. Uses default if None.
            engine: Optional engine type. Used for converting to Pandas if needed.
        """
        self.thresholds = thresholds or QUALITY_THRESHOLDS
        self.validation_results: List[Dict] = []
        self.engine = get_engine(engine) if engine else None
        
    def validate_dataset(
        self, 
        df, 
        year: int, 
        source_path: Optional[Path] = None
    ) -> Dict:
        """
        Validate a dataset against quality criteria.
        
        Args:
            df: DataFrame to validate
            year: Year of the dataset
            source_path: Path to source file (for logging)
        
        Returns:
            Dictionary with validation results
        """
        logger.info(f"Starting data quality validation for year {year}")
        
        # Convert to Pandas for validation (validation logic uses Pandas)
        if self.engine and self.engine.engine_type == "pyspark":
            df = self.engine.to_pandas(df)
        elif hasattr(df, 'toPandas'):  # Spark DataFrame
            df = df.toPandas()
        
        results = {
            "year": year,
            "source_path": str(source_path) if source_path else None,
            "timestamp": datetime.now().isoformat(),
            "passed": True,
            "errors": [],
            "warnings": [],
            "metrics": {}
        }
        
        # 1. Basic structure validation
        structure_result = self._validate_structure(df, year)
        results["metrics"].update(structure_result["metrics"])
        results["errors"].extend(structure_result["errors"])
        results["warnings"].extend(structure_result["warnings"])
        
        # 2. Row count validation
        row_result = self._validate_row_count(df, year)
        results["metrics"].update(row_result["metrics"])
        results["errors"].extend(row_result["errors"])
        
        # 3. Column validation
        col_result = self._validate_columns(df, year)
        results["metrics"].update(col_result["metrics"])
        results["errors"].extend(col_result["errors"])
        results["warnings"].extend(col_result["warnings"])
        
        # 4. Null values validation
        null_result = self._validate_null_values(df)
        results["metrics"].update(null_result["metrics"])
        results["warnings"].extend(null_result["warnings"])
        
        # 5. Duplicate validation
        dup_result = self._validate_duplicates(df)
        results["metrics"].update(dup_result["metrics"])
        results["warnings"].extend(dup_result["warnings"])
        
        # 6. Data type validation
        dtype_result = self._validate_data_types(df)
        results["metrics"].update(dtype_result["metrics"])
        results["warnings"].extend(dtype_result["warnings"])
        
        # 7. Business rules validation
        business_result = self._validate_business_rules(df, year)
        results["metrics"].update(business_result["metrics"])
        results["errors"].extend(business_result["errors"])
        results["warnings"].extend(business_result["warnings"])
        
        # Determine if validation passed
        results["passed"] = len(results["errors"]) == 0
        
        # Log results with details
        if results["passed"]:
            logger.info(f"[PASS] Validation PASSED for year {year}")
        else:
            logger.error(f"[FAIL] Validation FAILED for year {year}: {len(results['errors'])} errors")
            for error in results["errors"]:
                logger.error(f"  - {error}")
        
        if results["warnings"]:
            logger.warning(f"[WARN] {len(results['warnings'])} warnings for year {year}:")
            for warning in results["warnings"]:
                logger.warning(f"  - {warning}")
        
        self.validation_results.append(results)
        return results
    
    def _validate_structure(self, df: pd.DataFrame, year: int) -> Dict:
        """Validate basic structure of the dataset."""
        result = {"metrics": {}, "errors": [], "warnings": []}
        
        result["metrics"]["row_count"] = len(df)
        result["metrics"]["column_count"] = len(df.columns)
        result["metrics"]["memory_usage_mb"] = df.memory_usage(deep=True).sum() / 1024**2
        
        if len(df) == 0:
            result["errors"].append("Dataset is empty (0 rows)")
        
        if len(df.columns) == 0:
            result["errors"].append("Dataset has no columns")
        
        return result
    
    def _validate_row_count(self, df: pd.DataFrame, year: int) -> Dict:
        """Validate row count meets minimum threshold."""
        result = {"metrics": {}, "errors": []}
        
        min_rows = self.thresholds.get("min_rows_per_year", 100)
        row_count = len(df)
        
        result["metrics"]["min_rows_threshold"] = min_rows
        result["metrics"]["meets_min_rows"] = row_count >= min_rows
        
        if row_count < min_rows:
            result["errors"].append(
                f"Row count ({row_count}) below minimum threshold ({min_rows})"
            )
        
        return result
    
    def _validate_columns(self, df: pd.DataFrame, year: int) -> Dict:
        """Validate required columns are present."""
        from iptu_pipeline.utils.column_matcher import match_and_map_columns, KNOWN_COLUMN_MAPPINGS
        
        result = {"metrics": {}, "errors": [], "warnings": []}
        
        required_cols = set(self.thresholds.get("required_columns", []))
        actual_cols = set(df.columns)
        
        # Check for similar columns that could be matched
        similar_matches = match_and_map_columns(list(actual_cols), list(required_cols), threshold=0.7)
        fuzzy_mappings = {}
        
        for source_col, target_col in similar_matches.items():
            if target_col in required_cols and source_col not in actual_cols:
                # This is a column that exists with a different name
                fuzzy_mappings[source_col] = target_col
        
        missing_cols = required_cols - actual_cols
        extra_cols = actual_cols - required_cols
        
        # Check if missing columns have similar matches
        potentially_matched = {}
        for missing_col in list(missing_cols):
            for actual_col in actual_cols:
                from iptu_pipeline.utils.column_matcher import similarity_score
                score = similarity_score(missing_col, actual_col)
                if score >= 0.7 and actual_col not in required_cols:
                    potentially_matched[actual_col] = (missing_col, score)
                    break
        
        result["metrics"]["required_columns"] = len(required_cols)
        result["metrics"]["present_columns"] = len(actual_cols)
        result["metrics"]["missing_columns"] = list(missing_cols)
        result["metrics"]["extra_columns"] = list(extra_cols)
        result["metrics"]["fuzzy_mappings_found"] = potentially_matched
        
        # For 2024, _id is expected extra column
        if year == 2024 and "_id" in extra_cols:
            extra_cols = extra_cols - {"_id"}
        
        if missing_cols:
            # Check if any missing columns have known mappings (from KNOWN_COLUMN_MAPPINGS)
            from iptu_pipeline.utils.column_matcher import KNOWN_COLUMN_MAPPINGS
            
            fuzzy_candidates = {}
            for missing_col in missing_cols:
                # Check if this missing column is a target in known mappings
                for known_source, known_target in KNOWN_COLUMN_MAPPINGS.items():
                    if known_target == missing_col:
                        # Check if the source column exists in actual columns
                        for actual_col in actual_cols:
                            from iptu_pipeline.utils.column_matcher import similarity_score
                            score = similarity_score(known_source, actual_col)
                            if score >= 0.9:  # High threshold for known mappings
                                fuzzy_candidates[missing_col] = (actual_col, score)
                                break
                        break
            
            if fuzzy_candidates:
                warning_msg = (
                    f"Missing required columns: {', '.join(missing_cols)}. "
                    f"Similar columns found that will be renamed during transformation: "
                    f"{', '.join([f'{candidate[0]} -> {missing}' for missing, candidate in fuzzy_candidates.items()])}"
                )
                result["warnings"].append(warning_msg)
            else:
                result["errors"].append(
                    f"Missing required columns: {', '.join(missing_cols)}"
                )
        
        if extra_cols:
            # Check if extra columns match known mappings (will be renamed)
            from iptu_pipeline.utils.column_matcher import KNOWN_COLUMN_MAPPINGS
            
            similar_extra = {}
            for extra_col in list(extra_cols):
                # Check if this extra column is a source in known mappings
                for known_source, known_target in KNOWN_COLUMN_MAPPINGS.items():
                    from iptu_pipeline.utils.column_matcher import similarity_score
                    score = similarity_score(known_source, extra_col)
                    if score >= 0.9:  # High threshold for known mappings
                        similar_extra[extra_col] = (known_target, 1.0)
                        break
            
            if similar_extra:
                warning_msg = (
                    f"Extra columns with different names matching known mappings "
                    f"(will be renamed during transformation): "
                    f"{', '.join([f'{extra} -> {target[0]}' for extra, target in similar_extra.items()])}"
                )
                result["warnings"].append(warning_msg)
            
            # Still warn about truly extra columns
            truly_extra = extra_cols - set(similar_extra.keys())
            if truly_extra:
                result["warnings"].append(
                    f"Extra columns not in common schema: {', '.join(truly_extra)}"
                )
        
        return result
    
    def _validate_null_values(self, df: pd.DataFrame) -> Dict:
        """Validate null value percentages."""
        result = {"metrics": {}, "warnings": []}
        
        max_null_pct = self.thresholds.get("max_null_percentage", 50.0)
        null_counts = df.isnull().sum()
        null_percentages = (null_counts / len(df) * 100).round(2)
        
        high_null_cols = null_percentages[null_percentages > max_null_pct]
        
        result["metrics"]["max_null_threshold"] = max_null_pct
        result["metrics"]["total_null_count"] = null_counts.sum()
        result["metrics"]["columns_above_threshold"] = high_null_cols.to_dict()
        
        if len(high_null_cols) > 0:
            for col, pct in high_null_cols.items():
                result["warnings"].append(
                    f"Column '{col}' has {pct}% null values (threshold: {max_null_pct}%)"
                )
        
        return result
    
    def _validate_duplicates(self, df: pd.DataFrame) -> Dict:
        """Validate duplicate rows."""
        result = {"metrics": {}, "warnings": []}
        
        duplicate_count = df.duplicated().sum()
        duplicate_pct = (duplicate_count / len(df) * 100).round(2)
        
        result["metrics"]["duplicate_rows"] = int(duplicate_count)
        result["metrics"]["duplicate_percentage"] = duplicate_pct
        
        if duplicate_count > 0:
            result["warnings"].append(
                f"Found {duplicate_count} duplicate rows ({duplicate_pct}%)"
            )
        
        return result
    
    def _validate_data_types(self, df: pd.DataFrame) -> Dict:
        """Validate data types are appropriate."""
        result = {"metrics": {}, "warnings": []}
        
        # Check year column if present
        if "ano do exercício" in df.columns:
            if not pd.api.types.is_integer_dtype(df["ano do exercício"]):
                result["warnings"].append(
                    "Column 'ano do exercício' should be integer type"
                )
        
        # Check numeric columns that should be numeric
        numeric_cols = ["numero", "CEP", "Código Logradouro"]
        for col in numeric_cols:
            if col in df.columns:
                if not pd.api.types.is_numeric_dtype(df[col]):
                    result["warnings"].append(
                        f"Column '{col}' should be numeric type but is {df[col].dtype}"
                    )
        
        return result
    
    def _validate_business_rules(self, df: pd.DataFrame, year: int) -> Dict:
        """Validate business rules specific to IPTU data."""
        result = {"metrics": {}, "errors": [], "warnings": []}
        
        # Validate year consistency
        if "ano do exercício" in df.columns:
            year_values = df["ano do exercício"].unique()
            import numpy as np
            
            # Normalize year values - handle arrays/lists and convert to int
            normalized_years = []
            for y in year_values:
                # Extract first element if it's an array/list
                if isinstance(y, (list, np.ndarray)):
                    y = y[0] if len(y) > 0 else None
                # Convert to int (handles string years)
                if y is not None:
                    try:
                        normalized_years.append(int(y))
                    except (ValueError, TypeError):
                        normalized_years.append(y)
            
            if len(normalized_years) == 0:
                result["errors"].append("Year column is empty")
                result["metrics"]["year_consistency"] = False
            elif len(normalized_years) > 1 or normalized_years[0] != year:
                result["errors"].append(
                    f"Year mismatch: expected {year}, found {normalized_years}"
                )
                result["metrics"]["year_consistency"] = False
            else:
                result["metrics"]["year_consistency"] = True
        
        # Validate CEP format (should be numeric, 8 digits)
        if "CEP" in df.columns:
            cep_numeric = pd.to_numeric(df["CEP"], errors='coerce')
            invalid_cep = cep_numeric.isna().sum()
            if invalid_cep > 0:
                result["warnings"].append(
                    f"Found {invalid_cep} invalid CEP values"
                )
            result["metrics"]["invalid_cep_count"] = int(invalid_cep)
        
        # Validate city (should be Recife)
        # Note: Trim whitespace for comparison (whitespace will be trimmed in bronze layer)
        if "cidade" in df.columns:
            cities_raw = df["cidade"].unique()
            # Normalize by trimming and uppercasing for comparison
            cities_normalized = [str(c).strip().upper() if pd.notna(c) else str(c) for c in cities_raw]
            if len([c for c in cities_normalized if c != "NAN" and c != "NONE"]) > 1 or "RECIFE" not in cities_normalized:
                result["warnings"].append(
                    f"Unexpected city values (before trimming): {list(cities_raw)}"
                )
        
        # Validate estado (should be PE)
        # Note: Trim whitespace for comparison (whitespace will be trimmed in bronze layer)
        if "estado" in df.columns:
            estados_raw = df["estado"].unique()
            # Normalize by trimming for comparison
            estados_normalized = [str(e).strip() if pd.notna(e) else str(e) for e in estados_raw]
            if len([e for e in estados_normalized if e != "NAN" and e != "NONE"]) > 1 or "PE" not in estados_normalized:
                result["warnings"].append(
                    f"Unexpected estado values (before trimming): {list(estados_raw)}"
                )
        
        return result
    
    def generate_validation_report(self, output_path: Optional[Path] = None) -> pd.DataFrame:
        """
        Generate a validation report from all validation results.
        
        Args:
            output_path: Path to save report CSV. If None, only returns DataFrame.
        
        Returns:
            DataFrame with validation summary
        """
        if not self.validation_results:
            logger.warning("No validation results to report")
            return pd.DataFrame()
        
        report_data = []
        for result in self.validation_results:
            report_data.append({
                "year": result["year"],
                "passed": result["passed"],
                "errors_count": len(result["errors"]),
                "warnings_count": len(result["warnings"]),
                "row_count": result["metrics"].get("row_count", 0),
                "column_count": result["metrics"].get("column_count", 0),
                "duplicate_rows": result["metrics"].get("duplicate_rows", 0),
                "total_null_count": result["metrics"].get("total_null_count", 0),
                "source_path": result["source_path"],
            })
        
        report_df = pd.DataFrame(report_data)
        
        if output_path:
            report_df.to_csv(output_path, index=False)
            logger.info(f"Validation report saved to {output_path}")
        
        return report_df
    
    def get_errors_table(self) -> pd.DataFrame:
        """
        Get a detailed table of all errors found.
        
        Returns:
            DataFrame with error details
        """
        errors_data = []
        for result in self.validation_results:
            for error in result["errors"]:
                errors_data.append({
                    "year": result["year"],
                    "error_type": "ERROR",
                    "message": error,
                    "timestamp": result["timestamp"]
                })
            for warning in result["warnings"]:
                errors_data.append({
                    "year": result["year"],
                    "error_type": "WARNING",
                    "message": warning,
                    "timestamp": result["timestamp"]
                })
        
        return pd.DataFrame(errors_data)

