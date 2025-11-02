"""
Raw data catalog module for tracking source files before processing to Bronze layer.
Tracks extended metadata including checksums, schema snapshots, and processing status.
"""
import hashlib
import json
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime

try:
    import pandas as pd
except ImportError:
    pd = None

from iptu_pipeline.config import settings, CATALOG_DIR, RAW_DIR, CSV_YEARS, JSON_YEARS, DATA_PATHS
from iptu_pipeline.utils.logger import setup_logger

logger = setup_logger("raw_catalog")


class RawDataCatalog:
    """
    Catalog system for raw source data files.
    
    Tracks files before they're processed to Bronze layer, storing extended metadata
    in both JSON (human-readable) and Delta/Parquet (queryable) formats.
    """
    
    def __init__(self, engine: Optional[str] = None):
        """
        Initialize raw data catalog.
        
        Args:
            engine: Optional engine type ('pandas' or 'pyspark'). Uses config default if None.
        """
        # Lazy import to avoid circular dependency
        from iptu_pipeline.engine import get_engine
        self.engine = get_engine(engine)
        self.catalog_dir = CATALOG_DIR
        self.raw_dir = RAW_DIR  # Use RAW_DIR instead of DATA_DIR
        
        # Catalog file paths (stored in central catalog directory)
        self.json_catalog_path = self.catalog_dir / "data_catalog.json"
        self.delta_catalog_path = self.catalog_dir / "data_catalog_delta"
        self.parquet_catalog_path = self.catalog_dir / "data_catalog.parquet"
        
        # Ensure catalog directory exists
        self.catalog_dir.mkdir(parents=True, exist_ok=True)
        
        # In-memory catalog (updated during operations)
        self._catalog: Dict[int, Dict[str, Any]] = {}
        
        # Load existing catalog if it exists
        self._load_existing_catalog()
    
    def _load_existing_catalog(self):
        """Load existing catalog from disk."""
        try:
            if self.json_catalog_path.exists():
                with open(self.json_catalog_path, 'r', encoding='utf-8') as f:
                    catalog_list = json.load(f)
                    # Convert list to dict keyed by year
                    self._catalog = {item['year']: item for item in catalog_list if 'year' in item}
                logger.info(f"Loaded existing catalog with {len(self._catalog)} entries")
            elif self.engine.engine_type == "pyspark" and self._path_exists(self.delta_catalog_path):
                # Load from Delta table
                try:
                    catalog_df = self.engine.spark.read.format("delta").load(str(self.delta_catalog_path))
                    catalog_rows = catalog_df.collect()
                    self._catalog = {row['year']: row.asDict() for row in catalog_rows}
                    logger.info(f"Loaded existing Delta catalog with {len(self._catalog)} entries")
                except Exception as e:
                    logger.warning(f"Could not load Delta catalog: {str(e)}")
            elif self._path_exists(self.parquet_catalog_path):
                # Load from Parquet
                catalog_df = self.engine.read_parquet(self.parquet_catalog_path)
                if pd and isinstance(catalog_df, pd.DataFrame):
                    catalog_dict = catalog_df.set_index('year').to_dict('index')
                    self._catalog = {int(year): {k: v for k, v in data.items()} for year, data in catalog_dict.items()}
                    logger.info(f"Loaded existing Parquet catalog with {len(self._catalog)} entries")
        except Exception as e:
            logger.warning(f"Could not load existing catalog: {str(e)}. Starting fresh.")
            self._catalog = {}
    
    def _path_exists(self, path: Path) -> bool:
        """Check if a data path exists."""
        if path.suffix == ".parquet":
            return path.exists()
        return path.exists() and (path.is_dir() or any(path.glob("*.parquet")))
    
    def _calculate_md5(self, file_path: Path) -> str:
        """Calculate MD5 checksum of a file."""
        hash_md5 = hashlib.md5()
        try:
            with open(file_path, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b''):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        except Exception as e:
            logger.warning(f"Could not calculate MD5 for {file_path}: {str(e)}")
            return ""
    
    def _extract_year_from_path(self, file_path: Path) -> Optional[int]:
        """Extract year from file path or name."""
        # Try to extract from filename
        filename = file_path.name.lower()
        for year in range(2020, 2030):
            if str(year) in filename:
                return year
        
        # Try to extract from path
        path_str = str(file_path).lower()
        for year in range(2020, 2030):
            if str(year) in path_str:
                return year
        
        return None
    
    def _get_schema_snapshot(self, file_path: Path, year: int) -> str:
        """
        Get schema snapshot (column names and types) from first few rows.
        
        Args:
            file_path: Path to the file
            year: Year of the data
        
        Returns:
            JSON string with schema information
        """
        try:
            # Read just a sample (first 10 rows) to get schema
            if year in CSV_YEARS:
                if pd:
                    sample_df = pd.read_csv(file_path, sep=';', encoding='utf-8', nrows=10, low_memory=False)
                else:
                    return json.dumps({})
            elif year in JSON_YEARS:
                if pd:
                    sample_df = pd.read_json(file_path)
                    if len(sample_df) > 10:
                        sample_df = sample_df.head(10)
                else:
                    return json.dumps({})
            else:
                return json.dumps({})
            
            if pd and isinstance(sample_df, pd.DataFrame):
                schema = {
                    "columns": sample_df.columns.tolist(),
                    "dtypes": {col: str(dtype) for col, dtype in sample_df.dtypes.items()},
                    "sample_row_count": len(sample_df)
                }
                return json.dumps(schema)
        except Exception as e:
            logger.warning(f"Could not get schema snapshot for {file_path}: {str(e)}")
            return json.dumps({})
    
    def _get_row_count(self, file_path: Path, year: int) -> Optional[int]:
        """
        Try to get approximate row count without reading entire file.
        
        Args:
            file_path: Path to the file
            year: Year of the data
        
        Returns:
            Row count or None if unable to determine
        """
        try:
            # For CSV files, count lines (subtract header)
            if year in CSV_YEARS:
                with open(file_path, 'r', encoding='utf-8') as f:
                    return sum(1 for _ in f) - 1  # Subtract header
            # For JSON, would need to parse - skip for now to avoid loading large files
            elif year in JSON_YEARS:
                # For large JSON files, skip counting to avoid memory issues
                return None
        except Exception as e:
            logger.warning(f"Could not get row count for {file_path}: {str(e)}")
            return None
        return None
    
    def scan_raw_files(self) -> Dict[int, Dict[str, Any]]:
        """
        Scan RAW_DIR and discover all raw data files.
        
        Returns:
            Dictionary mapping year to file metadata
        """
        logger.info("Scanning raw data files...")
        
        discovered_files = {}
        
        # Scan configured data paths
        for year, file_path in DATA_PATHS.items():
            if not file_path.exists():
                logger.warning(f"Configured data file not found: {file_path}")
                continue
            
            logger.info(f"Discovered file for year {year}: {file_path}")
            
            # Get file metadata
            file_stat = file_path.stat()
            file_size = file_stat.st_size
            file_name = file_path.name
            
            # Calculate checksum
            md5_checksum = self._calculate_md5(file_path)
            
            # Extract year if not already known
            extracted_year = self._extract_year_from_path(file_path) or year
            
            # Get schema snapshot
            schema_snapshot = self._get_schema_snapshot(file_path, year)
            
            # Get row count
            row_count = self._get_row_count(file_path, year)
            
            # Determine file type
            if file_path.suffix.lower() == '.csv':
                file_type = 'csv'
            elif file_path.suffix.lower() == '.json':
                file_type = 'json'
            else:
                file_type = file_path.suffix.lower().lstrip('.')
            
            # Get relative path (relative to raw_dir)
            try:
                relative_path = str(file_path.relative_to(self.raw_dir))
            except ValueError:
                relative_path = str(file_path)
            
            # Check if this file already exists in catalog
            existing_entry = self._catalog.get(year, {})
            existing_md5 = existing_entry.get('md5_checksum', '')
            
            # If file changed (different MD5), update catalog
            if existing_md5 and existing_md5 != md5_checksum:
                logger.info(f"File {file_path} has changed (MD5 mismatch), updating catalog")
            
            # Create/update catalog entry
            discovery_timestamp = existing_entry.get('discovery_timestamp')
            if not discovery_timestamp:
                discovery_timestamp = datetime.now().isoformat()
            
            metadata = {
                "file_path": str(file_path.absolute()),
                "relative_path": relative_path,
                "file_name": file_name,
                "year": year,
                "file_type": file_type,
                "file_size_bytes": file_size,
                "md5_checksum": md5_checksum,
                "discovery_timestamp": discovery_timestamp,
                "processing_status": existing_entry.get('processing_status', 'pending'),
                "bronze_path": existing_entry.get('bronze_path', ''),
                "row_count": row_count,
                "column_count": len(json.loads(schema_snapshot).get('columns', [])) if schema_snapshot else None,
                "schema_snapshot": schema_snapshot,
                "validation_status": existing_entry.get('validation_status', 'pending'),
                "last_updated": datetime.now().isoformat()
            }
            
            discovered_files[year] = metadata
            self._catalog[year] = metadata
        
        logger.info(f"Scan complete: {len(discovered_files)} files cataloged")
        return discovered_files
    
    def get_pending_files(self) -> Dict[int, Dict[str, Any]]:
        """
        Get files that are pending processing (status is 'pending').
        
        Returns:
            Dictionary mapping year to file metadata
        """
        return {
            year: metadata
            for year, metadata in self._catalog.items()
            if metadata.get('processing_status') == 'pending'
        }
    
    def get_file_metadata(self, year: int) -> Optional[Dict[str, Any]]:
        """
        Get metadata for a specific file by year.
        
        Args:
            year: Year to look up
        
        Returns:
            File metadata dictionary or None if not found
        """
        return self._catalog.get(year)
    
    def mark_as_processing(self, year: int, bronze_path: Optional[str] = None):
        """
        Mark a file as being processed.
        
        Args:
            year: Year of the file
            bronze_path: Optional path where file will be saved in bronze layer
        """
        if year in self._catalog:
            self._catalog[year]['processing_status'] = 'processing'
            self._catalog[year]['last_updated'] = datetime.now().isoformat()
            if bronze_path:
                self._catalog[year]['bronze_path'] = bronze_path
            logger.info(f"Marked year {year} as processing")
        else:
            logger.warning(f"Year {year} not found in catalog")
    
    def mark_as_completed(self, year: int, bronze_path: Optional[str] = None):
        """
        Mark a file as completed (successfully processed to bronze).
        
        Args:
            year: Year of the file
            bronze_path: Path where file was saved in bronze layer
        """
        if year in self._catalog:
            self._catalog[year]['processing_status'] = 'completed'
            self._catalog[year]['last_updated'] = datetime.now().isoformat()
            if bronze_path:
                self._catalog[year]['bronze_path'] = bronze_path
            logger.info(f"Marked year {year} as completed")
        else:
            logger.warning(f"Year {year} not found in catalog")
    
    def mark_as_failed(self, year: int, error_message: Optional[str] = None):
        """
        Mark a file as failed processing.
        
        Args:
            year: Year of the file
            error_message: Optional error message
        """
        if year in self._catalog:
            self._catalog[year]['processing_status'] = 'failed'
            self._catalog[year]['last_updated'] = datetime.now().isoformat()
            if error_message:
                self._catalog[year]['error_message'] = error_message
            logger.warning(f"Marked year {year} as failed: {error_message}")
        else:
            logger.warning(f"Year {year} not found in catalog")
    
    def save_catalog(self):
        """Save catalog to both JSON and Delta/Parquet formats."""
        logger.info("Saving catalog to disk...")
        
        # Save JSON manifest (human-readable)
        catalog_list = list(self._catalog.values())
        # Simplify JSON for readability (don't include full schema_snapshot)
        json_data = []
        for entry in catalog_list:
            json_entry = {k: v for k, v in entry.items() if k != 'schema_snapshot'}
            json_data.append(json_entry)
        
        with open(self.json_catalog_path, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, indent=2, default=str)
        logger.info(f"Saved JSON catalog to {self.json_catalog_path}")
        
        # Save Delta/Parquet table (queryable) - non-blocking, failures won't crash pipeline
        if not catalog_list:
            logger.warning("No catalog entries to save")
            return
        
        if self.engine.engine_type == "pyspark":
            # Save as Delta table
            try:
                # Check if Spark session is still active
                if not hasattr(self.engine, 'spark') or self.engine.spark is None:
                    logger.warning("Spark session not available, skipping Parquet/Delta catalog save")
                    return
                
                # Apply spark.sql.debug.maxToStringFields config to prevent truncation warnings
                self.engine.spark.conf.set("spark.sql.debug.maxToStringFields", "200")
                
                from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, TimestampType
                from pyspark.sql.functions import to_timestamp, lit
                
                # Convert catalog list to DataFrame, handling None values
                # Create a clean list with proper types
                clean_catalog = []
                for entry in catalog_list:
                    clean_entry = {
                        'file_path': entry.get('file_path', ''),
                        'relative_path': entry.get('relative_path', ''),
                        'file_name': entry.get('file_name', ''),
                        'year': int(entry.get('year', 0)),
                        'file_type': entry.get('file_type', ''),
                        'file_size_bytes': int(entry.get('file_size_bytes', 0)),
                        'md5_checksum': entry.get('md5_checksum', ''),
                        'discovery_timestamp': entry.get('discovery_timestamp', ''),
                        'processing_status': entry.get('processing_status', 'pending'),
                        'bronze_path': entry.get('bronze_path', ''),
                        'row_count': int(entry.get('row_count', 0)) if entry.get('row_count') is not None else None,
                        'column_count': int(entry.get('column_count', 0)) if entry.get('column_count') is not None else None,
                        'schema_snapshot': entry.get('schema_snapshot', ''),
                        'validation_status': entry.get('validation_status', 'pending'),
                        'last_updated': entry.get('last_updated', '')
                    }
                    clean_catalog.append(clean_entry)
                
                catalog_df = self.engine.spark.createDataFrame(clean_catalog)
                
                # Save as Delta if available, otherwise Parquet
                try:
                    catalog_df.write.format("delta").mode("overwrite").save(str(self.delta_catalog_path))
                    logger.info(f"Saved Delta catalog to {self.delta_catalog_path}")
                except Exception as e:
                    logger.warning(f"Delta write failed, using Parquet: {str(e)}")
                    try:
                        catalog_df.write.format("parquet").mode("overwrite").save(str(self.delta_catalog_path))
                        logger.info(f"Saved Parquet catalog to {self.delta_catalog_path}")
                    except Exception as e2:
                        logger.warning(f"Parquet write also failed: {str(e2)}. Catalog JSON is still saved.")
            except Exception as e:
                # Non-blocking - log warning but don't crash
                logger.warning(f"Failed to save catalog with PySpark (JSON catalog is still saved): {str(e)}")
        else:
            # Save as Parquet (Pandas)
            if pd:
                try:
                    catalog_df = pd.DataFrame(catalog_list)
                    # Handle None values for numeric columns
                    catalog_df['row_count'] = pd.to_numeric(catalog_df['row_count'], errors='coerce')
                    catalog_df['column_count'] = pd.to_numeric(catalog_df['column_count'], errors='coerce')
                    self.engine.write_parquet(catalog_df, self.parquet_catalog_path)
                    logger.info(f"Saved Parquet catalog to {self.parquet_catalog_path}")
                except Exception as e:
                    logger.warning(f"Failed to save Parquet catalog (JSON catalog is still saved): {str(e)}")
            else:
                logger.warning("Pandas not available, skipping Parquet catalog save")
    
    def get_catalog_summary(self) -> Dict[str, Any]:
        """
        Get summary statistics of the catalog.
        
        Returns:
            Dictionary with summary statistics
        """
        if not self._catalog:
            return {
                "total_files": 0,
                "by_status": {},
                "by_file_type": {},
                "total_size_bytes": 0
            }
        
        by_status = {}
        by_file_type = {}
        total_size = 0
        
        for metadata in self._catalog.values():
            status = metadata.get('processing_status', 'unknown')
            by_status[status] = by_status.get(status, 0) + 1
            
            file_type = metadata.get('file_type', 'unknown')
            by_file_type[file_type] = by_file_type.get(file_type, 0) + 1
            
            total_size += metadata.get('file_size_bytes', 0)
        
        return {
            "total_files": len(self._catalog),
            "by_status": by_status,
            "by_file_type": by_file_type,
            "total_size_bytes": total_size,
            "total_size_gb": round(total_size / (1024**3), 2)
        }

