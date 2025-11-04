"""
Example of how to use Settings with environment variables.

You can override any setting using environment variables with the prefix 'IPTU_'.

Examples:
    # Set custom data directory
    export IPTU_DATA_DIR=/path/to/custom/data
    
    # Set custom output directory
    
    # Override quality thresholds
    export IPTU_MIN_ROWS_PER_YEAR=500
    export IPTU_MAX_NULL_PERCENTAGE=30.0
    
    # Set years to process
    export IPTU_CSV_YEARS="[2020,2021,2022]"
    export IPTU_JSON_YEARS="[2024]"
    
    # Set data processing engine
    export IPTU_DATA_ENGINE=pyspark  # or 'pandas'

Or create a .env file:
    IPTU_DATA_DIR=/custom/data/path
    IPTU_MIN_ROWS_PER_YEAR=500
    IPTU_DATA_ENGINE=pyspark
"""

from iptu_pipeline.config import settings

# Example: Access settings
print(f"Data directory: {settings.DATA_DIR}")
print(f"Min rows per year: {settings.MIN_ROWS_PER_YEAR}")
print(f"Data engine: {settings.DATA_ENGINE}")

# Example: Create custom settings instance
from iptu_pipeline.config import Settings

custom_settings = Settings(
    DATA_DIR="/custom/data",
    MIN_ROWS_PER_YEAR=500,
    DATA_ENGINE="pyspark"  # Use PySpark
)

