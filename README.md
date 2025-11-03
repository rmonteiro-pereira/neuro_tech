# IPTU Data Pipeline - Neuro Tech Challenge

> **A comprehensive, production-ready data pipeline for IPTU (Property Tax) data processing, featuring a medallion architecture with PySpark support, data quality validation, and automated analytics.**

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.4+-orange.svg)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.3+-0078D4.svg)](https://delta.io/)
[![PyDeequ](https://img.shields.io/badge/PyDeequ-1.5+-red.svg)](https://github.com/pydeequ/pydeequ)

---

## 📚 Table of Contents

- [Overview](#overview)
- [Key Features](#key-features)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Usage Guide](#usage-guide)
- [Configuration](#configuration)
- [Data Quality](#data-quality)
- [Analytics & Visualizations](#analytics--visualizations)
- [Outputs](#outputs)
- [Advanced Features](#advanced-features)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)

---

## 🎯 Overview

This project implements an enterprise-grade data pipeline for processing IPTU (Imposto Predial e Territorial Urbano) tax data from Recife, Brazil, covering years 2020-2024. Built on Python 3.11+, it features:

- **Medallion Architecture**: Raw → Bronze → Silver → Gold data layers
- **Dual Engine Support**: Pandas (default) and PySpark (distributed processing)
- **Data Quality Framework**: Automated validation with PyDeequ
- **Delta Lake Integration**: ACID transactions and schema evolution
- **Comprehensive Analytics**: Volume, distribution, and trend analysis
- **Interactive Dashboards**: Plotly-powered visualizations
- **Orchestration**: Apache Airflow DAGs for automated workflows

---

## ✨ Key Features

### 🏆 Core Capabilities

| Feature | Description | Status |
|---------|-------------|--------|
| **Multi-Format Support** | CSV (2020-2023) and JSON (2024) | ✅ |
| **Schema Evolution** | Automatic handling of schema changes between years | ✅ |
| **Incremental Processing** | Add new years without reprocessing entire dataset | ✅ |
| **Data Quality Checks** | 7-layer validation with PyDeequ integration | ✅ |
| **Medallion Architecture** | Raw → Bronze → Silver → Gold layers | ✅ |
| **Delta Lake** | ACID transactions, time travel, column mapping | ✅ |
| **Distributed Processing** | PySpark with Spark 3.4+ support | ✅ |
| **Automated Analytics** | Volume, distribution, and trend analysis | ✅ |
| **Interactive Dashboards** | Plotly and Matplotlib visualizations | ✅ |
| **Orchestration** | Apache Airflow DAG integration | ✅ |
| **Catalog System** | Centralized metadata tracking | ✅ |
| **Docker Support** | Containerized Spark environment | ✅ |

### 🔬 Data Quality Framework

- **PyDeequ Integration**: AWS Deequ automated data quality checks
- **Multi-Layer Validation**: Structure, completeness, uniqueness, business rules
- **Automated Reports**: JSON and CSV validation reports
- **Column Profiling**: Statistical analysis per column
- **Anomaly Detection**: Automated detection of data quality issues
- **Error Tracking**: Comprehensive error and warning logs

---

## 🏗️ Architecture

### Medallion Architecture (Delta Lake)

The pipeline follows a **medallion architecture** with four data layers:

```
┌─────────────────────────────────────────────────────────────────┐
│                     RAW LAYER (data/raw/)                      │
│  • Source CSV/JSON files                                        │
│  • No processing applied                                        │
│  • Immutable source of truth                                    │
│  • Cataloged with metadata (MD5, size, discovery date)         │
└─────────────────────────────────────────────────────────────────┘
                                  ↓
┌─────────────────────────────────────────────────────────────────┐
│                   BRONZE LAYER (data/bronze/)                  │
│  • Cleaned and cataloged data                                   │
│  • Normalized column names                                      │
│  • Standardized data types                                      │
│  • String trimming and categorical optimization                 │
│  • Partitioned by year                                          │
│  • Delta/Parquet format                                         │
│  • PyDeequ quality checks                                       │
└─────────────────────────────────────────────────────────────────┘
                                  ↓
┌─────────────────────────────────────────────────────────────────┐
│                   SILVER LAYER (data/silver/)                  │
│  • Consolidated multi-year dataset                              │
│  • Unified schema across all years                              │
│  • Time-series data ready for analytics                         │
│  • Deduplication applied                                        │
│  • Delta/Parquet format                                         │
└─────────────────────────────────────────────────────────────────┘
                                  ↓
┌─────────────────────────────────────────────────────────────────┐
│                    GOLD LAYER (data/gold/)                     │
│  • Business-ready analytics                                     │
│  • Aggregations by year/type/neighborhood                       │
│  • Analysis results (CSV)                                       │
│  • Visualizations (PNG, HTML)                                   │
│  • Dashboard reports                                            │
│  • Year-over-year trends                                        │
└─────────────────────────────────────────────────────────────────┘
```

### Processing Pipeline

```
┌──────────┐      ┌──────────────┐      ┌──────────────┐      ┌──────────────┐
│  SOURCE  │  →   │  INGESTION   │  →   │VALIDATION    │  →   │TRANSFORM     │
│  FILES   │      │   (Raw)      │      │  (Bronze)    │      │  (Silver)    │
└──────────┘      └──────────────┘      └──────────────┘      └──────────────┘
                                                                    ↓
┌──────────┐      ┌──────────────┐      ┌──────────────┐      ┌──────────────┐
│EXPORT FOR│  ←   │ DASHBOARD    │  ←   │  ANALYSIS    │  ←   │  CONSOLIDATE │
│SHARING   │      │  (Gold)      │      │   (Gold)     │      │   (Silver)   │
└──────────┘      └──────────────┘      └──────────────┘      └──────────────┘
```

---

## 📁 Project Structure

```
neuro_tech/
├── data/                                    # Data directory (gitignored)
│   ├── raw/                                 # Raw layer: source files
│   │   ├── iptu_2020/iptu_2020.csv
│   │   ├── iptu_2021/iptu_2021.csv
│   │   ├── iptu_2022/iptu_2022.csv
│   │   ├── iptu_2023/iptu_2023.csv
│   │   └── iptu_2024_json/iptu_2024_json.json
│   ├── bronze/                              # Bronze layer: cleaned data
│   │   ├── iptu_2020/                       # Delta/Parquet, partitioned
│   │   ├── iptu_2021/
│   │   ├── iptu_2022/
│   │   ├── iptu_2023/
│   │   └── iptu_2024/
│   ├── silver/                              # Silver layer: consolidated
│   │   └── iptu_silver_consolidated/        # Delta/Parquet
│   ├── gold/                                # Gold layer: analytics
│   │   ├── analyses/                        # Analysis results (CSV)
│   │   ├── plots/                           # Visualizations (PNG, HTML)
│   │   ├── gold_summary_by_neighborhood/    # Aggregations
│   │   ├── gold_summary_by_year_type/
│   │   └── gold_year_over_year_trends/
│   └── catalog/                             # Central catalog
│       ├── data_catalog.json                # Human-readable catalog
│       ├── data_catalog.parquet             # Queryable catalog
│       └── data_catalog_delta/              # Delta catalog table
│
├── src/iptu_pipeline/                       # Main source code
│   ├── __init__.py
│   ├── config.py                            # Configuration management
│   ├── engine.py                            # DataEngine abstraction
│   ├── dashboard.py                         # Dashboard generation
│   ├── visualizations.py                    # Plot generation
│   ├── orchestration.py                     # Prefect orchestration
│   ├── airflow_dag.py                       # Airflow DAG
│   │
│   ├── pipelines/                           # Pipeline modules
│   │   ├── __init__.py
│   │   ├── main_pipeline.py                 # Main orchestration
│   │   ├── ingestion.py                     # Data ingestion
│   │   ├── transformation.py                # Data transformation
│   │   └── analysis.py                      # Data analysis
│   │
│   └── utils/                               # Utilities
│       ├── __init__.py
│       ├── logger.py                        # Logging system
│       ├── data_quality.py                  # Basic validation
│       ├── medallion_quality.py             # PyDeequ validation
│       ├── raw_catalog.py                   # Catalog system
│       └── column_matcher.py                # Schema matching
│
├── scripts/                                 # Utility scripts
│   └── generate_plots.py                    # Standalone plot generator
│
├── notebooks/                               # Jupyter notebooks
│   ├── pipeline_execution.ipynb            # Pipeline walkthrough
│   └── visualizations.ipynb                # Visualization exploration
│
├── dags/                                    # Airflow DAGs
│   └── iptu_pipeline_dag.py                # Deployable DAG
│
├── outputs/                                 # Legacy outputs
│   ├── validation_report.csv
│   ├── validation_errors.csv
│   └── medallion_validation_report.json
│
├── logs/                                    # Application logs
│
├── docker-compose.yml                       # Spark cluster setup
├── docker-compose.standalone.yml            # Standalone Spark
│
├── main.py                                  # Entry point
├── export_refined_dataset.py               # Export script
├── test_pydeequ.py                         # PyDeequ tests
├── test_delta.py                           # Delta Lake tests
│
├── pyproject.toml                          # Project metadata
├── requirements-pyspark.txt                # PySpark dependencies
├── requirements.txt                        # Base dependencies
│
├── README.md                               # This file
├── ARCHITECTURE.md                         # Architecture details
├── DOCKER_SETUP.md                         # Docker guide
└── .gitignore
```

---

## 🚀 Installation

### Prerequisites

- **Python**: 3.11 or higher
- **Java** (for PySpark): 8 or 11
- **uv** (recommended) or pip/conda
- **Docker** (optional, for Spark)

### Step 1: Clone Repository

```bash
git clone <repository-url>
cd neuro_tech
```

### Step 2: Install Dependencies

**Option A: Using uv (Recommended)**

```bash
# Install uv if not already installed
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install base dependencies
uv sync

# Install PySpark dependencies (optional, for distributed processing)
uv sync --extra pyspark
```

**Option B: Using pip**

```bash
# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install base dependencies
pip install -e .

# Install PySpark dependencies (optional)
pip install -r requirements-pyspark.txt
```

**Option C: Using conda**

```bash
# Create conda environment
conda create -n neuro-tech python=3.11
conda activate neuro-tech

# Install dependencies
pip install -e .
pip install -r requirements-pyspark.txt
```

### Step 3: Verify Installation

```bash
python -c "from iptu_pipeline import config; print(f'Engine: {config.settings.DATA_ENGINE}')"
```

Expected output: `Engine: pandas`

---

## 🎮 Quick Start

### Run Full Pipeline (Pandas)

```bash
python main.py
```

This will:
1. Load data from `data/raw/`
2. Catalog files in `data/catalog/`
3. Process through Bronze → Silver → Gold layers
4. Generate analyses and visualizations
5. Save outputs to `data/gold/`

### Run Full Pipeline (PySpark)

```bash
# Windows PowerShell
$env:IPTU_DATA_ENGINE="pyspark"
python main.py

# Linux/Mac
export IPTU_DATA_ENGINE=pyspark
python main.py
```

### Run with Docker (Spark Standalone)

```bash
# Using standalone Spark (simplest)
docker-compose -f docker-compose.standalone.yml up --build

# Using Spark cluster
docker-compose up --build
```

Access Spark UI: http://localhost:4040

---

## 📖 Usage Guide

### Basic Usage

#### 1. Process All Years

```python
from iptu_pipeline.pipelines.main_pipeline import IPTUPipeline

pipeline = IPTUPipeline(engine='pandas')  # or 'pyspark'
consolidated_df = pipeline.run_full_pipeline(
    years=None,              # None = all years
    save_consolidated=True,
    run_analysis=True,
    incremental=False
)
```

#### 2. Process Specific Years

```python
pipeline = IPTUPipeline(engine='pyspark')
consolidated_df = pipeline.run_full_pipeline(
    years=[2020, 2021, 2022],
    save_consolidated=True,
    run_analysis=True,
    incremental=False
)
```

#### 3. Incremental Processing (Add New Years)

```python
# First run: process 2020-2023
pipeline.run_full_pipeline(years=[2020, 2021, 2022, 2023], incremental=False)

# Later: add 2024 without reprocessing previous years
pipeline.run_full_pipeline(years=[2024], incremental=True)
```

#### 4. Generate Visualizations

```python
from iptu_pipeline.visualizations import generate_plots_from_analysis_results

# Generate all plots
plot_files = generate_plots_from_analysis_results()
print(plot_files)
```

#### 5. Create Dashboard

```python
from iptu_pipeline.dashboard import IPTUDashboard
from iptu_pipeline.config import SILVER_DIR
from iptu_pipeline.engine import get_engine

# Load silver layer data
engine = get_engine()
if engine.engine_type == "pyspark":
    df = engine.read_parquet(SILVER_DIR / "iptu_silver_consolidated")
    df = engine.to_pandas(df)
else:
    import pandas as pd
    df = pd.read_parquet(SILVER_DIR / "iptu_silver_consolidated/data.parquet")

# Generate dashboard
dashboard = IPTUDashboard(df=df)
dashboard.generate_dashboard()
dashboard.generate_summary_report()
```

### Advanced Usage

#### Export Refined Dataset

```bash
python export_refined_dataset.py --output iptu_refined.parquet --engine pyspark
```

This creates a single Parquet file for sharing, with:
- Duplicates removed
- Metadata columns filtered
- Compatible timestamp formats
- All years consolidated

#### Run Airflow DAG

```bash
# Start Airflow (see AIRFLOW_SETUP.md)
airflow webserver --port 8080
airflow scheduler

# Trigger DAG
airflow dags trigger iptu_medallion_pipeline
```

#### Custom Configuration

```python
from iptu_pipeline.config import settings

# Modify settings
settings.DATA_ENGINE = "pyspark"
settings.MIN_ROWS_PER_YEAR = 500
settings.MAX_NULL_PERCENTAGE = 40.0

# Run pipeline with custom settings
pipeline = IPTUPipeline(engine='pyspark')
pipeline.run_full_pipeline()
```

---

## ⚙️ Configuration

### Environment Variables

The pipeline uses `pydantic-settings` for configuration. Set environment variables with prefix `IPTU_`:

```bash
# Windows PowerShell
$env:IPTU_DATA_ENGINE="pyspark"
$env:IPTU_MIN_ROWS_PER_YEAR="500"

# Linux/Mac
export IPTU_DATA_ENGINE=pyspark
export IPTU_MIN_ROWS_PER_YEAR=500
```

### Configuration File

Edit `src/iptu_pipeline/config.py`:

```python
class Settings(BaseSettings):
    # Base directories
    BASE_DIR: Path = ...
    DATA_DIR: Path = ...
    OUTPUT_DIR: Path = ...
    
    # Medallion layers
    RAW_DIR: Path = ...
    BRONZE_DIR: Path = ...
    SILVER_DIR: Path = ...
    GOLD_DIR: Path = ...
    CATALOG_DIR: Path = ...
    
    # Data years
    CSV_YEARS: List[int] = [2020, 2021, 2022, 2023]
    JSON_YEARS: List[int] = [2024]
    
    # Quality thresholds
    MIN_ROWS_PER_YEAR: int = 100
    MAX_NULL_PERCENTAGE: float = 50.0
    
    # Processing engine
    DATA_ENGINE: str = "pandas"  # or "pyspark"
```

### Spark Configuration

For PySpark, customize in `src/iptu_pipeline/engine.py`:

```python
builder = SparkSession.builder \
    .appName("IPTU_Pipeline") \
    .config("spark.driver.memory", "16g") \
    .config("spark.executor.memory", "16g") \
    .config("spark.sql.shuffle.partitions", "200")
```

---

## 🔍 Data Quality

### Validation Framework

The pipeline implements **multi-layer data quality validation**:

#### Layer 1: Basic Validation (Bronze)
- Structure checks (row count, column count)
- Data type validation
- Null value percentage
- Duplicate detection
- Business rules (CEP, city, state)

#### Layer 2: PyDeequ Validation
- **Completeness**: Check for null values
- **Unique values**: Detect duplicates
- **Uniqueness**: Primary key constraints
- **Range checks**: Value constraints
- **Column profiling**: Statistical analysis
- **Anomaly detection**: Automated issue detection

#### Layer 3: Medallion Validation (Silver/Gold)
- Year consistency across layers
- Schema evolution compatibility
- Deduplication verification
- Aggregation accuracy

### Quality Reports

Reports are generated at multiple stages:

1. **data/catalog/data_catalog.json**: Raw file catalog
2. **outputs/validation_report.csv**: Per-year validation summary
3. **outputs/validation_errors.csv**: Detailed errors/warnings
4. **outputs/medallion_validation_report.json**: Layer-wise validation

### Running Quality Checks

```python
from iptu_pipeline.utils.medallion_quality import MedallionDataQuality
from iptu_pipeline.engine import get_engine

engine = get_engine('pyspark')
quality = MedallionDataQuality(engine.spark)

# Validate bronze layer
df_bronze = engine.read_parquet(BRONZE_DIR / "iptu_2020")
results = quality.validate_bronze_layer(df_bronze, year=2020)

# Validate silver layer
df_silver = engine.read_parquet(SILVER_DIR / "iptu_silver_consolidated")
results = quality.validate_silver_layer(df_silver, year=None)
```

---

## 📊 Analytics & Visualizations

### Volume Analysis

Analyzes total number of properties:
- Total by year (2020-2024)
- Distribution by type of use
- Distribution by neighborhood
- Volume by year × type (cross-tabulation)
- Volume by year × neighborhood

### Distribution Analysis

Analyzes physical distribution:
- By construction type
- Temporal distribution
- Top neighborhoods by volume
- Percentage distributions

### Tax Value Analysis

Analyzes IPTU values (additional):
- Mean, median, min, max by year
- Total collected by year
- Top neighborhoods by average value
- Value trends over time

### Visualizations

Automatically generated plots:
- `volume_by_year.png`: Bar chart of properties per year
- `volume_by_type.png`: Pie + bar chart by type
- `top_neighborhoods.png`: Top 20 neighborhoods
- `volume_by_year_type.png`: Stacked area chart
- `tax_trends.png`: Line + bar chart of IPTU trends
- `top_tax_neighborhoods.png`: Top neighborhoods by IPTU
- `distribution_by_construction.png`: Bar chart by construction type
- `temporal_distribution.png`: Timeline of properties
- `visualizations_report.html`: All plots in one HTML

---

## 📤 Outputs

### Medallion Layers

| Layer | Location | Format | Description |
|-------|----------|--------|-------------|
| **Raw** | `data/raw/` | CSV/JSON | Source files |
| **Bronze** | `data/bronze/` | Delta/Parquet | Cleaned data |
| **Silver** | `data/silver/` | Delta/Parquet | Consolidated |
| **Gold** | `data/gold/` | CSV/PNG/HTML | Analytics |

### Analysis Results

`data/gold/analyses/`:
- `volume_analysis/`: Total volume by year/type/neighborhood
- `distribution_analysis/`: Physical distribution analyses
- `tax_value_analysis/`: IPTU value trends

### Visualizations

`data/gold/plots/`:
- 8 PNG files (300 DPI, publication-quality)
- 1 HTML report (all plots embedded)

### Legacy Outputs

`outputs/`:
- `validation_report.csv`: Validation summary
- `validation_errors.csv`: Detailed errors
- `medallion_validation_report.json`: Layer validation

### Catalog

`data/catalog/`:
- `data_catalog.json`: Human-readable metadata
- `data_catalog.parquet`: Queryable catalog
- `data_catalog_delta/`: Delta table catalog

---

## 🔬 Advanced Features

### Delta Lake Features

1. **ACID Transactions**: Ensure data integrity
2. **Schema Evolution**: Add/rename columns without rewriting
3. **Column Mapping**: Handle special characters in column names
4. **Time Travel**: Query historical versions
5. **Partitioning**: Partition by year for performance

### PyDeequ Integration

Automated data quality checks:
- Completeness checks
- Anomaly detection
- Column profiling
- Constraint checks

### Catalog System

Centralized metadata tracking:
- File discovery timestamp
- MD5 checksum
- Processing status
- Schema snapshots
- Row/column counts

### Incremental Processing

Add new years without reprocessing:
1. Checks existing years in Silver layer
2. Only processes new years
3. Appends to existing data
4. Updates analyses

### Export Script

`export_refined_dataset.py`:
- Removes duplicates
- Filters metadata columns
- Normalizes timestamps
- Creates single Parquet file

---

## 🐛 Troubleshooting

### Common Issues

#### 1. GLIBC Mismatch (Docker)

```
Error: /lib/x86_64-linux-gnu/libc.so.6: version `GLIBC_2.34' not found
```

**Solution**: Ensure `.venv` is built inside the container:

```dockerfile
# In Dockerfile
RUN python -m venv /app/.venv
```

#### 2. DateType Conversion Error

```
Error: Unable to map type DateType
```

**Solution**: Already fixed in `engine.py`. PySpark DateType is converted to TimestampType before `toPandas()`.

#### 3. Spark Connection Issues (Windows)

```
Error: Connecting to DESKTOP-XXX/192.168.X.X:XXXXX failed
```

**Solution**: Configured in `engine.py`:
```python
.config("spark.driver.host", "localhost")
.config("spark.driver.bindAddress", "127.0.0.1")
```

#### 4. Out of Memory (PySpark)

```
Error: OutOfMemoryError: Java heap space
```

**Solution**: Increase memory in `engine.py`:
```python
.config("spark.driver.memory", "16g")
.config("spark.executor.memory", "16g")
```

#### 5. Delta Column Mapping

```
Error: Found invalid character(s) among ' ,;{}()...' in column names
```

**Solution**: Enabled in `main_pipeline.py`:
```python
.option("delta.columnMapping.mode", "name")
```

### Debugging

Enable debug logging:

```python
import logging
logging.getLogger("iptu_pipeline").setLevel(logging.DEBUG)
```

View Spark logs:

```python
spark.sparkContext.setLogLevel("DEBUG")
```

---

## 🤝 Contributing

This project was developed for the Neuro Tech technical challenge. For contributions:

1. Fork the repository
2. Create a feature branch
3. Make changes with tests
4. Submit a pull request

---

## 📄 License

This project was developed for the Neuro Tech technical challenge.

---

## 📞 Support

For issues or questions:
- Check `ARCHITECTURE.md` for design decisions
- Check `DOCKER_SETUP.md` for Docker issues
- Review logs in `logs/` directory

---

## 🎯 Next Steps

Future enhancements:
- [ ] Unit test coverage
- [ ] CI/CD pipeline
- [ ] Web dashboard deployment
- [ ] Database integration (PostgreSQL/MongoDB)
- [ ] Real-time streaming support (Kafka)
- [ ] ML model integration
- [ ] Cost optimization for Spark

---

**Built with ❤️ for the Neuro Tech Challenge**
