# Command Reference - Quick Start Guide

## 🎯 Quick Commands

### 1️⃣ Pandas Pipeline (Default - Fastest)

```bash
# Using uv (recommended)
uv run python main.py

# Or using system Python
python main.py
```

**Use this for:** Development, testing, small-medium datasets

---

### 2️⃣ PySpark Pipeline (Local)

```bash
# Install PySpark first
uv sync --extra pyspark

# Run with PySpark
$env:IPTU_DATA_ENGINE="pyspark"  # PowerShell
python main.py

# Or Linux/Mac
export IPTU_DATA_ENGINE=pyspark
python main.py
```

**Use this for:** Demonstrating big data capabilities, larger datasets

---

### 3️⃣ Docker Standalone (PySpark in Container)

```bash
# Build and run
docker compose -f docker-compose.standalone.yml up --build

# Or detached
docker compose -f docker-compose.standalone.yml up -d

# View logs
docker compose -f docker-compose.standalone.yml logs -f

# Stop
docker compose -f docker-compose.standalone.yml down
```

**Access Spark UI:** http://localhost:4040

---

### 4️⃣ Docker Cluster Mode (Full Spark Cluster)

```bash
# Build and run cluster
docker compose up --build

# Or detached
docker compose up -d

# View logs
docker compose logs -f

# Stop
docker compose down
```

**Access:**
- Spark Master UI: http://localhost:8080
- Worker UI: http://localhost:8081

---

## 📝 Detailed Examples

### Using uv (Recommended)

```bash
# Sync dependencies
uv sync

# Run with Pandas
uv run python main.py

# Run with PySpark
uv sync --extra pyspark
$env:IPTU_DATA_ENGINE="pyspark"
uv run python main.py
```

### Using pip

```bash
# Install dependencies
pip install -e .

# Run with Pandas
python main.py

# Install with PySpark
pip install -e ".[pyspark]"

# Run with PySpark
$env:IPTU_DATA_ENGINE="pyspark"
python main.py
```

### Python Script Examples

```python
from iptu_pipeline.orchestration import run_orchestrated_pipeline

# Full pipeline with Pandas
consolidated_df = run_orchestrated_pipeline()

# Full pipeline with PySpark
consolidated_df = run_orchestrated_pipeline(engine="pyspark")

# Incremental (just new years)
consolidated_df = run_orchestrated_pipeline(
    years=[2024],
    incremental=True
)
```

### Generate Visualizations

```bash
# After running the pipeline
uv run python scripts/generate_plots.py

# Or in Jupyter
jupyter notebook notebooks/visualizations.ipynb
```

### Run Analysis Only

```python
import pandas as pd
from iptu_pipeline.pipelines.analysis import IPTUAnalyzer
from iptu_pipeline.config import CONSOLIDATED_DATA_PATH

df = pd.read_parquet(CONSOLIDATED_DATA_PATH)
analyzer = IPTUAnalyzer(df)
results = analyzer.generate_all_analyses()
```

---

## 🔍 Environment Variables

| Variable | Values | Default | Description |
|----------|--------|---------|-------------|
| `IPTU_DATA_ENGINE` | `pandas`, `pyspark` | `pandas` | Processing engine |
| `IPTU_DATA_DIR` | path | `./data` | Input data directory |
| `IPTU_OUTPUT_DIR` | path | `./outputs` | Output directory |
| `SPARK_MASTER` | url | `local[*]` | Spark master URL |

**Example:**
```bash
# PowerShell
$env:IPTU_DATA_ENGINE="pyspark"
$env:SPARK_MASTER="spark://localhost:7077"

# Bash
export IPTU_DATA_ENGINE="pyspark"
export SPARK_MASTER="spark://localhost:7077"
```

---

## 📊 Outputs

All commands generate outputs in:

```
outputs/
├── analyses/              # CSV analysis results
├── plots/                 # PNG plots + HTML report
├── iptu_consolidated.parquet
├── iptu_processed.parquet
├── validation_report.csv
└── validation_errors.csv

logs/
└── orchestration_*.log
```

---

## ⚡ Recommended Workflow

### For Development/Testing
```bash
# Fastest option
uv run python main.py
```

### For Demonstration
```bash
# Show Docker + Spark integration
docker compose -f docker-compose.standalone.yml up --build

# View Spark UI
start http://localhost:4040  # Windows
```

### For Production Deployment
```bash
# Use Airflow DAG
# See AIRFLOW_SETUP.md
```

---

## 🐛 Troubleshooting

### PySpark Not Found
```bash
uv sync --extra pyspark
```

### Docker Build Fails
```bash
docker compose -f docker-compose.standalone.yml build --no-cache
```

### Permission Issues (Docker)
```bash
docker compose -f docker-compose.standalone.yml down -v
docker compose -f docker-compose.standalone.yml up --build
```

### Lock File Issues
```bash
rm uv.lock
uv lock
```

---

## 📚 More Information

- **Engine Details:** See `README_ENGINE.md`
- **Docker Setup:** See `DOCKER_SETUP.md` (if exists) or `docker-compose.standalone.yml`
- **Architecture:** See `ARCHITECTURE.md`
- **Airflow:** See `AIRFLOW_SETUP.md`

