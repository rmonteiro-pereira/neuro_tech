# Docker Compose Setup for IPTU Pipeline

## Overview

This docker-compose setup runs:
- **Apache Spark** (Master + Worker) - for PySpark processing
- **Apache Airflow** (Webserver + Scheduler) - for orchestration
- **PostgreSQL** - for Airflow metadata

## Why Bitnami Images?

**Bitnami Airflow** is used because it's simpler:
- Cleaner environment variables (`AIRFLOW_DATABASE_HOST` vs `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`)
- Pre-configured defaults
- Less setup required
- Consistent with Bitnami Spark image already in use

## Quick Start

1. **Create required directories:**
   ```bash
   mkdir -p dags plugins logs
   ```

2. **Copy your DAG file:**
   ```bash
   cp src/iptu_pipeline/airflow_dag.py dags/
   ```

3. **Start services:**
   ```bash
   docker-compose up -d
   ```

4. **Access Airflow UI:**
   - URL: http://localhost:8081
   - Username: `admin`
   - Password: `admin`

5. **Access Spark UI:**
   - URL: http://localhost:8080

## Services

- **postgres**: PostgreSQL database for Airflow metadata
- **spark-master**: Spark master node (port 8080, 7077)
- **spark-worker**: Spark worker node
- **airflow-webserver**: Airflow web UI (port 8081)
- **airflow-scheduler**: Airflow task scheduler

## Volumes

All data directories are mounted:
- `./dags` → Airflow DAGs
- `./data` → Input data (raw/bronze/silver/gold)
- `./outputs` → Pipeline outputs
- `./logs` → Log files
- `./src` → Source code (for imports)

## Notes

- Using **LocalExecutor** (simplest, no Redis needed)
- Default credentials are for development only
- For production, change passwords and use CeleryExecutor

