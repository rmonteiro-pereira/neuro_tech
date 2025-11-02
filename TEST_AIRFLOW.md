# Testing Airflow DAG - Verification Guide

## Quick Verification Methods

### 1. **Check DAG Status in Airflow UI**
- Open http://localhost:8081
- Login: `admin` / `admin`
- Look for DAG: `iptu_medallion_pipeline`
- Status indicators:
  - **Green circle** = DAG is healthy and ready
  - **No import errors** = DAG parsed successfully
  - Check for any **red warnings** or errors

### 2. **Manual Trigger (Test Run)**
- In Airflow UI, find the DAG
- Click the **Play button** (▶️) to trigger manually
- Watch the DAG Run appear in "Recent Tasks"
- Monitor task execution:
  - `run_medallion_pipeline` runs first
  - Then `generate_visualizations`, `generate_dashboard`, `generate_validation_reports` run in parallel

### 3. **Check Logs via Airflow UI**
- Click on a task instance
- Click "Log" button to see execution logs
- Look for:
  - Pipeline start messages
  - Data processing progress
  - Success/completion messages

### 4. **Use Airflow CLI (in container)**
```bash
# Test DAG parsing
docker exec airflow-scheduler airflow dags list

# Check specific DAG
docker exec airflow-scheduler airflow dags list | grep iptu

# Test DAG import/syntax
docker exec airflow-scheduler airflow dags test iptu_medallion_pipeline 2025-11-02

# Trigger a DAG run
docker exec airflow-scheduler airflow dags trigger iptu_medallion_pipeline
```

### 5. **Verify Output Files**
After running, check if files are created:
- `data/bronze/` - Bronze layer files
- `data/silver/` - Silver layer files  
- `data/gold/` - Gold layer files
- `outputs/plots/` - Generated visualizations
- `outputs/analyses/` - Analysis results

### 6. **Check Container Logs**
```bash
# Check scheduler logs (where tasks execute)
docker-compose logs airflow-scheduler --tail=50

# Check webserver logs (for DAG parsing)
docker-compose logs airflow-webserver --tail=50
```

## Expected Behavior

✅ **Working DAG:**
- Appears in UI without errors
- Tasks can be triggered
- Tasks execute and show logs
- Output files are generated

❌ **Not Working:**
- Red error icon in UI
- Import errors visible
- Tasks fail immediately
- No output files created

