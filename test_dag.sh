#!/bin/bash
# Simple script to test Airflow DAG

echo "=== Testing Airflow DAG ==="
echo ""

echo "1. Listing all DAGs:"
docker exec airflow-scheduler airflow dags list | grep iptu

echo ""
echo "2. Testing DAG import (syntax check):"
docker exec airflow-scheduler airflow dags test iptu_medallion_pipeline 2025-11-02 || echo "Note: Test may take time or fail if data is not available"

echo ""
echo "3. Triggering DAG manually:"
docker exec airflow-scheduler airflow dags trigger iptu_medallion_pipeline

echo ""
echo "4. Check recent DAG runs:"
docker exec airflow-scheduler airflow dags list-runs -d iptu_medallion_pipeline --state running --no-backfill

echo ""
echo "=== Done ==="
echo "Check Airflow UI at http://localhost:8081 for DAG execution status"

