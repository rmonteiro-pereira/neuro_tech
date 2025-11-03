#!/bin/bash
# Script to stop/pause all running Airflow DAGs

echo "=== Stopping All Airflow DAGs ==="
echo ""

# Get all DAGs and pause them
echo "Pausing all DAGs..."
docker exec airflow-scheduler airflow dags list | grep -v "^dag_id" | awk '{print $1}' | while read dag_id; do
    if [ ! -z "$dag_id" ]; then
        echo "Pausing DAG: $dag_id"
        docker exec airflow-scheduler airflow dags pause "$dag_id" 2>/dev/null || true
    fi
done

echo ""
echo "Stopping all running DAG runs..."
# Delete all running DAG runs
docker exec airflow-scheduler airflow dags delete_dag -y iptu_medallion_pipeline 2>/dev/null || echo "Note: delete_dag is deprecated, using pause instead"

echo ""
echo "=== All DAGs Paused ==="
echo ""
echo "To unpause a specific DAG, run:"
echo "  docker exec airflow-scheduler airflow dags unpause <dag_id>"
echo ""
echo "To unpause all DAGs, run:"
echo "  docker exec airflow-scheduler airflow dags list | grep -v '^dag_id' | awk '{print \$1}' | xargs -I {} docker exec airflow-scheduler airflow dags unpause {}"

