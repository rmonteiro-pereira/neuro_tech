#!/bin/bash
# Install Python requirements for Airflow containers

echo "Installing requirements from requirements.txt..."
pip install -r /opt/airflow/requirements.txt

# Skip PySpark if SKIP_PYSPARK environment variable is set
if [ "${SKIP_PYSPARK:-false}" != "true" ]; then
    echo "Installing PySpark requirements from requirements-pyspark.txt..."
    pip install -r /opt/airflow/requirements-pyspark.txt
else
    echo "Skipping PySpark installation (SKIP_PYSPARK=true)"
fi

echo "Requirements installation complete!"

