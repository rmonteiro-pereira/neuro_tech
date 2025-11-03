#!/bin/bash
# Install Python requirements for Airflow containers
# This script should be run as the airflow user (not root)

set -e  # Exit on error

# Ensure Python user site-packages are enabled (they should be by default, but explicit is better)
# PYTHONNOUSERSITE=1 would disable them, so we ensure it's unset or 0
unset PYTHONNOUSERSITE || true

# Get Python version to determine user site-packages path
PYTHON_VERSION=$(python --version 2>&1 | awk '{print $2}' | cut -d. -f1,2)
PYTHON_USER_SITE=$(python -m site --user-site 2>/dev/null || echo "$HOME/.local/lib/python${PYTHON_VERSION}/site-packages")

# Ensure PYTHONPATH includes user site-packages
export PYTHONPATH="${PYTHON_USER_SITE}:${PYTHONPATH}"

echo "Installing requirements from requirements.txt..."
pip install --user --no-cache-dir -r /opt/airflow/requirements.txt

# Skip PySpark if SKIP_PYSPARK environment variable is set
if [ "${SKIP_PYSPARK:-false}" != "true" ]; then
    echo "Installing PySpark requirements from requirements-pyspark.txt..."
    pip install --user --no-cache-dir -r /opt/airflow/requirements-pyspark.txt
else
    echo "Skipping PySpark installation (SKIP_PYSPARK=true)"
fi

# Verify critical dependencies are installed (with explicit PYTHONPATH)
echo "Verifying critical dependencies..."
python -c "import sys; sys.path.insert(0, '${PYTHON_USER_SITE}'); import pydantic_settings; print(f'✓ pydantic_settings installed: {pydantic_settings.__version__}')" || {
    echo "✗ pydantic_settings not found!"
    echo "Debug: PYTHONPATH=${PYTHONPATH}"
    echo "Debug: User site-packages: ${PYTHON_USER_SITE}"
    pip list | grep -i pydantic || echo "No pydantic packages found"
    exit 1
}

echo "Requirements installation complete!"

