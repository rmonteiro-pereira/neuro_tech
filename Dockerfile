FROM apache/airflow:2.8.0-python3.11

# Switch to root to install system dependencies
USER root

# Install Java (required for PySpark)
RUN apt-get update -qq && \
    apt-get install -y -qq default-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Find and set JAVA_HOME (default-java is standard on Debian/Ubuntu)
# Also set it in profile files for shell sessions
RUN JAVA_HOME_PATH=$(find /usr/lib/jvm -maxdepth 1 -type d \( -name "java-*" -o -name "default-java" \) 2>/dev/null | head -1) && \
    if [ -z "$JAVA_HOME_PATH" ]; then \
        JAVA_HOME_PATH=/usr/lib/jvm/default-java; \
    fi && \
    echo "JAVA_HOME=$JAVA_HOME_PATH" >> /etc/environment && \
    echo "export JAVA_HOME=$JAVA_HOME_PATH" >> /etc/profile.d/java.sh && \
    chmod +x /etc/profile.d/java.sh

# Set JAVA_HOME environment variable persistently (default-java is the standard location)
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$JAVA_HOME/bin:$PATH

# Copy requirements files
COPY requirements.txt /opt/airflow/requirements.txt
COPY requirements-pyspark.txt /opt/airflow/requirements-pyspark.txt

# Switch to airflow user for Python package installation
USER airflow

# Set Python user installation paths
ENV PYTHONUSERBASE=/home/airflow/.local
ENV PYTHONNOUSERSITE=0
ENV PYTHONPATH=/home/airflow/.local/lib/python3.11/site-packages

# Install base requirements first (smaller packages)
RUN pip install --user --no-cache-dir -r /opt/airflow/requirements.txt

# Install PySpark and related dependencies (larger packages)
# This layer will be cached unless requirements-pyspark.txt changes
RUN pip install --user --no-cache-dir -r /opt/airflow/requirements-pyspark.txt

# Verify critical dependencies
RUN python -c "import sys; sys.path.insert(0, '/home/airflow/.local/lib/python3.11/site-packages'); import pyspark; print(f'PySpark {pyspark.__version__} installed successfully')" && \
    python -c "import sys; sys.path.insert(0, '/home/airflow/.local/lib/python3.11/site-packages'); import pydantic_settings; print(f'pydantic_settings {pydantic_settings.__version__} installed successfully')"

# Ensure proper permissions
USER root
RUN chown -R airflow:root /home/airflow/.local
USER airflow

