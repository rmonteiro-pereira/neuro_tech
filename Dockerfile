# IPTU Pipeline Dockerfile with Spark
FROM ubuntu:22.04

# Install system dependencies including Python 3.11 and uv
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-11-jdk \
    curl \
    ca-certificates \
    python3.11 \
    python3.11-venv \
    python3.11-dev \
    python3-pip \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install Spark
ARG SPARK_VERSION=3.5.0
ARG HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark \
    JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 \
    PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

RUN curl -sSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" | tar -xz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} $SPARK_HOME

# Set working directory
WORKDIR /app

# Environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app/src \
    PYSPARK_PYTHON=/usr/bin/python3.11 \
    PYSPARK_DRIVER_PYTHON=/usr/bin/python3.11

# Create spark user
RUN useradd -m spark

# Copy dependency files first
COPY requirements-pyspark.txt ./

# Create venv and install dependencies as root
RUN python3.11 -m venv /app/.venv && \
    /app/.venv/bin/pip install --no-cache-dir --upgrade pip setuptools wheel && \
    /app/.venv/bin/pip install --no-cache-dir -r requirements-pyspark.txt

# Copy source code
COPY . .

# Add venv to PATH
ENV PATH="/app/.venv/bin:$PATH"

# Fix permissions
RUN chown -R spark:spark /app

# Switch to spark user
USER spark

# Expose Spark ports
EXPOSE 4040 8080 8081 7077 6066

# Run the pipeline
CMD ["python3.11", "main.py"]
