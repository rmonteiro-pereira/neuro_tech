# Restart Instructions

## To apply Java installation changes:

1. **Stop all containers:**
   ```powershell
   docker-compose down
   ```

2. **Restart containers (Java will be installed automatically):**
   ```powershell
   docker-compose up -d
   ```

3. **Monitor the startup:**
   ```powershell
   docker-compose logs -f airflow-scheduler
   ```
   
   Look for:
   - "Installing Java 11 (required for PySpark)..."
   - "Java installed: ..."
   - "Connecting to Spark cluster: spark://spark-master:7077"

4. **Verify Java is installed:**
   ```powershell
   docker exec airflow-scheduler java -version
   ```

5. **Verify Spark connection:**
   ```powershell
   docker exec airflow-scheduler python -c "from pyspark.sql import SparkSession; spark = SparkSession.builder.master('spark://spark-master:7077').appName('test').getOrCreate(); print('Spark connected!'); spark.stop()"
   ```

## Note
Java installation happens automatically when containers start. The first startup will take longer (~30-60 seconds) while Java is being installed.

