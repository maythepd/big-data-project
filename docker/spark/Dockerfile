# Spark plus the Python packages the jobs in docker-compose.spark.yml need beyond
# PySpark: python-dotenv for src/common/config.py, psycopg for the dashboard database
FROM apache/spark:3.5.9-python3
USER root
RUN pip install --no-cache-dir python-dotenv "psycopg[binary]" PyYAML
USER spark
