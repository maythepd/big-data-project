# Spark with python-dotenv (config), pymongo (serving database), pyarrow and pandas (Silver/Gold
# file counts, AQI pandas UDFs)
# and pytest (the test suite runs here; it needs Java, which this image has)
FROM apache/spark:3.5.9-python3
USER root
RUN pip install --no-cache-dir python-dotenv "pymongo==4.10.1" PyYAML "pyarrow==17.0.0" "pandas==2.2.3" "pytest==8.3.4"
# owned by spark so the ivy named volume (docker-compose.spark.yml) is writable
RUN mkdir -p /tmp/.ivy2 && chown spark /tmp/.ivy2
USER spark
