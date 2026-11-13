# Producers: Kafka client, API requests, the station catalogs (Parquet) and the message contract (jsonschema)
FROM python:3.12-slim
RUN pip install --no-cache-dir "kafka-python>=3.0" pandas pyarrow python-dotenv requests "jsonschema==4.23.0"
