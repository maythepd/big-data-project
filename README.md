#Big data project

# Vietnamese Air Quality Big data
## Phase 1: Data stream
- First, regulate which area to fetch data from, i.e giving locations and sensors that we will fetch data from:
```
python -m ingestion.openaq.fetch_dimensions      
python -m ingestion.waqi.fetch_dimensions         
python -m ingestion.openweather.fetch_dimensions   
```
this will return a table inside a parquet file signaling where to call API (most are api/{locationid})
- Second, forming the data stream:
```
python -m ingestion.openaq.producer
python -m ingestion.waqi.producer
python -m ingestion.openweather.producer
```
each make an API request using a key, then format them into 1 uniform format to process