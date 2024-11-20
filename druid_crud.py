from pyspark.sql import SparkSession
import requests
from config import DRUID_SQL_ENDPOINT, DRUID_INGEST_ENDPOINT, DRUID_HOST

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Druid CRUD Operations") \
    .config("spark.jars", "/path/to/druid-sql-driver.jar") \
    .getOrCreate()

# 1. Create (Ingest Data)
def ingest_data(file_path, datasource_name):
    """
    Ingest data into Druid from a JSON or CSV file using REST API.
    """
    ingestion_spec = {
        "type": "index_parallel",
        "spec": {
            "dataSchema": {
                "dataSource": datasource_name,
                "dimensionsSpec": {
                    "dimensions": ["dim1", "dim2"]  # Replace with your schema
                },
                "timestampSpec": {
                    "column": "timestamp",  # Ensure your data has a timestamp column
                    "format": "iso"
                }
            },
            "ioConfig": {
                "type": "index_parallel",
                "inputSource": {
                    "type": "local",
                    "baseDir": "/path/to/data/",
                    "filter": file_path
                },
                "inputFormat": {
                    "type": "json"  # Use "csv" if ingesting CSV
                }
            },
            "tuningConfig": {"type": "index_parallel"}
        }
    }

    response = requests.post(DRUID_INGEST_ENDPOINT, json=ingestion_spec)
    return response.json()

# 2. Read
def read_from_druid(sql_query):
    """
    Query data from Druid using JDBC or REST API.
    """
    # Using REST API
    headers = {"Content-Type": "application/json"}
    payload = {"query": sql_query}
    response = requests.post(DRUID_SQL_ENDPOINT, json=payload, headers=headers)
    return response.json()

# 3. Update (Re-indexing in Druid)
def update_data(datasource_name, update_spec):
    """
    Perform updates in Druid by re-indexing.
    """
    ingestion_spec = {
        "type": "index_parallel",
        "spec": {
            "dataSchema": update_spec["dataSchema"],
            "ioConfig": {
                "type": "index_parallel",
                "inputSource": update_spec["inputSource"],
                "inputFormat": update_spec["inputFormat"]
            },
            "tuningConfig": {"type": "index_parallel"}
        }
    }
    response = requests.post(DRUID_INGEST_ENDPOINT, json=ingestion_spec)
    return response.json()

# 4. Delete
def delete_from_druid(datasource_name):
    """
    Delete a Druid data source.
    """
    delete_endpoint = f"{DRUID_HOST}/druid/coordinator/v1/datasources/{datasource_name}"
    response = requests.delete(delete_endpoint)
    return response.status_code
