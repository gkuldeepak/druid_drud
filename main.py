from druid_crud import ingest_data, read_from_druid, update_data, delete_from_druid

# Create: Ingest data
print("Ingesting data...")
ingestion_response = ingest_data("data.json", "sample_datasource")
print(f"Ingestion Response: {ingestion_response}")

# Read: Query data
print("Querying data...")
query = "SELECT * FROM sample_datasource LIMIT 10"
query_result = read_from_druid(query)
print(f"Query Result: {query_result}")

# Update: Re-index data
print("Re-indexing data...")
update_spec = {
    "dataSchema": {
        "dataSource": "sample_datasource",
        "dimensionsSpec": {"dimensions": ["dim1", "dim2"]},
        "timestampSpec": {"column": "timestamp", "format": "iso"}
    },
    "inputSource": {
        "type": "local",
        "baseDir": "/path/to/data/",
        "filter": "updated_data.json"
    },
    "inputFormat": {"type": "json"}
}
update_response = update_data("sample_datasource", update_spec)
print(f"Update Response: {update_response}")

# Delete: Drop data source
print("Deleting data source...")
delete_status = delete_from_druid("sample_datasource")
print(f"Delete Status: {delete_status}")
