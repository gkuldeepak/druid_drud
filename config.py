# config.py
DRUID_HOST = "http://localhost:8888"
DRUID_SQL_ENDPOINT = f"{DRUID_HOST}/druid/v2/sql"
DRUID_INGEST_ENDPOINT = f"{DRUID_HOST}/druid/indexer/v1/task"
DRUID_DATASOURCE = "your_datasource_name"

# JDBC configuration
JDBC_URL = "jdbc:druid://localhost:8082/druid/v2/sql/"
JDBC_DRIVER = "org.apache.druid.sql.Driver"
JDBC_USER = "druid"
JDBC_PASSWORD = "password"
