from nyc_taxi_pipeline.bronze.ingest_bronze import ingest_bronze
from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.profile("azure-dev").serverless().getOrCreate()

ingest_bronze(spark,"nyc_taxi_dev","/Volumes/nyc_taxi_dev/bronze/raw_landing","/Volumes/nyc_taxi_dev/bronze/raw_schema","/Volumes/nyc_taxi_dev/bronze/raw_to_bronze")