from nyc_taxi_pipeline.silver.ingest_silver import ingest_silver
from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.profile("azure-dev").serverless().getOrCreate()

ingest_silver(spark,"nyc_taxi_dev","/Volumes/nyc_taxi_dev/silver/bronze_to_silver","/Volumes/nyc_taxi_dev/silver/silver_quarantine")