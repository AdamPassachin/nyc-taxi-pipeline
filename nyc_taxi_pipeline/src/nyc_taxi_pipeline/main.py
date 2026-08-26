from databricks.connect import DatabricksSession
from nyc_taxi_pipeline.bronze.ingest_bronze import ingest_bronze
from nyc_taxi_pipeline.silver.ingest_silver import ingest_silver
import argparse

# Creating Spark session
spark = DatabricksSession.builder.profile("azure-dev").serverless().getOrCreate()

def main():
    """
    This function orchestrates the scripts.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--catalog', required=True)
    parser.add_argument('--raw_path', required=True)
    parser.add_argument('--checkpoint_path', required=True)
    args = parser.parse_args()
    catalog = args.catalog
    raw_path = args.raw_path
    checkpoint_path = args.checkpoint_path
    
    ingest_bronze(spark,catalog,raw_path,checkpoint_path)
    ingest_silver(spark, catalog)



