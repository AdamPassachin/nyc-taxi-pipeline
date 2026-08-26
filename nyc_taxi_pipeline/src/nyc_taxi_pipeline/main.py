from pyspark.sql import SparkSession
from nyc_taxi_pipeline.bronze.ingest_bronze import ingest_bronze
import argparse

# Creating Spark session
spark = SparkSession.builder.appName("nyc-taxi-pipeline").getOrCreate()

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



