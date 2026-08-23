from pyspark.sql import SparkSession
from nyc_taxi_pipeline.bronze.ingest_bronze import ingest_bronze

# Creating Spark session
spark = SparkSession.builder.appName("nyc-taxi-pipeline").getOrCreate()
raw_path = "/Volumes/nyc_taxi/bronze/raw_landing/yellow_taxi/year=2026/month=01"

def main():
    """
    This function orchestrates the scripts.
    """
    ingest_bronze(spark,raw_path)



