from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("nyc-taxi-pipeline").getOrCreate()
raw_path = "abfss://nyc-taxi@stgnyctaxidev.dfs.core.windows.net/raw/yellow_taxi/year=2026/month=01"

raw_df = spark.read.parquet('/Users/adampassachin/Desktop/yellow_tripdata_2026-01.parquet')
ts_df = (
        raw_df.withColumn(
            "ingestion_ts", F.current_timestamp(),
        )



)
