from pyspark.sql import functions as F

def ingest_silver(spark, catalog):
    """
    This function reads data from the bronze delta table, applies data quality rules and ingests into silver layer
    Args:
      spark - SparkSession object
      catalog - Unity Catalog 
    """
    bronze_df = (
        spark.read.table(f'{catalog}.bronze.yellow_taxi_trips')
    )

    silver_df = (
        bronze_df.withColumn("duration_minutes",
            (F.unix_timestamp(F.col("tpep_dropoff_datetime")) - F.unix_timestamp(F.col("tpep_pickup_datetime"))) / 60
        )
    )
    silver_df.select("duration_minutes").show(5)
    

