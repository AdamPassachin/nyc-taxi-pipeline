from pyspark.sql import functions as F

def ingest_silver(spark, catalog, bronze_to_silver_checkpoint, silver_quarantine_checkpoint):
    """
    This function reads data from the bronze delta table, applies data quality rules and ingests into silver layer
    Args:
      spark - SparkSession object
      catalog - Unity Catalog 
      bronze_to_silver_checkpoint - Checkpoint used for the delta streaming to load from bronze to silver
      silver_quarantine_checkpoint - Checkpoint used for the delta streaming to load from bronze to quarantine
    """
    print(f"Reading data from {catalog}.bronze.yellow_taxi_trips...")
    df = (
        spark.readStream
        .option("skipChangeCommits", "true")
        .table(f'{catalog}.bronze.yellow_taxi_trips')
    )

    df = (
        df.withColumn("duration_minutes",
            (F.unix_timestamp(F.col("tpep_dropoff_datetime")) - F.unix_timestamp(F.col("tpep_pickup_datetime"))) / 60
        )
    )

    # Keep only rows with valid duration
    valid_df = (
        df.filter((F.col("duration_minutes") > 0) & (F.col("duration_minutes") < 360))
    )

    # Quarantine invalid rows
    quarantine_df = (
        df.filter((F.col("duration_minutes") < 0) | (F.col("duration_minutes") > 360))
    )

    print(f"Ingesting data to {catalog}.silver.yellow_taxi_trips...")
    silver_data = (
    valid_df.writeStream
    .trigger(availableNow=True)
    .option("checkpointLocation",bronze_to_silver_checkpoint)
    .toTable(f'{catalog}.silver.yellow_taxi_trips')
    )

    print(f"Ingesting quarantine data into {catalog}.silver.quarantine_trips...")
    quarantine_data = (
    quarantine_df.writeStream
    .trigger(availableNow = True)
    .option("checkpointLocation", silver_quarantine_checkpoint)
    .toTable(f'{catalog}.silver.quarantine_trips')
    )

    silver_data.awaitTermination()
    quarantine_data.awaitTermination()
    
