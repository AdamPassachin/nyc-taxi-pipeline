from pyspark.sql import functions as F

def ingest_bronze(spark, catalog, raw_path, raw_schema, raw_to_bronze_checkpoint):
    """
    Function responsible for transferring raw data from source to bronze delta table
    Args:
        spark - SparkSession object
        raw_path - Raw path in ADLS storage
        catalog - Unity Catalog
        raw_to_bronze_checkpoint - Checkpoint used for the autoloader to load from raw to bronze
    """
    # Autoloader
    print(f"Reading files at {raw_path}...")
    raw_df = (spark.readStream.format("cloudFiles")
        .option('cloudFiles.format','parquet',)
        .option("cloudFiles.schemaLocation", raw_schema)
        .load(raw_path)
    )

    bronze_df = (
            raw_df.withColumn(
                "ingestion_ts", F.current_timestamp(),
            ).withColumn(
                "source_file", F.col('_metadata.file_path')

            )

    )
    print(f'Writing files to {catalog}.bronze.yellow_taxi_trips...')

    # Writing new files to delta table yellow_taxi_trips
    query = (bronze_df.writeStream.trigger(availableNow=True)
             .option("checkpointLocation",raw_to_bronze_checkpoint)
             .toTable(f'{catalog}.bronze.yellow_taxi_trips')
    )
    query.awaitTermination()