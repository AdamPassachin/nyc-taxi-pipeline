from pyspark.sql import functions as F

def ingest_bronze(spark, catalog, raw_path, checkpoint_path):
    """
    Function responsible for transferring raw data from source to bronze delta table
    Args:
        spark - SparkSession object
        raw_path - Raw path in ADLS storage
        catalog - Unity Catalog
        checkpoint_path - Checkpoint used for the autoloader
    """
    # Autoloader
    print(f"Reading files at {raw_path}...")
    raw_df = (spark.readStream.format("cloudFiles")
        .option('cloudFiles.format','parquet',)
        .option('cloudFiles.schemaLocation',checkpoint_path)
        .load(raw_path)
    )

    bronze_df = (
            raw_df.withColumn(
                "ingestion_ts", F.current_timestamp(),
            ).withColumn(
                "source_file", F.col('_metadata.file_path')

            )

    )
    print("Writing raw files to bronze...")

    # Writing new files to delta table yellow_taxi_trips
    query = (bronze_df.writeStream.trigger(availableNow=True)
             .option("checkpointLocation",checkpoint_path)
             .toTable(f'{catalog}.bronze.yellow_taxi_trips')
    )
    query.awaitTermination()