from pyspark.sql import functions as F

def ingest_bronze(spark, catalog, path):
    """
    Function responsible for transferring raw data from source to bronze delta table
    Args:
        spark - SparkSession object
        path - Raw path in ADLS storage
        catalog - Unity Catalog
    """

    print(f"Reading files at {path}...")
    raw_df = spark.read.parquet(path)
    ts_df = (
            raw_df.withColumn(
                "ingestion_ts", F.current_timestamp(),
            ).withColumn(
                "source_file", F.col('_metadata.file_path')

            )

    )
    print("Writing raw files to bronze...")
    ts_df.write.mode('append').format('delta').saveAsTable(f'{catalog}.bronze.yellow_taxi_trips')
