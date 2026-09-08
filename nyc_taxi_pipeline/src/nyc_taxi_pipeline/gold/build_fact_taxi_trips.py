from pyspark.sql import functions as F
from delta.tables import DeltaTable

def build_fact_taxi_trips(spark,catalog):

    df_silver = spark.read.table(f'{catalog}.silver.yellow_taxi_trips')

    df_silver = df_silver.select(
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "RatecodeID",
    "store_and_fwd_flag",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "Airport_fee",
    "cbd_congestion_fee",
    "year",
    "month",
    "duration_minutes",
    "ingestion_ts"
    )

    df_silver= (
        df_silver.withColumnRenamed('PULocationID','pickup_location_key')
        .withColumnRenamed('DOLocationID','dropoff_location_key')
        .withColumnRenamed('RatecodeID','rate_code_key')
        .withColumnRenamed('payment_type','payment_type_key')
    )

    df_silver = (
        df_silver.withColumn('pickup_date_key', F.date_format('tpep_pickup_datetime','yyyyMMdd').cast('int'))
        .withColumn('dropoff_date_key', F.date_format('tpep_dropoff_datetime','yyyyMMdd').cast('int'))
    )

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.gold.pipeline_state (
        pipeline_name STRING,
        last_processed_ingestion_ts TIMESTAMP
    )
    USING DELTA
    """)

    state_df = (spark.table(f"{catalog}.gold.pipeline_state")
                .filter(F.col('pipeline_name') == 'silver_to_fact_taxi_trips')
    )

    row = state_df.select('last_processed_ingestion_ts').first()
    last_watermark = (
        row['last_processed_ingestion_ts'] if row is not None else None
    )

    if last_watermark is not None:
        df_silver = df_silver.filter(F.col('ingestion_ts') > F.lit(last_watermark))


    print(f"Rows to process into {catalog}.gold.fact_taxi_trips:", df_silver.count())
    df_silver.write.mode('append').saveAsTable(f'{catalog}.gold.fact_taxi_trips')

    new_watermark = df_silver.agg(F.max("ingestion_ts").alias('new_watermark')).first()['new_watermark']

    if new_watermark is not None:
        state_table = DeltaTable.forName(spark, f'{catalog}.gold.pipeline_state')
        source_df = spark.createDataFrame([('silver_to_fact_taxi_trips', new_watermark)], ["pipeline_name", "last_processed_ingestion_ts"])

        (state_table.alias('target')
        .merge(source_df.alias('source'), "target.pipeline_name = source.pipeline_name")
        .whenMatchedUpdate(set= 
                        {
                            'last_processed_ingestion_ts': 'source.last_processed_ingestion_ts'
                        }
                )
        .whenNotMatchedInsert(values=
                            {
                                'pipeline_name' : 'source.pipeline_name',
                                'last_processed_ingestion_ts' : 'source.last_processed_ingestion_ts'

                    }
            )
            .execute()
        )
