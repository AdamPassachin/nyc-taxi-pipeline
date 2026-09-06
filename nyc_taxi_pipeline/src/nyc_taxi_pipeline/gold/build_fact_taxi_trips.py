from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.profile("azure-dev").serverless().getOrCreate()
def build_fact_taxi_trips(spark,catalog):

    df = spark.read.table(f'{catalog}.silver.yellow_taxi_trips')

    df.select(
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
    )

    df = (
        df.withColumnRenamed('PULocationID','pickup_location_key')
        .withColumnRenamed('DOLocationID','dropoff_location_key')
        .withColumnRenamed('RatecodeID','rate_code_key')
        .withColumnRenamed('payment_type_key','payment_type_key')
    )
    df.show()
build_fact_taxi_trips(spark,'nyc_taxi_dev')