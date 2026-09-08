from pyspark.sql import functions as F

def build_dim_location(spark,catalog):
    df = spark.read.option("header", True).csv(f'/Volumes/{catalog}/silver/reference/taxi_zone_lookup.csv')

    df = (
        df.select(F.col('LocationID').cast('int'),'Borough', 'Zone', 'service_zone')
    )

    df.write.mode('ignore').saveAsTable(f'{catalog}.gold.dim_location')


