
def build_dim_rate_code(spark,catalog):

    df = spark.createDataFrame(
        [(1,'Standard Rate'),(2,'JFK'),(3,'Newark'),(4,'Nassau or Westchester'),(5,'Negotiated Fare'),(6,'Group Ride'),(99,'Null/unknown')],
        ['rate_code_key','rate_code']
    )

    df.write.mode('ignore').saveAsTable(f'{catalog}.gold.dim_rate_code')
