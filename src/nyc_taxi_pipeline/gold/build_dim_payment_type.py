
def build_dim_payment_type(spark,catalog):

    df = spark.createDataFrame(
        [(0,'Flex Fare'),(1,'Credit Card'),(2,'Cash'),(3,'No Charge'),(4,'Dispute'),(5,'Unknown'),(6,'Voided Trip')],
        ['payment_type_key','payment_type']
    )

    df.write.mode('ignore').saveAsTable(f'{catalog}.gold.dim_payment_type')

