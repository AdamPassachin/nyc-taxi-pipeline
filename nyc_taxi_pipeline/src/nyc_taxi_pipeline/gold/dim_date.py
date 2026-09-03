from databricks.connect import DatabricksSession
from pyspark.sql import functions as F

spark = (
    DatabricksSession
    .builder
    .profile("azure-dev")
    .serverless()
    .getOrCreate()
)

def generate_dates():
    start_date = '2025-01-01'
    end_date = '2027-12-31'

    df = (
        spark.createDataFrame([{'date':1}])
        .select(F.explode(F.sequence(F.to_date(F.lit(start_date)),F.to_date(F.lit(end_date)),F.expr("INTERVAL 1 DAY"))).alias("calendar_date"))
    )

    df.show()

generate_dates()
