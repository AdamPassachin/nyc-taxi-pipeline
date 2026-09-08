from databricks.connect import DatabricksSession
from nyc_taxi_pipeline.bronze.ingest_bronze import ingest_bronze
from nyc_taxi_pipeline.silver.ingest_silver import ingest_silver
from nyc_taxi_pipeline.gold.build_dim_date import build_dim_date
from nyc_taxi_pipeline.gold.build_dim_location import build_dim_location
from nyc_taxi_pipeline.gold.build_dim_payment_type import build_dim_payment_type
from nyc_taxi_pipeline.gold.build_dim_rate_code import build_dim_rate_code
from nyc_taxi_pipeline.gold.build_fact_taxi_trips import build_fact_taxi_trips
import argparse

def main():
    """
    This function orchestrates the scripts.
    """
    spark = DatabricksSession.builder.getOrCreate()

    parser = argparse.ArgumentParser()
    parser.add_argument('--catalog', required=True)
    parser.add_argument('--raw_path', required=True)
    parser.add_argument('--raw_schema', required=True)
    parser.add_argument('--raw_to_bronze_checkpoint', required=True)
    parser.add_argument('--bronze_to_silver_checkpoint', required=True)
    parser.add_argument('--silver_quarantine_checkpoint', required=True)
    args = parser.parse_args()
    catalog = args.catalog
    raw_path = args.raw_path
    raw_schema = args.raw_schema
    raw_to_bronze_checkpoint = args.raw_to_bronze_checkpoint
    bronze_to_silver_checkpoint = args.bronze_to_silver_checkpoint
    silver_quarantine_checkpoint = args.silver_quarantine_checkpoint
    
    ingest_bronze(spark,catalog,raw_path,raw_schema,raw_to_bronze_checkpoint)
    ingest_silver(spark, catalog, bronze_to_silver_checkpoint,silver_quarantine_checkpoint)
    if not spark.catalog.tableExists(f'{catalog}.gold.dim_date'):
        build_dim_date(spark,catalog)
    if not spark.catalog.tableExists(f'{catalog}.gold.dim_location'):
        build_dim_location(spark,catalog)
    if not spark.catalog.tableExists(f'{catalog}.gold.dim_payment_type'):
        build_dim_payment_type(spark,catalog)
    if not spark.catalog.tableExists(f'{catalog}.gold.dim_rate_code'):
        build_dim_rate_code(spark,catalog)
    build_fact_taxi_trips(spark,catalog)



