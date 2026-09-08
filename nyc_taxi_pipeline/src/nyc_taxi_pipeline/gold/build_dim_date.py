from pyspark.sql import functions as F

def build_dim_date(spark,catalog):
    start_date = '2025-01-01'
    end_date = '2027-12-31'

    df = (
        spark.createDataFrame([{'date':1}])
        .select(F.explode(F.sequence(F.to_date(F.lit(start_date)),F.to_date(F.lit(end_date)),F.expr("INTERVAL 1 DAY"))).alias("calendar_date"))
    )
    df = (
        df.withColumn(
            'date_key', F.date_format('calendar_date','yyyyMMdd').cast('int')
         )
         .withColumn(
             'year', F.year('calendar_date')
         )
         .withColumn(
             'quarter', F.quarter('calendar_date')
         )
         .withColumn(
             'month', F.month('calendar_date')
         )
         .withColumn(
             'month_name', F.monthname('calendar_date')
         )
         .withColumn(
             'day', F.day('calendar_date')
         )
         .withColumn(
             'day_of_week', F.dayname('calendar_date')
         )
         .withColumn(
             'day_of_week_num', F.weekday('calendar_date')
         )
         .withColumn(
             'is_weekend', F.when((F.col('day_of_week_num') == 5) | (F.col('day_of_week_num') == 6), True).otherwise(False)
         )
    )

    df.write.mode('ignore').saveAsTable(f'{catalog}.gold.dim_date')

