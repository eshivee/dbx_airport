from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dp.table(
    name = 'dbx_dbt_flight.silver.stg_bookings'
)

def silver_stg_bookings():
    df = spark.readStream.format('delta')\
                    .load('/Volumes/dbx_dbt_flight/bronze/bronze_volume/bookings/data/')
    return df


@dp.view(
    name =  'silver_bookings_view'
)
def silver_bookings_view():
    df = spark.readStream.table('dbx_dbt_flight.silver.stg_bookings')

    df = df.withColumn('amount', col('amount').cast('double'))\
            .withColumn('booking_date', col('booking_date').cast('date'))\
            .withColumn('modified_date', current_timestamp())\
            .drop('_rescued_data')
    return df 

rules = {
    'rule_1' : "booking_id IS NOT NULL",
    'rule_2' : "passenger_id IS NOT NULL"
}

@dp.table(
    name = 'dbx_dbt_flight.silver.bookings'
)
@dp.expect_all(rules)
def silver_bookings():
    df = spark.readStream.table('silver_bookings_view')
    return df





















