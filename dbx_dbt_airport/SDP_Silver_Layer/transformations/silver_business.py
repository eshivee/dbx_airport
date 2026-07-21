from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *


@dp.table(
    name = 'dbx_dbt_flight.silver.silver_business'
)

def silver_business():
    df = spark.readStream.table('dbx_dbt_flight.silver.bookings')
    df_business = df.join(spark.readStream.table('dbx_dbt_flight.silver.flights').drop('modified_date'), on = 'flight_id', how = 'inner')\
        .join(spark.readStream.table('dbx_dbt_flight.silver.airports').drop('modified_date'), on = 'airport_id', how = 'inner')\
        .join(spark.readStream.table('dbx_dbt_flight.silver.passengers').drop('modified_date'), on = 'passenger_id', how = 'inner')

    return df_business