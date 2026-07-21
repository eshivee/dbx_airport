from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dp.view(
    name = "silver_flights_view"
)

def silver_flights_view():
    df  = spark.readStream.format('delta').load('/Volumes/dbx_dbt_flight/bronze/bronze_volume/flights/data/')
    
    df = df.withColumn('flight_date', to_date(col('flight_date')))\
                    .withColumn('modified_date', current_timestamp())\
                    .drop('_rescued_data')
    
    return df


dp.create_streaming_table(
  name = 'dbx_dbt_flight.silver.flights'
)

dp.create_auto_cdc_flow(
    source = 'silver_flights_view',
    target = 'dbx_dbt_flight.silver.flights',
    keys = ['flight_id'],
    sequence_by = 'modified_date',
    stored_as_scd_type = 1,
)



