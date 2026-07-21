from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dp.view(
    name = "silver_airports_view"
)

def silver_airports_view():
    df  = spark.readStream.format('delta').load('/Volumes/dbx_dbt_flight/bronze/bronze_volume/airports/data/')
    
    df = df.withColumn('modified_date', current_timestamp())\
                    .drop('_rescued_data')
    
    return df


dp.create_streaming_table(
  name = 'dbx_dbt_flight.silver.airports'
)

dp.create_auto_cdc_flow(
    source = 'silver_airports_view',
    target = 'dbx_dbt_flight.silver.airports',
    keys = ['airport_id'],
    sequence_by = 'modified_date',
    stored_as_scd_type = 1,
)

