from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dp.view(
    name = "silver_passengers_view"
)

def silver_passengers_view():
    df  = spark.readStream.format('delta').load('/Volumes/dbx_dbt_flight/bronze/bronze_volume/passengers/data/')
    
    df = df.withColumn('modified_date', current_timestamp())\
                    .drop('_rescued_data')
    
    return df


dp.create_streaming_table(
  name = 'dbx_dbt_flight.silver.passengers'
)

dp.create_auto_cdc_flow(
    source = 'silver_passengers_view',
    target = 'dbx_dbt_flight.silver.passengers',
    keys = ['passenger_id'],
    sequence_by = 'modified_date',
    stored_as_scd_type = 1,
)
