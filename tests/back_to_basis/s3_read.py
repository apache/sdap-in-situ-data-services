from datetime import datetime

import findspark
findspark.init()

from pyspark.sql import DataFrame
from parquet_flask.io_logic.cdms_schema import CdmsSchema
from parquet_flask.io_logic.retrieve_spark_session import RetrieveSparkSession
from parquet_flask.utils.config import Config
import pyspark.sql.functions as pyspark_functions

parquet_name = 's3a://cdms-dev-in-situ-parquet/CDMS_insitu.parquet/provider=NCAR/project=ICOADS Release 3.0/platform_code=41/year=2017/month=1'
config = Config()
spark = RetrieveSparkSession().retrieve_spark_session('Test1', config.get_value('master_spark_url'))
read_df: DataFrame = spark.read.schema(CdmsSchema.ALL_SCHEMA).parquet(parquet_name)

stats = read_df.select(pyspark_functions.min('latitude'), pyspark_functions.max('latitude'), pyspark_functions.min('longitude'), pyspark_functions.max('longitude'), pyspark_functions.min('depth'), pyspark_functions.max('depth'), pyspark_functions.min('time_obj'), pyspark_functions.max('time_obj')).collect()
stats = stats[0].asDict()

print(stats['min(time_obj)'])
print(stats['min(time_obj)'].strftime('%Y-%m-%dT%H:%M:%S.%fZ'))

filtered_input_dsert = read_df.where(f'depth != -99999')
stats = filtered_input_dsert.select(pyspark_functions.min('depth')).collect()
stats = stats[0].asDict()
print(stats)


