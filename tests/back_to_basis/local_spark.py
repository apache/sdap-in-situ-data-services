import os


os.environ['master_spark_url'] = ''
os.environ['spark_app_name'] = ''
os.environ['parquet_file_name'] = ''
os.environ['aws_access_key_id'] = ''
os.environ['aws_secret_access_key'] = ''
os.environ['aws_session_token'] = ''
os.environ['in_situ_schema'] = '/Users/wphyo/Projects/access/parquet_test_1/in_situ_schema.json'
os.environ['authentication_type'] = ''
os.environ['authentication_key'] = ''
os.environ['parquet_metadata_tbl'] = ''

import json
from hashlib import sha1, sha256
from operator import add

from pyspark.worker import read_udfs

from parquet_flask.io_logic.sanitize_record import SanitizeRecord
from parquet_flask.utils.config import Config
from parquet_flask.utils.general_utils import GeneralUtils
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import to_timestamp, year, month, lit, dayofyear, hour
from pyspark.sql.types import StructType, StructField, DoubleType, DataType, StringType
from parquet_flask.io_logic.cdms_constants import CDMSConstants

spark = SparkSession.builder \
    .master("local") \
    .appName('TestAppName') \
    .getOrCreate()

import pyspark.sql.functions as pyspark_functions

# integer datatype is defined
# new_f = F.udf(GeneralUtils.floor_lat_long, StringType())
# read_udfs
with open('/Users/wphyo/Downloads/KAOU_20170514.json', 'r') as f:
    data_list = json.loads(f.read())
df: DataFrame = spark.createDataFrame(data_list['observations']) \
    .withColumn('hour', hour('time'))
# df = df.withColumn('geospatial_interval', lit(GeneralUtils.floor_lat_long(df.latitude, df.longitude)))
# df = df.withColumn('geospatial_interval', new_f(df['latitude'], df['longitude']))



_GEO_SPATIAL_INTERVAL = pyspark_functions.udf(lambda latitude, longitude: f'{int(latitude - divmod(latitude, 5)[1])}_{int(longitude - divmod(longitude, 5)[1])}', StringType())

df: DataFrame = df.withColumn('geo_spatial_interval', pyspark_functions.udf(lambda latitude, longitude: f'{int(latitude - divmod(latitude, 5)[1])}_{int(longitude - divmod(longitude, 5)[1])}', StringType())(df['latitude'], df['longitude']))
df.printSchema()
stats = df.select(min(CDMSConstants.lat_col))  # , max(CDMSConstants.lat_col), min(CDMSConstants.lon_col), max(CDMSConstants.lon_col), min(CDMSConstants.depth_col), max(CDMSConstants.depth_col)
print(stats)