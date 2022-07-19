import json

import findspark
from parquet_flask.io_logic.cdms_constants import CDMSConstants
from pyspark.sql.dataframe import DataFrame

from parquet_flask.utils.file_utils import FileUtils

from parquet_flask.aws.aws_s3 import AwsS3

findspark.init()
from parquet_flask.utils.config import Config

from parquet_flask.io_logic.retrieve_spark_session import RetrieveSparkSession

config = Config()
spark = RetrieveSparkSession().retrieve_spark_session('Test1', config.get_value('master_spark_url'))
s3 = AwsS3()
local_file_path = s3.set_s3_url('s3://cdms-dev-fsu-in-situ-stage/KAOU_20170222.json.gz').download('/tmp')
local_file_path = FileUtils.gunzip_file_os(local_file_path)
with open(local_file_path, 'r') as f:
    data_list = json.loads(f.read())
df: DataFrame = spark.createDataFrame(data_list['observations'])

import pyspark.sql.functions as pyspark_functions
from pyspark.sql.types import StringType

from pyspark.sql.functions import to_timestamp, year, month, lit

df: DataFrame = df.withColumn(CDMSConstants.time_obj_col, to_timestamp(CDMSConstants.time_col))\
            .withColumn(CDMSConstants.year_col, year(CDMSConstants.time_col))\
            .withColumn(CDMSConstants.month_col, month(CDMSConstants.time_col))\
            .withColumn(CDMSConstants.platform_code_col, df[CDMSConstants.platform_col][CDMSConstants.code_col])\
            .withColumn(CDMSConstants.job_id_col, lit("job-id-1"))\
            .withColumn(CDMSConstants.provider_col, lit('provider-1'))\
            .withColumn(CDMSConstants.project_col, lit('project-1'))

df: DataFrame = df.withColumn('geo_spatial_interval', pyspark_functions.udf(lambda latitude, longitude: f'{int(latitude - divmod(latitude, 5)[1])}_{int(longitude - divmod(longitude, 5)[1])}', StringType())(df['latitude'], df['longitude']))



df.sort()
print(df.schema)
df = df.repartition(1)
all_partitions = [CDMSConstants.provider_col, CDMSConstants.project_col, CDMSConstants.platform_code_col,
                          CDMSConstants.year_col, CDMSConstants.month_col,
                          'geo_spatial_interval',
                          CDMSConstants.job_id_col]

df_writer = df.write.partitionBy(all_partitions)   # .partitionBy(['hour'])
df_writer.mode('append').parquet('s3a://cdms-dev-in-situ-parquet/back_to_basis.1', compression='snappy')  # snappy GZIP
