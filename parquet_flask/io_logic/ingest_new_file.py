# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
from math import isnan
from os import environ
import json
from pandas import DataFrame as PandaDataFrame
from pyspark.sql.dataframe import DataFrame

from parquet_flask.io_logic.cdms_constants import CDMSConstants
from parquet_flask.io_logic.retrieve_spark_session import RetrieveSparkSession
from parquet_flask.io_logic.sanitize_record import SanitizeRecord
from parquet_flask.utils.config import Config
from parquet_flask.utils.file_utils import FileUtils

from pyspark.sql.functions import to_timestamp, year, month, lit, col

import pyspark.sql.functions as pyspark_functions # XXX: why not merge with import statement above?
from pyspark.sql.types import StringType, DoubleType

LOGGER = logging.getLogger(__name__)
GEOSPATIAL_INTERVAL = 30


def get_geospatial_interval(project: str) -> dict:
    """
    Get geospatial interval dict object from environment variable. If not found, return empty dict.

    :param project: project name
    :return: geospatial interval dict
    """
    interval_dict = {}
    geo_spatial_interval_by_platform = environ.get(CDMSConstants.geospatial_interval_by_platform)
    if not geo_spatial_interval_by_platform:
        return interval_dict
    geo_spatial_interval_by_platform_dict = json.loads(geo_spatial_interval_by_platform)
    if not isinstance(geo_spatial_interval_by_platform_dict, dict):
        return interval_dict
    if project not in geo_spatial_interval_by_platform_dict or not isinstance(geo_spatial_interval_by_platform_dict[project], dict):
        return interval_dict
    return geo_spatial_interval_by_platform_dict[project]


class IngestNewJsonFile:
    def __init__(self, is_overwriting=False, compression='GZIP'):
        self.__sss = RetrieveSparkSession()
        config = Config()
        self.__app_name = config.get_spark_app_name()
        self.__master_spark = config.get_value('master_spark_url')
        self.__mode = 'overwrite' if is_overwriting else 'append'
        self.__parquet_name = config.get_value('parquet_file_name')
        self.__sanitize_record = True
        self.__compression = compression

    @property
    def sanitize_record(self):
        return self.__sanitize_record

    @sanitize_record.setter
    def sanitize_record(self, val):
        """
        :param val:
        :return: None
        """
        self.__sanitize_record = val
        return

    @staticmethod
    def prepare_spark_df(spark_df, job_id, provider, project, include_geo_partition=False):
        LOGGER.debug(f'adding columns')
        try:
            df: DataFrame = spark_df.withColumn(CDMSConstants.time_obj_col, to_timestamp(CDMSConstants.time_col)) \
                .withColumn(CDMSConstants.year_col, year(CDMSConstants.time_col)) \
                .withColumn(CDMSConstants.month_col, month(CDMSConstants.time_col)) \
                .withColumn(CDMSConstants.platform_id_col, spark_df[CDMSConstants.platform_col][CDMSConstants.id_col]) \
                .withColumn(CDMSConstants.job_id_col, lit(job_id)) \
                .withColumn(CDMSConstants.provider_col, lit(provider)) \
                .withColumn(CDMSConstants.project_col, lit(project))
            if include_geo_partition:
                LOGGER.debug(f'creating geo_partition')
                geospatial_interval_dict = get_geospatial_interval(project)
                df: DataFrame = df.withColumn(
                    CDMSConstants.geo_spatial_interval_col,
                    pyspark_functions.udf(
                        lambda platform_code, latitude, longitude: f'{int(latitude - divmod(latitude, int(geospatial_interval_dict.get(platform_code, GEOSPATIAL_INTERVAL)))[1])}_{int(longitude - divmod(longitude, int(geospatial_interval_dict.get(platform_code, GEOSPATIAL_INTERVAL)))[1])}',
                        StringType())(
                        df[CDMSConstants.platform_code_col],
                        df[CDMSConstants.lat_col],
                        df[CDMSConstants.lon_col]))
            df: DataFrame = df.repartition(1)  # combine to 1 data frame to increase size
            LOGGER.debug(f'create writer')
            all_partitions = [
                CDMSConstants.provider_col,
                CDMSConstants.project_col,
                CDMSConstants.platform_id_col,
                CDMSConstants.geo_spatial_interval_col,
                CDMSConstants.year_col,
                CDMSConstants.month_col,
                CDMSConstants.job_id_col
            ] if include_geo_partition else [
                CDMSConstants.provider_col,
                CDMSConstants.project_col,
                CDMSConstants.platform_id_col,
                CDMSConstants.year_col,
                CDMSConstants.month_col,
                CDMSConstants.job_id_col
            ]
            df = df.repartition(1)
            df_writer = df.write
            LOGGER.debug(f'create partitions')
            df_writer = df_writer.partitionBy(all_partitions)
            LOGGER.debug(f'created partitions')
        except BaseException as e:
            LOGGER.exception(
                f'unexpected exception. latitude: {df[CDMSConstants.lat_col]}. longitude: {df[CDMSConstants.lon_col]}')
            raise e
        return df_writer

    @staticmethod
    def create_df(spark_session, data_list, job_id, provider, project):
        LOGGER.debug(f'creating data frame with length {len(data_list)}')
        df = spark_session.createDataFrame(data_list)
        return IngestNewJsonFile.prepare_spark_df(df, job_id, provider, project)

    def ingest_df(self, panda_df: PandaDataFrame, job_id: str, provider: str, project: str):
        # from parquet_flask.parquet_stat_extractor.local_spark_session import LocalSparkSession
        # spark_session = LocalSparkSession().get_spark_session()
        spark_session = self.__sss.retrieve_spark_session(self.__app_name, self.__master_spark)
        LOGGER.debug(f'creating data frame with length {panda_df.shape}')
        spark_df = spark_session.createDataFrame(panda_df)
        df_writer = self.prepare_spark_df(spark_df, job_id, provider, project)
        df_writer.mode(self.__mode).parquet(self.__parquet_name, compression=self.__compression)  # snappy GZIP
        LOGGER.debug(f'finished writing parquet')
        return panda_df.shape[0]

    def ingest(self, abs_file_path, job_id):
        """
        This method will assume that incoming file has data with in_situ_schema file.

        So, it will definitely has `time`, `project`, and `provider`.

        :param abs_file_path:
        :param job_id:
        :return: int - number of records
        """
        if not FileUtils.file_exist(abs_file_path):
            raise ValueError('missing file to ingest it. path: {}'.format(abs_file_path))
        LOGGER.debug(f'sanitizing the files ? : {self.__sanitize_record}')
        if self.sanitize_record is True:
            input_json = SanitizeRecord(Config().get_value('in_situ_schema')).start(abs_file_path)
        else:
            if not FileUtils.file_exist(abs_file_path):
                raise ValueError('json file does not exist: {}'.format(abs_file_path))
            input_json = FileUtils.read_json(abs_file_path)
        df_writer = self.create_df(
            self.__sss.retrieve_spark_session(self.__app_name, self.__master_spark),
            input_json[CDMSConstants.observations_key],
            job_id,
            input_json[CDMSConstants.provider_col],
            input_json[CDMSConstants.project_col])
        df_writer.mode(self.__mode).parquet(self.__parquet_name, compression=self.__compression)  # snappy GZIP
        LOGGER.debug(f'finished writing parquet')
        return len(input_json[CDMSConstants.observations_key])
