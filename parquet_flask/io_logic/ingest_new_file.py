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

import pandas
from pyspark.sql.dataframe import DataFrame

from parquet_flask.insitu.file_structure_setting import FileStructureSetting
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


def get_geospatial_interval(project: str) -> int:
    """
    Get geospatial interval from environment variable. If not found, return default value.

    :param project: project name
    :return: geospatial interval
    """
    interval = GEOSPATIAL_INTERVAL
    geo_spatial_interval_by_project = environ.get(CDMSConstants.geospatial_interval_by_project)
    if not geo_spatial_interval_by_project:
        return interval
    geo_spatial_interval_by_project_dict = json.loads(geo_spatial_interval_by_project)
    if not isinstance(geo_spatial_interval_by_project_dict, dict):
        return interval
    if project not in geo_spatial_interval_by_project_dict:
        return interval
    try:
        return int(geo_spatial_interval_by_project_dict[project])
    except:
        LOGGER.exception(
            f'geo_spatial_interval_by_project_dict for {project} is not int: {geo_spatial_interval_by_project_dict[project]}')
        return interval


class IngestNewJsonFile:
    def __init__(self, is_overwriting=False):
        self.__sss = RetrieveSparkSession()
        config = Config()
        self.__app_name = config.get_spark_app_name()
        self.__master_spark = config.get_value('master_spark_url')
        self.__mode = 'overwrite' if is_overwriting else 'append'
        self.__parquet_name = config.get_value('parquet_file_name')
        self.__sanitize_record = True
        self.__file_structure_setting = FileStructureSetting(FileUtils.read_json(Config().get_value(Config.in_situ_schema)), FileUtils.read_json(Config().get_value(Config.file_structure_setting)))
        self.__pyspark_func_mappers = {
            'time': to_timestamp,
            'year': year,
            'month': month,
            'literal': lit,
            'column': col,
        }

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

    def create_df(self, spark_session, input_json):
        data_list = input_json[self.__file_structure_setting.get_data_array_key()]
        LOGGER.debug(f'creating data frame with length {len(data_list)}')
        df = spark_session.createDataFrame(data_list)
        # spark_session.sparkContext.addPyFile('/usr/app/parquet_flask/lat_lon_udf.py')
        LOGGER.debug(f'adding columns')
        df = self.__add_columns(df, input_json)
        try:
            df: DataFrame = df.repartition(1)  # combine to 1 data frame to increase size
            # .withColumn('ingested_date', lit(TimeUtils.get_current_time_str()))
            LOGGER.debug(f'create writer')
            df = df.repartition(1)  # XXX: is this line repeated?
            df_writer = df.write
            LOGGER.debug(f'create partitions')
            df_writer = df_writer.partitionBy(self.__file_structure_setting.get_partitioning_columns())
            LOGGER.debug(f'created partitions')
        except BaseException as e:
            LOGGER.exception(f'unexpected exception.')
            raise e
        return df_writer

    def __add_columns(self, df: DataFrame, input_json: dict):
        for column_name, column_details in self.__file_structure_setting.get_derived_columns().items():
            original_column = column_details['original_column']
            updated_type = column_details['updated_type']
            if updated_type == 'insitu_geo_spatial':
                geospatial_interval = get_geospatial_interval(input_json[column_details['split_interval_key']])
                df: DataFrame = df.withColumn(
                    column_name,
                    pyspark_functions.udf(
                        lambda latitude, longitude: f'{int(latitude - divmod(latitude, geospatial_interval)[1])}_{int(longitude - divmod(longitude, geospatial_interval)[1])}',
                        StringType())(
                        df[original_column[0]],
                        df[original_column[1]]))
                continue
            if updated_type == 'literal':
                original_column = input_json[original_column]
            df = df.withColumn(column_name, self.__pyspark_func_mappers[updated_type](original_column))
        return df

    def ingest(self, abs_file_path, job_id, spark_session=None):
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
            input_json = SanitizeRecord(Config().get_value('in_situ_schema'), self.__file_structure_setting).start(abs_file_path)
        else:
            if not FileUtils.file_exist(abs_file_path):
                raise ValueError('json file does not exist: {}'.format(abs_file_path))
            input_json = FileUtils.read_json(abs_file_path)
        for each_record in input_json[self.__file_structure_setting.get_data_array_key()]:
            # TODO abstraction: why is this hardcoded?
            # this is because of JSON schema limitation
            # in json schema, "float" is validated using the type "number".
            # both integers and floats are valid for "number".
            # result is sometimes ints are part of raw data (usually missing values flag like -99999).
            # easier solution is to use pandas / numpy to force the numbers to float.
            # But that means 1 more conversion layer which might cause more discrepancies from original data.
            # current approach is to manually forcing the type record by record which is pretty slow.
            if 'depth' in each_record:
                each_record['depth'] = float(each_record['depth'])
            if 'wind_from_direction' in each_record:
                each_record['wind_from_direction'] = float(each_record['wind_from_direction'])
            if 'wind_to_direction' in each_record:
                each_record['wind_to_direction'] = float(each_record['wind_from_direction'])
        input_json['job_id'] = job_id
        current_spark_session = self.__sss.retrieve_spark_session(
                self.__app_name,
                self.__master_spark) if spark_session is None else spark_session
        df_writer = self.create_df(current_spark_session,
                                   input_json)
        df_writer.mode(self.__mode).parquet(self.__parquet_name, compression='GZIP')  # snappy GZIP
        LOGGER.debug(f'finished writing parquet')
        return len(input_json[self.__file_structure_setting.get_data_array_key()])
