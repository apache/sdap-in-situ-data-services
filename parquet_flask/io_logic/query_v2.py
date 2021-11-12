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
from datetime import datetime

# from pyspark import F
from pyspark.sql.dataframe import DataFrame

from parquet_flask.io_logic.cdms_constants import CDMSConstants
from parquet_flask.utils.config import Config
from parquet_flask.utils.time_utils import TimeUtils

LOGGER = logging.getLogger(__name__)

QUERY_PROPS_SCHEMA = {
    'type': 'object',
    'properties': {
        'start_from': {'type': 'integer'},
        'size': {'type': 'integer'},
        'columns': {
            'type': 'array',
            'items': {'type': 'string'},
            'minItems': 0,
        },
        'platform_code': {'type': 'string'},
        'provider': {'type': 'string'},
        'project': {'type': 'string'},
        'min_depth': {'type': 'number'},
        'max_depth': {'type': 'number'},
        'min_time': {'type': 'string'},
        'max_time': {'type': 'string'},
        'min_lat_lon': {'type': 'array', 'items': {'type': 'number'}, 'minItems': 2, 'maxItems': 2},
        'max_lat_lon': {'type': 'array', 'items': {'type': 'number'}, 'minItems': 2, 'maxItems': 2},
    },
    'required': ['start_from', 'size', 'min_depth', 'max_depth', 'min_time', 'max_time', 'min_lat_lon', 'max_lat_lon'],
}


class QueryProps:
    def __init__(self):
        self.__variable = None
        self.__quality_flag = False
        self.__platform_code = None
        self.__project = None
        self.__provider = None
        self.__device = None
        self.__min_depth = None
        self.__max_depth = None
        self.__min_datetime = None
        self.__max_datetime = None
        self.__min_lat_lon = None
        self.__max_lat_lon = None
        self.__start_at = 0
        self.__size = 0
        self.__columns = []

    @property
    def variable(self):
        return self.__variable

    @variable.setter
    def variable(self, val):
        """
        :param val:
        :return: None
        """
        self.__variable = val
        return

    @property
    def quality_flag(self):
        return self.__quality_flag

    @quality_flag.setter
    def quality_flag(self, val):
        """
        :param val:
        :return: None
        """
        self.__quality_flag = val
        return

    @property
    def platform_code(self):
        return self.__platform_code

    @platform_code.setter
    def platform_code(self, val):
        """
        :param val:
        :return: None
        """
        self.__platform_code = val
        return

    def from_json(self, input_json):
        self.start_at = input_json['start_from']
        self.size = input_json['size']
        self.min_depth = input_json['min_depth']
        self.max_depth = input_json['max_depth']
        self.min_datetime = input_json['min_time']
        self.max_datetime = input_json['max_time']
        self.min_lat_lon = input_json['min_lat_lon']
        self.max_lat_lon = input_json['max_lat_lon']
        if 'project' in input_json:
            self.project = input_json['project']
        if 'provider' in input_json:
            self.provider = input_json['provider']
        if 'device' in input_json:
            self.provider = input_json['device']
        if 'platform_code' in input_json:
            self.platform_code = input_json['platform_code']
        if 'columns' in input_json:
            self.columns = input_json['columns']
        return self

    @property
    def project(self):
        return self.__project

    @project.setter
    def project(self, val):
        """
        :param val:
        :return: None
        """
        self.__project = val
        return

    @property
    def provider(self):
        return self.__provider

    @provider.setter
    def provider(self, val):
        """
        :param val:
        :return: None
        """
        self.__provider = val
        return

    @property
    def device(self):
        return self.__device

    @device.setter
    def device(self, val):
        """
        :param val:
        :return: None
        """
        self.__device = val
        return

    @property
    def min_depth(self):
        return self.__min_depth

    @min_depth.setter
    def min_depth(self, val):
        """
        :param val:
        :return: None
        """
        self.__min_depth = val
        return

    @property
    def max_depth(self):
        return self.__max_depth

    @max_depth.setter
    def max_depth(self, val):
        """
        :param val:
        :return: None
        """
        self.__max_depth = val
        return

    @property
    def min_datetime(self):
        return self.__min_datetime

    @min_datetime.setter
    def min_datetime(self, val):
        """
        :param val:
        :return: None
        """
        self.__min_datetime = val
        return

    @property
    def max_datetime(self):
        return self.__max_datetime

    @max_datetime.setter
    def max_datetime(self, val):
        """
        :param val:
        :return: None
        """
        self.__max_datetime = val
        return

    @property
    def min_lat_lon(self):
        return self.__min_lat_lon

    @min_lat_lon.setter
    def min_lat_lon(self, val):
        """
        :param val:
        :return: None
        """
        self.__min_lat_lon = val
        return

    @property
    def max_lat_lon(self):
        return self.__max_lat_lon

    @max_lat_lon.setter
    def max_lat_lon(self, val):
        """
        :param val:
        :return: None
        """
        self.__max_lat_lon = val
        return

    @property
    def start_at(self):
        return self.__start_at

    @start_at.setter
    def start_at(self, val):
        """
        :param val:
        :return: None
        """
        self.__start_at = val
        return

    @property
    def size(self):
        return self.__size

    @size.setter
    def size(self, val):
        """
        :param val:
        :return: None
        """
        self.__size = val
        return

    @property
    def columns(self):
        return self.__columns

    @columns.setter
    def columns(self, val):
        """
        :param val:
        :return: None
        """
        self.__columns = val
        return


class Query:
    def __init__(self, props=QueryProps()):
        self.__props = props
        config = Config()
        self.__app_name = config.get_value('spark_app_name')
        self.__master_spark = config.get_value('master_spark_url')
        self.__parquet_name = config.get_value('parquet_file_name')

    def __add_conditions(self):
        conditions = []
        min_year = None
        max_year = None
        if self.__props.platform_code is not None:
            LOGGER.debug(f'setting platform_code condition: {self.__props.platform_code}')
            conditions.append(f"{CDMSConstants.platform_code_col} == '{self.__props.platform_code}'")
        if self.__props.provider is not None:
            LOGGER.debug(f'setting provider condition: {self.__props.provider}')
            conditions.append(f"{CDMSConstants.provider_col} == '{self.__props.provider}'")
        if self.__props.project is not None:
            LOGGER.debug(f'setting project condition: {self.__props.project}')
            conditions.append(f"{CDMSConstants.project_col} == '{self.__props.project}'")
        if self.__props.min_datetime is not None:
            LOGGER.debug(f'setting datetime min condition: {self.__props.min_datetime}')
            min_year = TimeUtils.get_datetime_obj(self.__props.min_datetime).year
            conditions.append(f"{CDMSConstants.year_col} >= {min_year}")
            conditions.append(f"{CDMSConstants.time_obj_col} >= '{self.__props.min_datetime}'")
        if self.__props.max_datetime is not None:
            LOGGER.debug(f'setting datetime max condition: {self.__props.max_datetime}')
            max_year = TimeUtils.get_datetime_obj(self.__props.max_datetime).year
            conditions.append(f"{CDMSConstants.year_col} <= {max_year}")
            conditions.append(f"{CDMSConstants.time_obj_col} <= '{self.__props.max_datetime}'")
        if min_year is not None and max_year is not None and min_year == max_year:
            LOGGER.debug(f'setting month duration condition: {self.__props.max_datetime}')
            conditions.append(f"{CDMSConstants.month_col} >= {TimeUtils.get_datetime_obj(self.__props.min_datetime).month}")
            conditions.append(f"{CDMSConstants.month_col} <= {TimeUtils.get_datetime_obj(self.__props.max_datetime).month}")
        if self.__props.min_lat_lon is not None:
            LOGGER.debug(f'setting Lat-Lon min condition: {self.__props.min_lat_lon}')
            conditions.append(f"{CDMSConstants.lat_col} >= {self.__props.min_lat_lon[0]}")
            conditions.append(f"{CDMSConstants.lon_col} >= {self.__props.min_lat_lon[1]}")
        if self.__props.max_lat_lon is not None:
            LOGGER.debug(f'setting Lat-Lon max condition: {self.__props.max_lat_lon}')
            conditions.append(f"{CDMSConstants.lat_col} <= {self.__props.max_lat_lon[0]}")
            conditions.append(f"{CDMSConstants.lon_col} <= {self.__props.max_lat_lon[1]}")
        if self.__props.min_depth is not None:
            LOGGER.debug(f'setting depth min condition: {self.__props.min_depth}')
            conditions.append(f"{CDMSConstants.depth_col} >= {self.__props.min_depth}")
        if self.__props.max_depth is not None:
            LOGGER.debug(f'setting depth max condition: {self.__props.max_depth}')
            conditions.append(f"{CDMSConstants.depth_col} <= {self.__props.max_depth}")
        if self.__props.variable is not None:
            LOGGER.debug(f'setting not null variable: {self.__props.variable}')
            conditions.append(f"{self.__props.variable} <= NULL")
            self.__props.columns.append(self.__props.variable)
            if self.__props.quality_flag is True:
                LOGGER.debug(f'adding quality flag for : {self.__props.variable}')
                self.__props.columns.append(f'{self.__props.variable}_quality')
        LOGGER.debug(f'conditions list: {conditions}')
        return ' AND '.join(conditions)

    def __retrieve_spark(self):
        from parquet_flask.io_logic.retrieve_spark_session import RetrieveSparkSession
        spark = RetrieveSparkSession().retrieve_spark_session(self.__app_name, self.__master_spark)
        return spark

    def __sql_query(self, spark_session=None):
        conditions = self.__add_conditions()
        sql_stmt = 'select count(*) from ParquetTable '
        if len(conditions) > 0:
            sql_stmt = f'{sql_stmt} where {conditions} ; '
        LOGGER.debug(f'query statement: {sql_stmt}')
        time_start = datetime.now()
        spark = self.__retrieve_spark() if spark_session is None else spark_session
        spark.read.parquet(self.__parquet_name).createOrReplaceTempView("parquetTable")
        read_df_time = datetime.now()
        LOGGER.debug(f'query_2 parquet read created at {read_df_time}. took: {read_df_time - time_start}')
        result_count = spark.sql(sql_stmt).collect()
        time_end = datetime.now()
        LOGGER.debug(f'query_2 count duration: {time_end - time_start}')

        sql_stmt = 'select * from ParquetTable '
        if len(conditions) > 0:
            sql_stmt = f'{sql_stmt} where {conditions} ; '  #  limit {self.__props.start_at + self.__props.size},{self.__props.size}
        LOGGER.debug(f'query statement: {sql_stmt}')
        removing_cols = [CDMSConstants.time_obj_col, CDMSConstants.year_col, CDMSConstants.month_col]
        result = spark.sql(sql_stmt).coalesce(1).limit(self.__props.start_at + self.__props.size).drop(*removing_cols).tail(self.__props.size)
        time_end = datetime.now()
        LOGGER.debug(f'query_2 result duration: {time_end - time_start}')
        return {'result': result}

    def search(self, spark_session=None):
        # LOGGER.debug(f'self.__sql_query(spark_session): {self.__sql_query(spark_session)}')
        conditions = self.__add_conditions()
        query_begin_time = datetime.now()
        LOGGER.debug(f'query begins at {query_begin_time}')
        spark = self.__retrieve_spark() if spark_session is None else spark_session
        created_spark_session_time = datetime.now()
        LOGGER.debug(f'spark session created at {created_spark_session_time}. duration: {created_spark_session_time - query_begin_time}')
        read_df: DataFrame = spark.read.parquet(self.__parquet_name)
        read_df_time = datetime.now()
        LOGGER.debug(f'parquet read created at {read_df_time}. duration: {read_df_time - created_spark_session_time}')
        query_result = read_df.where(conditions)
        query_result = query_result.coalesce(1)
        query_time = datetime.now()
        LOGGER.debug(f'parquet read filtered at {query_time}. duration: {query_time - read_df_time}')
        LOGGER.debug(f'total duration: {query_time - query_begin_time}')
        total_result = int(query_result.coalesce(1).count())
        # total_result = 1000  # faking this for now. TODO revert it.
        LOGGER.debug(f'total calc count duration: {datetime.now() - query_time}')
        if self.__props.size < 1:
            LOGGER.debug(f'returning only the size: {total_result}')
            return {
                'total': total_result,
                'results': [],
            }
        query_time = datetime.now()
        # result = query_result.withColumn('_id', F.monotonically_increasing_id())
        removing_cols = [CDMSConstants.time_obj_col, CDMSConstants.year_col, CDMSConstants.month_col]
        # result = result.where(F.col('_id').between(self.__props.start_at, self.__props.start_at + self.__props.size)).drop(*removing_cols)
        if len(self.__props.columns) > 0:
            result = query_result.select(self.__props.columns)
        LOGGER.debug(f'returning size : {total_result}')
        result = query_result.limit(self.__props.start_at + self.__props.size).drop(*removing_cols).tail(self.__props.size)
        query_result.unpersist()
        LOGGER.debug(f'total retrieval duration: {datetime.now() - query_time}')
        # spark.stop()
        return {
            'total': total_result,
            'results': [k.asDict() for k in result],
        }
