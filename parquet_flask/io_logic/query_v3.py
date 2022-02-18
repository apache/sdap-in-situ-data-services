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

from parquet_flask.io_logic.cdms_schema import CdmsSchema
from parquet_flask.io_logic.query_v2 import QueryProps
from pyspark.sql.dataframe import DataFrame

from parquet_flask.io_logic.cdms_constants import CDMSConstants
from parquet_flask.io_logic.parquet_query_condition_management import ParquetQueryConditionManagement
from parquet_flask.utils.config import Config
from parquet_flask.utils.general_utils import GeneralUtils

LOGGER = logging.getLogger(__name__)


class QueryV3:
    def __init__(self, props=QueryProps()):
        self.__props = props
        config = Config()
        self.__app_name = config.get_value('spark_app_name')
        self.__master_spark = config.get_value('master_spark_url')
        self.__parquet_name = config.get_value('parquet_file_name')
        self.__parquet_name = self.__parquet_name if not self.__parquet_name.endswith('/') else self.__parquet_name[:-1]
        self.__missing_depth_value = CDMSConstants.missing_depth_value
        self.__conditions = []
        self.__min_year = None
        self.__max_year = None
        self.__default_columns = [CDMSConstants.time_col, CDMSConstants.depth_col, CDMSConstants.lat_col, CDMSConstants.lon_col]   # , CDMSConstants.provider_col, CDMSConstants.project_col, CDMSConstants.platform_col]
        self.__set_missing_depth_val()

    def __set_missing_depth_val(self):
        possible_missing_depth = Config().get_value(Config.missing_depth_value)
        if GeneralUtils.is_int(possible_missing_depth):
            self.__missing_depth_value = int(possible_missing_depth)
        return

    def __retrieve_spark(self):
        from parquet_flask.io_logic.retrieve_spark_session import RetrieveSparkSession
        spark = RetrieveSparkSession().retrieve_spark_session(self.__app_name, self.__master_spark)
        return spark

    def search(self, spark_session=None):
        condition_manager = ParquetQueryConditionManagement(self.__parquet_name, self.__missing_depth_value, self.__props)
        condition_manager.manage_query_props()

        conditions = ' AND '.join(condition_manager.conditions)
        query_begin_time = datetime.now()
        LOGGER.debug(f'query begins at {query_begin_time}')
        spark = self.__retrieve_spark() if spark_session is None else spark_session
        created_spark_session_time = datetime.now()
        LOGGER.debug(f'spark session created at {created_spark_session_time}. duration: {created_spark_session_time - query_begin_time}')
        LOGGER.debug(f'__parquet_name: {condition_manager.parquet_name}')
        read_df: DataFrame = spark.read.schema(CdmsSchema.ALL_SCHEMA).parquet(condition_manager.parquet_name)
        read_df_time = datetime.now()
        LOGGER.debug(f'parquet read created at {read_df_time}. duration: {read_df_time - created_spark_session_time}')
        query_result = read_df.where(conditions)
        query_result = query_result
        query_time = datetime.now()
        LOGGER.debug(f'parquet read filtered at {query_time}. duration: {query_time - read_df_time}')
        LOGGER.debug(f'total duration: {query_time - query_begin_time}')
        total_result = int(query_result.count())
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
        if len(condition_manager.columns) > 0:
            query_result = query_result.select(condition_manager.columns)
        else:
            query_result = query_result.drop(*removing_cols)
        LOGGER.debug(f'returning size : {total_result}')
        result = query_result.limit(self.__props.start_at + self.__props.size).tail(self.__props.size)
        query_result.unpersist()
        LOGGER.debug(f'total retrieval duration: {datetime.now() - query_time}')
        # spark.stop()
        return {
            'total': total_result,
            'results': [k.asDict() for k in result],
        }
