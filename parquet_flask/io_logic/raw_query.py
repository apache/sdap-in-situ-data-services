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
from pyspark.sql.dataframe import DataFrame

from parquet_flask.io_logic.cdms_constants import CDMSConstants
from parquet_flask.utils.config import Config
LOGGER = logging.getLogger(__name__)


class RawQueryProps:
    def __init__(self):
        self.__start_at = 0
        self.__size = 0
        self.__columns = []

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


class RawQuery:
    def __init__(self, props: RawQueryProps=RawQueryProps()):
        self.__props = props
        config = Config()
        self.__app_name = config.get_spark_app_name()
        self.__master_spark = config.get_value('master_spark_url')
        self.__parquet_name = config.get_value('parquet_file_name')

    def __retrieve_spark(self):
        from parquet_flask.io_logic.retrieve_spark_session import RetrieveSparkSession
        spark = RetrieveSparkSession().retrieve_spark_session(self.__app_name, self.__master_spark)
        return spark

    def search(self, conditions: str, spark_session=None):
        """

        :param conditions: str - SQL conditions string
        :param spark_session:
        :return:
        """
        # LOGGER.debug(f'self.__sql_query(spark_session): {self.__sql_query(spark_session)}')
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
