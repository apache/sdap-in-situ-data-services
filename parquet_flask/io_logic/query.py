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

from parquet_flask.io_logic.retrieve_spark_session import RetrieveSparkSession
from parquet_flask.utils.config import Config

LOGGER = logging.getLogger(__name__)


class QueryParquet:
    def __init__(self):
        self.__sss = RetrieveSparkSession()
        config = Config()
        self.__app_name = config.get_value('spark_app_name')
        self.__master_spark = config.get_value('master_spark_url')
        self.__parquet_name = config.get_value('parquet_file_name')
        self.__depth = None
        self.__min_time = None
        self.__max_time = None
        self.__min_lat_lon = None
        self.__max_lat_lon = None
        self.__limit = 0
        self.__size = 1000

    @property
    def limit(self):
        return self.__limit

    @limit.setter
    def limit(self, val):
        """
        :param val:
        :return: None
        """
        self.__limit = val
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
    def depth(self):
        return self.__depth

    @depth.setter
    def depth(self, val):
        """
        :param val:
        :return: None
        """
        self.__depth = val
        return

    @property
    def min_time(self):
        return self.__min_time

    @min_time.setter
    def min_time(self, val):
        """
        :param val:
        :return: None
        """
        self.__min_time = val
        return

    @property
    def max_time(self):
        return self.__max_time

    @max_time.setter
    def max_time(self, val):
        """
        :param val:
        :return: None
        """
        self.__max_time = val
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

    def __construct_query(self):
        conditions = []
        if self.depth is not None:
            conditions.append(f'depth >= {self.depth}')
        if self.min_time is not None:
            conditions.append(f'time_obj >= {self.min_time}')
        if self.max_time is not None:
            conditions.append(f'time_obj <= {self.max_time}')
        if self.min_lat_lon is not None:
            conditions.append(f'latitude >= {self.min_lat_lon[0]}')
            conditions.append(f'longitude >= {self.min_lat_lon[1]}')
        if self.max_lat_lon is not None:
            conditions.append(f'latitude <= {self.max_lat_lon[0]}')
            conditions.append(f'longitude <= {self.max_lat_lon[1]}')
        return conditions

    def search(self):
        conditions = self.__construct_query()
        conditions = ' AND '.join(conditions)
        sql_stmt = 'select * from ParquetTable '
        if len(conditions) > 0:
            sql_stmt = '{} where {} ; '.format(sql_stmt, conditions)
        LOGGER.debug(f'query statement: {sql_stmt}')
        spark = self.__sss.retrieve_spark_session(self.__app_name, self.__master_spark)
        spark.read.parquet(self.__parquet_name).createOrReplaceTempView("parquetTable")
        result = spark.sql(sql_stmt).limit(self.limit).collect()
        LOGGER.debug(f'query result: {result}')
        return [k.asDict() for k in result]
