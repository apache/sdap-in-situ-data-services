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

from typing import Union

from pyspark.sql.dataframe import DataFrame

from parquet_flask.io_logic.cdms_constants import CDMSConstants
from parquet_flask.utils.config import Config
from parquet_flask.utils.general_utils import GeneralUtils
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
        'platform_code': {'type': 'array', 'items': {'type': 'string'}, 'minItems': 1},
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
        self.__variable: list = []
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
    def variable(self) -> list:
        return self.__variable

    @variable.setter
    def variable(self, val: list):
        """
        :param val: list
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
        if 'variable' in input_json:
            self.variable = input_json['variable']
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
