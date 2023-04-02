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

from parquet_flask.insitu.get_query_transformer import GetQueryTransformer

from parquet_flask.aws.es_abstract import ESAbstract
from parquet_flask.aws.es_factory import ESFactory
from parquet_flask.insitu.file_structure_setting import FileStructureSetting
from parquet_flask.io_logic.parquet_path_retriever import ParquetPathRetriever
from parquet_flask.io_logic.partitioned_parquet_path import PartitionedParquetPath

LOGGER = logging.getLogger(__name__)


class ParquetQueryConditionManagementV4:
    def __init__(self, parquet_name: str, missing_depth_value, es_config: dict, file_structure_setting: FileStructureSetting, query_dict: dict):
        self.__file_structure_setting = file_structure_setting
        self.__conditions = []
        self.__columns = []
        self.__parquet_name = parquet_name if not parquet_name.endswith('/') else parquet_name[:-1]
        self.__query_dict = query_dict
        self.__missing_depth_value = missing_depth_value
        self.__parquet_names: [PartitionedParquetPath] = []
        self.__es_config = es_config

    def stringify_parquet_names(self):
        return [k.generate_path() for k in self.__parquet_names]

    @property
    def parquet_names(self):
        return self.__parquet_names

    @parquet_names.setter
    def parquet_names(self, val):
        """
        :param val:
        :return: None
        """
        self.__parquet_names = val
        return

    @property
    def conditions(self):
        return self.__conditions

    @conditions.setter
    def conditions(self, val):
        """
        :param val:
        :return: None
        """
        self.__conditions = val
        return

    @property
    def parquet_name(self):
        return self.__parquet_name

    @parquet_name.setter
    def parquet_name(self, val):
        """
        :param val:
        :return: None
        """
        self.__parquet_name = val
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

    def manage_query_props(self):
        query_transformer = GetQueryTransformer(self.__file_structure_setting)
        query_object = query_transformer.transform_param(self.__query_dict)
        LOGGER.debug(f'query_object: {query_object}')
        self.conditions = query_transformer.generate_parquet_conditions(query_object)
        LOGGER.debug(f'conditions: {self.conditions}')
        self.columns = query_transformer.generate_retrieving_columns(query_object)
        LOGGER.debug(f'columns: {self.columns}')
        aws_es: ESAbstract = ESFactory().get_instance('AWS', index=self.__es_config['es_index'], base_url=self.__es_config['es_url'], port=self.__es_config.get('es_port', 443))
        es_retriever = ParquetPathRetriever(aws_es, self.__file_structure_setting, self.__parquet_name)
        self.parquet_names = es_retriever.start(query_object)
        return self
