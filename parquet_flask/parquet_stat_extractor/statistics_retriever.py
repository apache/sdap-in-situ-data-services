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

from pyspark.sql.dataframe import DataFrame
import pyspark.sql.functions as pyspark_functions

from parquet_flask.insitu.file_structure_setting import FileStructureSetting
LOGGER = logging.getLogger(__name__)


class StatisticsRetriever:
    def __init__(self, input_dataset: DataFrame, file_structure_setting: FileStructureSetting):
        self.__file_structure_setting = file_structure_setting
        self.__input_dataset = input_dataset
        self.__stat_result = {}
        self.__querying_stat_list = []
        self.__result_keys = {}
        self.__special_mapping = {}

    def to_json(self) -> dict:
        """
        :return:
        """
        return self.__stat_result

    def __data_type_record_counts(self):
        data_columns = self.__file_structure_setting.get_data_columns()
        data_type_counts = {}
        for each_data_key in data_columns:
            try:
                obs_count = self.__input_dataset.where(self.__input_dataset[each_data_key].isNotNull()).count()
            except Exception as e:
                LOGGER.exception(f'error while getting total for key: {each_data_key}')
                obs_count = 0
            data_type_counts[each_data_key] = obs_count
        return data_type_counts

    def __excluded_min_max_count(self, column_name, excluded_val, is_min_stat=True):
        filtered_input_dsert = self.__input_dataset.where(f'{column_name} {">" if is_min_stat else "<"} {excluded_val}')
        querying_stat_list = [
            pyspark_functions.min(column_name) if is_min_stat else pyspark_functions.max(column_name)
        ]
        stats = filtered_input_dsert.select(querying_stat_list).collect()
        if len(stats) != 1:
            raise ValueError(f'invalid row count on stats function: {stats}')
        stats = stats[0].asDict()
        return stats[f'{"min" if is_min_stat else "max"}({column_name})']

    def start(self):
        # TODO abstraction - meta is part of data columns now. remove it?
        self.__stat_result = {}
        for each_stat_dict in self.__file_structure_setting.get_parquet_file_data_stats_config():
            if each_stat_dict['stat_type'] == 'minmax':
                self.__result_keys[f'min({each_stat_dict["column"]})'] = f'min_{each_stat_dict["output_name"]}'
                self.__result_keys[f'max({each_stat_dict["column"]})'] = f'max_{each_stat_dict["output_name"]}'
                if 'min_excluded' in each_stat_dict:
                    self.__stat_result[f'min({each_stat_dict["column"]})'] = self.__excluded_min_max_count(
                        each_stat_dict['column'],
                        each_stat_dict['min_excluded'],
                        True
                    )
                else:
                    self.__querying_stat_list.append(pyspark_functions.min(each_stat_dict['column']))
                if 'max_excluded' in each_stat_dict:
                    self.__stat_result[f'max({each_stat_dict["column"]})'] = self.__excluded_min_max_count(
                        each_stat_dict['column'],
                        each_stat_dict['max_excluded'],
                        False
                    )
                else:
                    self.__querying_stat_list.append(pyspark_functions.max(each_stat_dict['column']))
                if 'special_data_type' in each_stat_dict:
                    self.__special_mapping[f'min({each_stat_dict["column"]})'] = each_stat_dict['special_data_type']
                    self.__special_mapping[f'max({each_stat_dict["column"]})'] = each_stat_dict['special_data_type']
            elif each_stat_dict['stat_type'] == 'record_count':
                self.__result_keys['overall_totals'] = each_stat_dict["output_name"]
                self.__stat_result['overall_totals'] = int(self.__input_dataset.count())
            elif each_stat_dict['stat_type'] == 'data_type_record_count':
                self.__result_keys['individual_data_totals'] = each_stat_dict["output_name"]
                self.__stat_result['individual_data_totals'] = self.__data_type_record_counts()

        stats = self.__input_dataset.select(self.__querying_stat_list).collect()
        if len(stats) != 1:
            raise ValueError(f'invalid row count on stats function: {stats}')

        stats = stats[0].asDict()
        self.__stat_result = {**self.__stat_result, **stats}
        for k, v in self.__special_mapping.items():
            self.__stat_result[k] = getattr(stats[k], v)()
        renamed_stats = {}
        for k, v in self.__stat_result.items():
            renamed_stats[self.__result_keys[k]] = v
        self.__stat_result = renamed_stats
        return self
