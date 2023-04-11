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

import json
import logging

from parquet_flask.insitu.file_structure_setting import FileStructureSetting
from parquet_flask.insitu.get_query_transformer import GetQueryTransformer
from parquet_flask.io_logic.cdms_schema import CdmsSchema
from parquet_flask.io_logic.cdms_constants import CDMSConstants  # This is done.
from parquet_flask.aws.es_abstract import ESAbstract
from parquet_flask.utils.time_utils import TimeUtils

LOGGER = logging.getLogger(__name__)


class SubCollectionStatistics:
    def __init__(self, es_mw: ESAbstract, insitu_schema: dict, query_dict: dict, file_struct_setting: FileStructureSetting):
        self.__query_dict = query_dict
        self.__file_struct_setting = file_struct_setting
        self.__es: ESAbstract = es_mw
        self.__insitu_schema = insitu_schema
        self.__data_column_names = self.__file_struct_setting.get_data_columns()

    def __transform_result(self, transformer_type, input_value):
        if transformer_type == 'datetime':
            return TimeUtils().parse_from_unix(input_value, True).get_datetime_str()
        raise ValueError(f'unknown transformer_type: {transformer_type}')

    def __retrieve_raw_stats(self, es_result_agg: dict, group_by_list: list=None):
        """
        {
            "by_provider": {
                "doc_count_error_upper_bound": 0,
                "sum_other_doc_count": 0,
                "buckets": [
                    {
                        "key": "Florida State University, COAPS",
                        "doc_count": 4724,
                        "by_project": {
                            "doc_count_error_upper_bound": 0,
                            "sum_other_doc_count": 0,
                            "buckets": [
                                {
                                    "key": "SAMOS",
                                    "doc_count": 4724,
                                    "by_platform_code": {
                                        "doc_count_error_upper_bound": 0,
                                        "sum_other_doc_count": 0,
                                        "buckets": [
                                            {
                                                "key": "30",
                                                "doc_count": 4724,
                                                "min_lon": {
                                                    "value": 179.9308
                                                },
                                                "max_lat": {
                                                    "value": 80.5424
                                                },
                                                "max_datetime": {
                                                    "value": 1546300740
                                                },
                                                "max_lon": {
                                                    "value": 179.9996
                                                },
                                                "min_datetime": {
                                                    "value": 1546214460
                                                },
                                                "max_depth": {
                                                    "value": 6
                                                },
                                                "totals": {
                                                    "value": 14530387
                                                },
                                                "min_lat": {
                                                    "value": 80.5317
                                                },
                                                "min_depth": {
                                                    "value": 4
                                                }
                                            }
                                        ]
                                    }
                                }
                            ]
                        }
                    }
                ]
            }
        }
        :param es_result_agg:
        :param group_by_list:
        :return:
        """
        if group_by_list is None:
            stats_instructions = self.__file_struct_setting.get_query_statistics_instructions()
            group_by_list = stats_instructions['group_by']
        if len(group_by_list) < 1:
            stats_instructions = self.__file_struct_setting.get_query_statistics_instructions()
            stat_result = {}
            transformers = self.__file_struct_setting.get_query_statistics_instructions()['transformers']
            for agg_type, columns in stats_instructions['stats'].items():
                for each_column in columns:
                    stat_result[each_column] = es_result_agg[each_column]['value'] if each_column not in transformers else self.__transform_result(transformers[each_column], es_result_agg[each_column]['value'])
                    # TODO: need backward compatibility. (punted on 2023-03-26. This is the result of statistics which other applications may depend, but new format is more standardized.)
            data_stats_instructions = self.__file_struct_setting.get_query_statistics_instructions()['data_stats']
            if data_stats_instructions['is_included']:
                data_stats = {}
                for each_data_column in self.__data_column_names:
                    data_stats[each_data_column] = es_result_agg[each_data_column]['value']
                stat_result['data_stats'] = data_stats
            return stat_result
        next_group_by = group_by_list[1:]
        agg_result = []
        for each_agg in es_result_agg[group_by_list[0]]['buckets']:
            current_key = each_agg['key']
            current_results = self.__retrieve_raw_stats(each_agg, next_group_by)
            agg_result.append({
                group_by_list[0]: current_key,
                f'{group_by_list[0]}_stats': current_results
            })
        return agg_result

    def __get_data_stats(self):
        data_stats = {}
        data_stats_instructions = self.__file_struct_setting.get_query_statistics_instructions()['data_stats']
        if not data_stats_instructions['is_included']:
            return data_stats
        data_columns_prefix = data_stats_instructions['data_prefix'] if 'data_prefix' in data_stats_instructions else ''
        for each_data_column in self.__data_column_names:
            data_stats[each_data_column] = {
                data_stats_instructions['stats']: {'field': f'{data_columns_prefix}{each_data_column}'}
            }
        return data_stats

    def generate_dsl(self):
        query_transformer = GetQueryTransformer(self.__file_struct_setting)
        es_terms = query_transformer.generate_dsl_conditions(self.__query_dict)

        normal_stats = {}
        stats_instructions = self.__file_struct_setting.get_query_statistics_instructions()
        for agg_type, columns in stats_instructions['stats'].items():
            for each_column in columns:
                normal_stats[each_column] = {agg_type: {"field": each_column}}
        aggregations_group_by = {}
        current_agg_pointer = aggregations_group_by
        for each_agg in stats_instructions['group_by']:
            current_agg_pointer['aggs'] = {
                each_agg: {'terms': {'field': each_agg}}
            }
            current_agg_pointer = current_agg_pointer['aggs'][each_agg]
        current_agg_pointer['aggs'] = {
            **normal_stats,
            **self.__get_data_stats(),
        }
        stats_dsl = {
            **{
                "size": 0,
                "query": {
                    'bool': {
                        'must': es_terms
                    }
                }
            },
            **aggregations_group_by
        }
        return stats_dsl
    def start(self):
        stats_dsl = self.generate_dsl()
        LOGGER.debug(f'es_dsl: {json.dumps(stats_dsl)}')
        es_result = self.__es.query(stats_dsl, CDMSConstants.es_index_parquet_stats)
        # statistics = {k: v['value'] for k, v in es_result['aggregations'].items()}
        return self.__retrieve_raw_stats(es_result['aggregations'])
