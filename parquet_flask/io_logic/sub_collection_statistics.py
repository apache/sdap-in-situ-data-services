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
        self.__cdms_obs_names = CdmsSchema().get_observation_names(self.__insitu_schema, self.__file_struct_setting.get_non_data_columns())

    def __restructure_core_stats(self, core_stats: dict):
        """
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
        :param core_stats:
        :return:
        """
        core_stats = {
            "platform": core_stats['key'],
            "statistics": {
                "total": core_stats['totals']['value'],
                "min_lat_lon": [core_stats['min_lat']['value'], core_stats['min_lon']['value']],
                "max_lat_lon": [core_stats['max_lat']['value'], core_stats['max_lon']['value']],
                "min_depth": core_stats['min_depth']['value'],
                "max_depth": core_stats['max_depth']['value'],
                "min_datetime": TimeUtils.get_time_str(int(core_stats['min_datetime']['value']), in_ms=False),
                "max_datetime": TimeUtils.get_time_str(int(core_stats['max_datetime']['value']), in_ms=False),
                'observation_counts': {k: core_stats[k]['value'] for k in self.__cdms_obs_names}
            }
        }
        LOGGER.debug(f'core_stats: {core_stats}')
        return core_stats

    def __restructure_stats(self, es_result: dict):
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
        :param es_result:
        :return:
        """
        stats_instructions = self.__file_struct_setting.get_query_statistics_instructions()
        stats_result = {}
        current_agg_pointer = stats_result
        for each_agg in stats_instructions['group_by']:
            current_agg_pointer['aggs'] = {
                each_agg: {'terms': {'field': each_agg}}
            }
            current_agg_pointer[each_agg] = [
                {
                    each_agg: k['key'],
                } for k in es_result[each_agg]['buckets']
            ]

        restructured_stats = {
            "providers": [
                {
                    "provider": m['key'],
                    "projects": [
                        {
                            "project": l['key'],
                            "platforms": [
                                self.__restructure_core_stats(k) for k in l['by_platform_code']['buckets']
                            ]
                        } for l in m['by_project']['buckets']
                    ]
                } for m in es_result['by_provider']['buckets']
            ]
        }
        LOGGER.debug(f'restructured_stats: {restructured_stats}')
        return restructured_stats

    def __get_data_stats(self):
        data_stats = {}
        data_stats_instructions = self.__file_struct_setting.get_query_statistics_instructions()['data_stats']
        if not data_stats_instructions['is_included']:
            return data_stats
        data_columns_prefix = data_stats_instructions['data_prefix'] if 'data_prefix' in data_stats_instructions else ''
        for each_data_column in self.__cdms_obs_names:  # TODO need to rename
            data_stats[each_data_column] = {
                data_stats_instructions['stats']: {'field': f'{data_columns_prefix}{each_data_column}'}
            }
        return data_stats

    def generate_dsl(self):
        query_transformer = GetQueryTransformer(self.__file_struct_setting)
        query_object = query_transformer.transform_param(self.__query_dict)
        es_terms = query_transformer.generate_dsl_conditions(query_object)

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
        LOGGER.warning(f'es_dsl: {json.dumps(stats_dsl)}')
        es_result = self.__es.query(stats_dsl, CDMSConstants.es_index_parquet_stats)
        # statistics = {k: v['value'] for k, v in es_result['aggregations'].items()}
        return self.__restructure_stats(es_result['aggregations'])
