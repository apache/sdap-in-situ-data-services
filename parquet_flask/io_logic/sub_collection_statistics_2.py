import json
import logging

from parquet_flask.insitu.file_structure_setting import FileStructureSetting
from parquet_flask.insitu.get_query_transformer import GetQueryTransformer
from parquet_flask.io_logic.cdms_schema import CdmsSchema
from parquet_flask.io_logic.query_v2 import QueryProps
from parquet_flask.utils.file_utils import FileUtils

from parquet_flask.io_logic.cdms_constants import CDMSConstants
from parquet_flask.utils.config import Config

from parquet_flask.aws.es_factory import ESFactory

from parquet_flask.aws.es_abstract import ESAbstract
from parquet_flask.utils.time_utils import TimeUtils

LOGGER = logging.getLogger(__name__)


class SubCollectionStatistics2:
    def __init__(self, es_mw: ESAbstract, in_situ_schema_file_path: str, in_situ_file_structure_config_file_path: str, query_object: dict):
        self.__es = es_mw
        self.__file_structure_setting = FileStructureSetting({}, FileUtils.read_json(in_situ_file_structure_config_file_path))
        self.__query_object = query_object

        self.__insitu_schema = FileUtils.read_json(in_situ_schema_file_path)
        self.__cdms_obs_names = CdmsSchema().get_observation_names(self.__insitu_schema)

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

    def __get_observation_agg_stmts(self):
        agg_stmts = {k: {
            'sum': {
                'field': f'observation_counts.{k}'
            }
        } for k in self.__cdms_obs_names}
        return agg_stmts

    def start(self):
        query_transformer = GetQueryTransformer(self.__file_structure_setting)
        self.__query_object = query_transformer.transform_param(self.__query_object)
        es_terms = query_transformer.generate_dsl_conditions(self.__query_object)

        normal_agg_stmts = {
            "totals": {
                "sum": {"field": "total"}}
            ,
            "max_datetime": {
                "max": {
                    "field": CDMSConstants.max_datetime
                }
            },
            "max_depth": {
                "max": {
                    "field": CDMSConstants.max_depth
                }
            },
            "max_lat": {
                "max": {
                    "field": CDMSConstants.max_lat
                }
            },
            "max_lon": {
                "max": {
                    "field": CDMSConstants.max_lon
                }
            },
            "min_datetime": {
                "min": {
                    "field": CDMSConstants.min_datetime
                }
            },
            "min_depth": {
                "min": {
                    "field": CDMSConstants.min_depth
                }
            },
            "min_lat": {
                "min": {
                    "field": CDMSConstants.min_lat
                }
            },
            "min_lon": {
                "min": {
                    "field": CDMSConstants.min_lon
                }
            }
        }
        stats_dsl = {
            "size": 0,
            "query": {
                'bool': {
                    'must': es_terms
                }
            },
            "aggs": {
                "by_provider": {
                    "terms": {
                        "field": CDMSConstants.provider_col
                    },
                    "aggs": {
                        "by_project": {
                            "terms": {"field": CDMSConstants.project_col},
                            "aggs": {
                                "by_platform_code": {
                                    "terms": {"field": CDMSConstants.platform_code_col},
                                    "aggs": {**normal_agg_stmts, **self.__get_observation_agg_stmts()}
                                }
                            }
                        }
                    }
                }
            }
        }
        LOGGER.warning(f'es_dsl: {json.dumps(stats_dsl)}')
        es_result = self.__es.query(stats_dsl, CDMSConstants.es_index_parquet_stats)
        # statistics = {k: v['value'] for k, v in es_result['aggregations'].items()}
        return self.__restructure_stats(es_result['aggregations'])
