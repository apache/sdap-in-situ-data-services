import json
import logging

from parquet_flask.io_logic.cdms_schema import CdmsSchema
from parquet_flask.io_logic.query_v2 import QueryProps
from parquet_flask.utils.file_utils import FileUtils

from parquet_flask.io_logic.cdms_constants import CDMSConstants
from parquet_flask.utils.config import Config

from parquet_flask.aws.es_factory import ESFactory

from parquet_flask.aws.es_abstract import ESAbstract
from parquet_flask.utils.time_utils import TimeUtils

LOGGER = logging.getLogger(__name__)


class SubCollectionStatistics:
    def __init__(self, query_props: QueryProps):
        config = Config()
        self.__es: ESAbstract = ESFactory().get_instance('AWS',
                                                         index=CDMSConstants.es_index_parquet_stats,
                                                         base_url=config.get_value(Config.es_url),
                                                         port=int(config.get_value(Config.es_port, '443')))
        self.__query_props = query_props
        self.__insitu_schema = FileUtils.read_json(Config().get_value(Config.in_situ_schema))
        self.__cdms_obs_names = CdmsSchema().get_observation_names(self.__insitu_schema)

    def with_provider(self, provider: str):
        self.__provider = provider
        return self

    def with_project(self, project: str):
        self.__project = project
        return self

    def with_platforms(self, platform_code: list):
        self.__platform_codes = platform_code
        return self

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
        es_terms = []
        if self.__query_props.provider is not None:
            es_terms.append({'term': {CDMSConstants.provider_col: self.__query_props.provider}})
        if self.__query_props.project is not None:
            es_terms.append({'term': {CDMSConstants.project_col: self.__query_props.project}})
        if self.__query_props.platform_code is not None:
            if isinstance(self.__query_props.platform_code, list):
                es_terms.append({
                    'bool': {
                        'should': [
                            {'term': {CDMSConstants.platform_code_col: k}} for k in self.__query_props.platform_code
                        ]
                    }
                })
            else:
                es_terms.append({'term': {CDMSConstants.platform_code_col: self.__query_props.platform_code}})
        if self.__query_props.min_depth is not None and self.__query_props.max_depth is not None:
            es_terms.append({'range': {CDMSConstants.max_depth: {'gte': self.__query_props.min_depth}}})
            es_terms.append({'range': {CDMSConstants.min_depth: {'lte': self.__query_props.max_depth}}})

        if self.__query_props.min_datetime is not None and self.__query_props.max_datetime is not None:
            es_terms.append({'range': {CDMSConstants.max_datetime: {'gte': self.__query_props.min_datetime}}})
            es_terms.append({'range': {CDMSConstants.min_datetime: {'lte': self.__query_props.max_datetime}}})

        if self.__query_props.min_lat_lon is not None and self.__query_props.max_lat_lon is not None:
            es_terms.append({'range': {CDMSConstants.max_lat: {'gte': self.__query_props.min_lat_lon[0]}}})
            es_terms.append({'range': {CDMSConstants.min_lat: {'lte': self.__query_props.max_lat_lon[0]}}})

            es_terms.append({'range': {CDMSConstants.max_lon: {'gte': self.__query_props.min_lat_lon[1]}}})
            es_terms.append({'range': {CDMSConstants.min_lon: {'lte': self.__query_props.max_lat_lon[1]}}})

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
