import json
import logging

from parquet_flask.io_logic.cdms_constants import CDMSConstants
from parquet_flask.utils.config import Config

from parquet_flask.aws.es_factory import ESFactory

from parquet_flask.aws.es_abstract import ESAbstract
from parquet_flask.utils.time_utils import TimeUtils

LOGGER = logging.getLogger(__name__)


class SubCollectionStatistics:
    def __init__(self):
        config = Config()
        self.__es: ESAbstract = ESFactory().get_instance('AWS',
                                                         index=CDMSConstants.es_index_parquet_stats,
                                                         base_url=config.get_value(Config.es_url),
                                                         port=int(config.get_value(Config.es_port, '443')))
        self.__provider = None
        self.__project = None
        self.__platform_codes = []

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

    def start(self):
        es_terms = []
        if self.__provider is not None:
            es_terms.append({'term': {CDMSConstants.provider_col: self.__provider}})
        if self.__project is not None:
            es_terms.append({'term': {CDMSConstants.project_col: self.__project}})
        if self.__platform_codes is not None:
            if isinstance(self.__platform_codes, list):
                es_terms.append({
                    'bool': {
                        'should': [
                            {'term': {CDMSConstants.platform_code_col: k}} for k in self.__platform_codes
                        ]
                    }
                })
            else:
                es_terms.append({'term': {CDMSConstants.platform_code_col: self.__platform_codes}})

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
                                    "aggs": {
                                        "totals": {
                                            "sum": {"field": "total"}}
                                        ,
                                        "max_datetime": {
                                            "max": {
                                                "field": "max_datetime"
                                            }
                                        },
                                        "max_depth": {
                                            "max": {
                                                "field": "max_depth"
                                            }
                                        },
                                        "max_lat": {
                                            "max": {
                                                "field": "max_lat"
                                            }
                                        },
                                        "max_lon": {
                                            "max": {
                                                "field": "max_lon"
                                            }
                                        },
                                        "min_datetime": {
                                            "min": {
                                                "field": "min_datetime"
                                            }
                                        },
                                        "min_depth": {
                                            "min": {
                                                "field": "min_depth"
                                            }
                                        },
                                        "min_lat": {
                                            "min": {
                                                "field": "min_lat"
                                            }
                                        },
                                        "min_lon": {
                                            "min": {
                                                "field": "min_lon"
                                            }
                                        }
                                    }
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
