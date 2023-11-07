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

from parquet_flask.io_logic.cdms_schema import CdmsSchema
from parquet_flask.io_logic.query_v2 import QueryProps
from parquet_flask.utils.cql_parser import CqlParser
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
            "platform_short_name": core_stats['platform_short_name']['buckets'][0]['key'],
            "total": core_stats['totals']['value'],
            "lat": core_stats['max_lat']['value'],
            "lon": core_stats['max_lon']['value'],
            "min_datetime": TimeUtils.get_time_str(int(core_stats['min_datetime']['value']), in_ms=False),
            "max_datetime": TimeUtils.get_time_str(int(core_stats['max_datetime']['value']), in_ms=False),
            CDMSConstants.observation_counts: {k: core_stats[k]['value'] for k in self.__cdms_obs_names},
            'units': {k: self.__insitu_schema['definitions']['observation']['properties'][k]['units'] for k in self.__cdms_obs_names}
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
                                self.__restructure_core_stats(k) for k in l['by_platform_id']['buckets']
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
                'field': f'{CDMSConstants.observation_counts}.{k}'
            },
        } for k in self.__cdms_obs_names}
        return agg_stmts

    def start(self):
        es_terms = []

        # Provider and project
        if self.__query_props.provider is not None:
            es_terms.append({'term': {CDMSConstants.provider_col: self.__query_props.provider}})
        if self.__query_props.project is not None:
            es_terms.append({'term': {CDMSConstants.project_col: self.__query_props.project}})

        # Platforms
        if self.__query_props.platform_id is not None:
            if isinstance(self.__query_props.platform_id, list):
                es_terms.append({
                    'bool': {
                        'should': [
                            {'term': {CDMSConstants.platform_id_col: k}} for k in self.__query_props.platform_id
                        ]
                    }
                })
            else:
                es_terms.append({'term': {CDMSConstants.platform_id_col: self.__query_props.platform_id}})

        if self.__query_props.variable is not None and len(self.__query_props.variable) > 0:
            es_terms.append({
                'bool': {
                    'must': [{
                        'range': {
                            f'{CDMSConstants.observation_counts}.{k}': {
                                'gte': 1,
                            } for k in self.__query_props.variable
                        }
                    }]
                }
            })
        if self.__query_props.filter_cql is not None:
            LOGGER.debug(f'it has some additional filter. {self.__query_props.filter_cql}')
            cql_to_dsl = CqlParser(CDMSConstants.observation_min_max).transform(self.__query_props.filter_cql)
            LOGGER.debug(f'cql_to_dsl = {cql_to_dsl}')
            es_terms.append(cql_to_dsl)
        # Time range
        if self.__query_props.min_datetime is not None and self.__query_props.max_datetime is not None:
            es_terms.append({'range': {CDMSConstants.max_datetime: {'gte': self.__query_props.min_datetime}}})
            es_terms.append({'range': {CDMSConstants.min_datetime: {'lte': self.__query_props.max_datetime}}})

        # Bounding box (lat lon range)
        if self.__query_props.min_lat_lon is not None and self.__query_props.max_lat_lon is not None:
            # Lat range
            es_terms.append({'range': {CDMSConstants.max_lat: {'gte': self.__query_props.min_lat_lon[0]}}})
            es_terms.append({'range': {CDMSConstants.min_lat: {'lte': self.__query_props.max_lat_lon[0]}}})
            # Lon range
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
            "min_lat": {
                "min": {
                    "field": CDMSConstants.min_lat
                }
            },
            "min_lon": {
                "min": {
                    "field": CDMSConstants.min_lon
                }
            },
            "platform_short_name": {
                "terms": {
                    "field": CDMSConstants.platform_short_name_col,
                    "size": 1
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
                        "field": CDMSConstants.provider_col,
                        "size": 100
                    },
                    "aggs": {
                        "by_project": {
                            "terms": {
                                "field": CDMSConstants.project_col,
                                "size": 100
                            },
                            "aggs": {
                                "by_platform_id": {
                                    "terms": {
                                        "field": CDMSConstants.platform_id_col,
                                        "size": 10000
                                    },
                                    "aggs": {
                                        **normal_agg_stmts,
                                        **self.__get_observation_agg_stmts()
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
