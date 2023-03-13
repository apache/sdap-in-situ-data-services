import logging
from collections import defaultdict

from parquet_flask.aws.es_factory import ESFactory

from parquet_flask.aws.es_abstract import ESAbstract
from parquet_flask.io_logic.cdms_constants import CDMSConstants
from parquet_flask.io_logic.partitioned_parquet_path import PartitionedParquetPath
from parquet_flask.io_logic.query_v2 import QueryProps
from parquet_flask.utils.time_utils import TimeUtils

LOGGER = logging.getLogger(__name__)


class ParquetPathsEsRetriever:
    def __init__(self, base_path: str, props=QueryProps()):
        self.__base_path = base_path
        self.__props = props
        self.__es: ESAbstract = None

    def load_es_obj(self, es: ESAbstract):
        self.__es = es
        return self

    def load_es_from_config(self, es_url: str, es_index: str, es_port: int):
        self.__es: ESAbstract = ESFactory().get_instance('AWS', index=es_index, base_url=es_url, port=es_port)
        return self

    def __step_1(self, es_results: [PartitionedParquetPath]):
        base_map = defaultdict(list)
        for each in es_results:
            each: PartitionedParquetPath = each
            base_key = f'{each.provider}_{each.project}_{each.platform}_{each.lat_lon}'
            base_map[base_key].append(each)
        raise NotImplementedError(f'this will be an enhancement to reduce lots of tiny paths')

    def start(self):
        """
{
  "_index": "parquet_stats_v1",
  "_type": "_doc",
  "_id": "part-00000-9cfcbe81-3ca9-4084-9b8c-db451bd8c076.c000.gz.parquet",
  "_score": 1,
  "_source": {
    "s3_url": "s3://cdms-dev-in-situ-parquet/CDMS_insitu.geo2.parquet/provider=Florida State University, COAPS/project=SAMOS/platform_code=30/geo_spatial_interval=-25_150/year=2017/month=6/job_id=6f33d0e5-65ca-4281-b4df-2d703adee683/part-00000-9cfcbe81-3ca9-4084-9b8c-db451bd8c076.c000.gz.parquet",
    "bucket": "cdms-dev-in-situ-parquet",
    "name": "part-00000-9cfcbe81-3ca9-4084-9b8c-db451bd8c076.c000.gz.parquet",
    "provider": "Florida State University, COAPS",
    "project": "SAMOS",
    "platform_code": "30",
    "geo_spatial_interval": "-25_150",
    "year": "2017",
    "month": "6",
    "total": 8532,
    "min_datetime": 1497312000,
    "max_datetime": 1497398340,
    "min_depth": -31.5,
    "max_depth": 5.9,
    "min_lat": -23.8257,
    "max_lat": -23.6201,
    "min_lon": 154.4868,
    "max_lon": 154.6771
  }
}        :return:
        """
        if self.__es is None:
            raise ValueError(f'ES Object is not loaded')
        es_terms = []
        if self.__props.provider is not None:
            es_terms.append({'term': {CDMSConstants.provider_col: self.__props.provider}})
        if self.__props.project is not None:
            es_terms.append({'term': {CDMSConstants.project_col: self.__props.project}})
        if self.__props.platform_code is not None:
            if isinstance(self.__props.platform_code, list):
                es_terms.append({
                    'bool': {
                        'should': [
                            {'term': {CDMSConstants.platform_code_col: k}} for k in self.__props.platform_code
                        ]
                    }
                })
            else:
                es_terms.append({'term': {CDMSConstants.platform_code_col: self.__props.platform_code}})
        if self.__props.min_datetime is not None:
            es_terms.append({'range': {'max_datetime': {'gte': TimeUtils.get_datetime_obj(self.__props.min_datetime).timestamp()}}})
        if self.__props.max_datetime is not None:
            es_terms.append({'range': {'min_datetime': {'lte': TimeUtils.get_datetime_obj(self.__props.max_datetime).timestamp()}}})
        if self.__props.min_lat_lon is not None:
            es_terms.append({'range': {'max_lat': {'gte': self.__props.min_lat_lon[0]}}})
            es_terms.append({'range': {'max_lon': {'gte': self.__props.min_lat_lon[1]}}})
        if self.__props.max_lat_lon is not None:
            es_terms.append({'range': {'min_lat': {'lte': self.__props.max_lat_lon[0]}}})
            es_terms.append({'range': {'min_lon': {'lte': self.__props.max_lat_lon[1]}}})

        es_dsl = {
            'query': {
                'bool': {
                    'must': es_terms
                }
            },
            'sort': [
                {'min_datetime': {'order': 'asc'}},
                {CDMSConstants.platform_code_col: {'order': 'asc'}},
                {'min_lat': {'order': 'asc'}},
                {'min_lon': {'order': 'asc'}},
                {'s3_url': {'order': 'asc'}},
            ]
        }
        import json
        LOGGER.warning(f'es_dsl: {json.dumps(es_dsl)}')
        #         self.__sorting_columns = [CDMSConstants.time_col, CDMSConstants.platform_code_col, CDMSConstants.depth_col, CDMSConstants.lat_col, CDMSConstants.lon_col]
        result = self.__es.query_pages(es_dsl)
        result = [PartitionedParquetPath(self.__base_path).load_from_es(k['_source']) for k in result['items']]
        return result
