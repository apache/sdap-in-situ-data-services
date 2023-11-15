import json
import logging
from tempfile import TemporaryDirectory

import pandas
from pandas import DataFrame
from pandas.io.json import json_normalize

from parquet_flask.aws.aws_s3 import AwsS3
from parquet_flask.aws.es_abstract import ESAbstract
from parquet_flask.aws.es_factory import ESFactory
from parquet_flask.io_logic.cdms_constants import CDMSConstants
from parquet_flask.utils.file_utils import FileUtils
from parquet_flask.utils.general_utils import GeneralUtils
LOGGER = logging.getLogger(__name__)


class InsituQueryProps:
    def __init__(self):
        self.__provider = None
        self.__project = None
        self.__timestamp = None
        self.__size = 1000
        self.__min_lat_lon = None
        self.__max_lat_lon = None
        self.__bbox = None
        self.__variable = []
        self.__columns = []
        self.__marker = []

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
    def timestamp(self):
        return self.__timestamp

    @timestamp.setter
    def timestamp(self, val):
        """
        :param val:
        :return: None
        """
        self.__timestamp = val
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
    def variable(self):
        return self.__variable

    @variable.setter
    def variable(self, val):
        """
        :param val:
        :return: None
        """
        self.__variable = val
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

    @property
    def marker(self):
        return self.__marker

    @marker.setter
    def marker(self, val):
        """
        :param val:
        :return: None
        """
        self.__marker = val
        return


class InsituRecordsToEs:
    def __init__(self, es_url):
        self.__s3 = AwsS3()
        self.__es: ESAbstract = ESFactory().get_instance('AWS', index=CDMSConstants.insitu_records_index_alias, base_url=es_url, port=443)

    def query(self, query_props: InsituQueryProps):
        if any([k is None for k in [query_props.timestamp, query_props.project, query_props.provider]]):
            raise ValueError(f'missing timestamp or project or provider')
        query_dsl = {
            'sort': [{'platform.id': {'order': 'asc'}}],
            'size': query_props.size,
            'query': {
                'bool': {
                    'must': [
                        {'term': {'time': {'value': query_props.timestamp}}},
                        {'term': {'provider': {'value': query_props.provider}}},
                        {'term': {'project': {'value': query_props.project}}},
                    ]
                }
            }
        }
        if len(query_props.variable) > 0:
            query_dsl['query']['bool']['must'].append({'bool': {'should': [{'exists': {'field': k}} for k in query_props.variable]}})
        if len(query_props.marker) > 0:
            query_dsl['search_after'] = query_props.marker
        LOGGER.debug(f'query_dsl: {query_dsl}')
        print(json.dumps(query_dsl, indent=2))
        es_results = self.__es.query(query_dsl)
        records = [k['_source'] for k in es_results['hits']['hits']]
        pagination_marker = es_results['hits']['hits'][-1]['sort'] if len(records) > 0 else None
        return {
            'hits': records,
            'marker': pagination_marker,
        }

    def ingest(self, s3_url):
        with TemporaryDirectory() as tmp_dir_name:
            local_file_path = self.__s3.set_s3_url(s3_url).download(tmp_dir_name)
            if s3_url.endswith('.gz'):
                local_file_path = FileUtils.gunzip_file_os(local_file_path)
            # local_file_path = '/Users/wphyo/Downloads/2023-10_daily.json'
            insitu_records = FileUtils.read_json(local_file_path)

            panda_records = pandas.json_normalize(insitu_records['observations'], sep='___')
            # panda_records = DataFrame.from_records(insitu_records['observations'])
            panda_records['provider'] = insitu_records['provider']
            panda_records['project'] = insitu_records['project']
            panda_records['platform___short_name'] = panda_records['platform___short_name'].fillna('')
            panda_records = panda_records.assign(platform=lambda x: x.apply(lambda row: {'id': row['platform___id'], 'short_name': row['platform___short_name']}, axis=1))
            # temp = panda_records[panda_records['platform___id'] == '840MMLEM1016']
            panda_records.drop(['platform___id', 'platform___short_name'], axis=1, inplace=True)
            list_of_dicts = panda_records.apply(lambda row: row.dropna().to_dict(), axis=1).tolist()

            for each_chunk in GeneralUtils.chunk_list(list_of_dicts, 20):
                doc_dict = {f'{each_record["platform"]["id"]}__{each_record["time"]}': each_record for each_record in each_chunk}
                self.__es.index_many(doc_dict=doc_dict)
        return self
