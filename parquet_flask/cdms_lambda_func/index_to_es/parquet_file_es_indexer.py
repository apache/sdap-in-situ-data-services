import json
import os

from insitu.file_structure_setting import FileStructureSetting
from parquet_flask.cdms_lambda_func.index_to_es.parquet_file_path_stat_extractor import ParquetFilePathStatExtractor
from parquet_flask.parquet_stat_extractor.local_statistics_retriever import LocalStatisticsRetriever
from parquet_flask.utils.file_utils import FileUtils

from parquet_flask.aws.aws_s3 import AwsS3
from parquet_flask.aws.es_abstract import ESAbstract

from parquet_flask.aws.es_factory import ESFactory
from parquet_flask.cdms_lambda_func.cdms_lambda_constants import CdmsLambdaConstants
from parquet_flask.cdms_lambda_func.index_to_es.parquet_stat_extractor import ParquetStatExtractor
from parquet_flask.cdms_lambda_func.lambda_logger_generator import LambdaLoggerGenerator
from parquet_flask.cdms_lambda_func.s3_records.s3_2_sqs import S3ToSqs

LOGGER = LambdaLoggerGenerator.get_logger(__name__, log_level=LambdaLoggerGenerator.get_level_from_env())


class ParquetFileEsIndexer:
    def __init__(self):
        self.__s3_url = None
        self.__es_url = os.environ.get(CdmsLambdaConstants.es_url, None)
        self.__es_index = os.environ.get(CdmsLambdaConstants.es_index, None)
        self.__es_port = int(os.environ.get(CdmsLambdaConstants.es_port, '443'))
        self.__file_structure_json = os.environ.get(CdmsLambdaConstants.file_structure_setting_json, None)  # TODO update setting
        if any([k is None for k in [self.__es_url, self.__es_index, self.__file_structure_json]]):
            raise ValueError(f'invalid env. must have {[CdmsLambdaConstants.es_url, CdmsLambdaConstants.es_index]}')
        self.__es: ESAbstract = ESFactory().get_instance('AWS', index=self.__es_index, base_url=self.__es_url, port=self.__es_port)

    def extract_stats_locally(self):
        LOGGER.debug('downloading parquet file locally to extract stats')
        local_parquet_file_path = AwsS3().set_s3_url(self.__s3_url).download('/tmp')
        stats_json = LocalStatisticsRetriever(local_parquet_file_path,
                                              os.environ.get(CdmsLambdaConstants.insitu_schema_file, '/etc/in_situ_schema.json'),
                                              os.environ.get(CdmsLambdaConstants.file_structure_setting_json, '/etc/insitu.file.structure.config.json')).start()
        LOGGER.debug(f'locally extracted stats: {stats_json}')
        FileUtils.del_file(local_parquet_file_path)
        return stats_json

    def extract_stats_remotely(self):
        LOGGER.debug('calling server to extract stats')
        s3_bucket, s3_key = AwsS3().split_s3_url(self.__s3_url)
        parquet_stat = ParquetStatExtractor().start(s3_key)
        LOGGER.debug(f'server extracted stats: {parquet_stat}')
        return parquet_stat

    def ingest_file(self):
        if self.__s3_url is None:
            raise ValueError('s3 url is null. Set it first')
        file_structure_setting = FileStructureSetting({}, json.loads(self.__file_structure_json))
        stat_extractor = ParquetFilePathStatExtractor(file_structure_setting, self.__s3_url, 'bucket', 'name', 's3_url').start()
        stat_extractor_json = stat_extractor.to_json()
        LOGGER.debug(f'file_path_stat: {stat_extractor_json}')
        parquet_stat = self.extract_stats_locally()
        LOGGER.debug(f'parquet_stat: {parquet_stat}')
        self.__es.index_one({'s3_url': self.__s3_url, **stat_extractor_json, **parquet_stat}, self.__s3_url)
        return

    def remove_file(self):
        if self.__s3_url is None:
            raise ValueError('s3 url is null. Set it first')
        delete_result = self.__es.delete_by_id(self.__s3_url)
        LOGGER.debug(f'deletion result: {delete_result}. id: {self.__s3_url}')
        return

    def start(self, event):
        # LOGGER.warning(self.__es.query({
        #     'size': 10,
        #     'query': {
        #         'match_all': {}
        #     }
        # }))
        s3_records = S3ToSqs(event)
        ignoring_phrases = ['spark-staging', '_temporary']
        for i in range(s3_records.size()):
            self.__s3_url = s3_records.get_s3_url(i)
            if any([k in self.__s3_url for k in ignoring_phrases]):
                LOGGER.debug(f'skipping temp file: {self.__s3_url}')
                return
            LOGGER.debug(f'executing: {self.__s3_url}')
            s3_event = s3_records.get_event_name(i).strip().lower()
            if s3_event.startswith('objectcreated'):
                LOGGER.debug('executing index')
                self.ingest_file()
            elif s3_event.startswith('objectremoved'):
                LOGGER.debug('executing to remove index')
                self.remove_file()
            else:
                raise ValueError(f'invalid s3_event: {s3_event}')
        return
