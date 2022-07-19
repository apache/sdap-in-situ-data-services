import os

from parquet_flask.aws.aws_s3 import AwsS3
from parquet_flask.aws.es_abstract import ESAbstract

from parquet_flask.aws.es_factory import ESFactory
from parquet_flask.cdms_lambda_func.cdms_lambda_constants import CdmsLambdaConstants
from parquet_flask.cdms_lambda_func.index_to_es.parquet_stat_extractor import ParquetStatExtractor
from parquet_flask.cdms_lambda_func.index_to_es.s3_stat_extractor import S3StatExtractor
from parquet_flask.cdms_lambda_func.s3_records.s3_2_sqs import S3ToSqs


class ParquetFileEsIndexer:
    def __init__(self):
        self.__s3_url = None
        self.__es_url = os.environ.get(CdmsLambdaConstants.es_url, None)
        self.__es_index = os.environ.get(CdmsLambdaConstants.es_index, None)
        self.__es_port = int(os.environ.get(CdmsLambdaConstants.es_port, '443'))
        if any([k is None for k in [self.__es_url, self.__es_index]]):
            raise ValueError(f'invalid env. must have {[CdmsLambdaConstants.es_url, CdmsLambdaConstants.es_index]}')
        self.__es: ESAbstract = ESFactory().get_instance('AWS', index=self.__es_index, base_url=self.__es_url, port=self.__es_port)

    def ingest_file(self):
        if self.__s3_url is None:
            raise ValueError('s3 url is null. Set it first')
        s3_stat = S3StatExtractor(self.__s3_url).start()
        s3_bucket, s3_key = AwsS3().split_s3_url(self.__s3_url)
        parquet_stat = ParquetStatExtractor().start(s3_key)
        self.__es.index_one({'s3_url': self.__s3_url, **s3_stat.to_json(), **parquet_stat}, s3_stat.s3_url)
        return

    def remove_file(self):
        if self.__s3_url is None:
            raise ValueError('s3 url is null. Set it first')
        self.__es.delete_by_id(self.__s3_url)
        return

    def start(self, event):
        s3_record = S3ToSqs(event)
        self.__s3_url = s3_record.get_s3_url()
        s3_event = s3_record.get_event_name().strip().lower()
        if s3_event.startswith('objectcreated'):
            self.ingest_file()
        elif s3_event.startswith('objectremoved'):
            self.remove_file()
        else:
            raise ValueError(f'invalid s3_event: {s3_event}')
        return
