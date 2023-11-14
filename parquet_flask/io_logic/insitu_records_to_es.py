import json
from tempfile import TemporaryDirectory

import pandas

from parquet_flask.aws.aws_s3 import AwsS3
from parquet_flask.aws.es_abstract import ESAbstract
from parquet_flask.aws.es_factory import ESFactory
from parquet_flask.io_logic.cdms_constants import CDMSConstants
from parquet_flask.utils.file_utils import FileUtils
from parquet_flask.utils.general_utils import GeneralUtils


class InsituRecordsToEs:
    def __init__(self, s3_url, es_url):
        self.__s3 = AwsS3()
        self.__s3_url = s3_url
        self.__es: ESAbstract = ESFactory().get_instance('AWS', index=CDMSConstants.insitu_records_index_alias, base_url=es_url, port=443)

    def start(self):
        with TemporaryDirectory() as tmp_dir_name:
            local_file_path = self.__s3.set_s3_url(self.__s3_url).download(tmp_dir_name)
            if self.__s3_url.endswith('.gz'):
                local_file_path = FileUtils.gunzip_file_os(local_file_path)
            insitu_records = FileUtils.read_json(local_file_path)
            # panda_records = pandas.read_json(insitu_records['observations'])
            # panda_records['provider'] = insitu_records['provider']
            # panda_records['project'] = insitu_records['project']
            for each_chunk in GeneralUtils.chunk_list(insitu_records['observations'], 20):
                doc_dict = {}
                for each_record in each_chunk:
                    each_record['provider'] = insitu_records['provider']
                    each_record['project'] = insitu_records['project']
                    doc_dict[f'{each_record["platform"]["id"]}__{each_record["time"]}'] = each_record
                self.__es.index_many(doc_dict=doc_dict)
        return self
