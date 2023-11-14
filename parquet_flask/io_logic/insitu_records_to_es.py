import json
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


class InsituRecordsToEs:
    def __init__(self, es_url):
        self.__s3 = AwsS3()
        self.__es: ESAbstract = ESFactory().get_instance('AWS', index=CDMSConstants.insitu_records_index_alias, base_url=es_url, port=443)

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
