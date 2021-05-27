import os

import boto3

from parquet_flask.utils.config import Config
from parquet_flask.utils.file_utils import FileUtils


class AwsS3:
    def __init__(self):
        self.__valid_s3_schemas = ['s3://', 's3a://', 's3s://']
        self.__s3_client = boto3.Session(aws_access_key_id=Config().get_value('aws_access_key_id'),
                                         aws_secret_access_key=Config().get_value('aws_secret_access_key'),
                                         aws_session_token=Config().get_value('aws_session_token'),
                                         region_name='us-west-2').client('s3')

    def split_s3_url(self, s3_url):
        s3_schema = [k for k in self.__valid_s3_schemas if s3_url.startswith(k)]
        if len(s3_schema) != 1:
            raise ValueError('invalid s3 url: {}'.format(s3_url))

        s3_schema_length = len(s3_schema[0])
        split_index = s3_url[s3_schema_length:].find('/')
        bucket = s3_url[s3_schema_length: split_index+s3_schema_length]
        key = s3_url[(split_index + s3_schema_length + 1):]
        return bucket, key

    def download(self, s3_url, local_dir, file_name=None):
        if not FileUtils.dir_exist(local_dir):
            raise ValueError('missing directory')
        bucket, key = self.split_s3_url(s3_url)
        if file_name is None:
            file_name = os.path.basename(s3_url)
        local_file_path = os.path.join(local_dir, file_name)
        self.__s3_client.download_file(bucket, key, local_file_path)
        return local_file_path
