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
        self.__target_bucket = None
        self.__target_key = None

    def set_s3_url(self, s3_url):
        self.__target_bucket, self.__target_key = self.split_s3_url(s3_url)
        return self

    def split_s3_url(self, s3_url):
        s3_schema = [k for k in self.__valid_s3_schemas if s3_url.startswith(k)]
        if len(s3_schema) != 1:
            raise ValueError('invalid s3 url: {}'.format(s3_url))

        s3_schema_length = len(s3_schema[0])
        split_index = s3_url[s3_schema_length:].find('/')
        bucket = s3_url[s3_schema_length: split_index+s3_schema_length]
        key = s3_url[(split_index + s3_schema_length + 1):]
        return bucket, key

    def __tag_existing_obj(self, other_tags={}):
        if len(other_tags) == 0:
            return
        tags = {
            'TagSet': []
        }
        for key, val in other_tags.items():
            tags['TagSet'].append({
                'Key': key,
                'Value': str(val)
            })
        self.__s3_client.put_object_tagging(Bucket=self.__target_bucket, Key=self.__target_key, Tagging=tags)
        return

    def add_tags_to_obj(self, other_tags={}):
        """
        retrieve existing tags first and append new tags to them

        :param bucket: string
        :param s3_key: string
        :param other_tags: dict
        :return: bool
        """
        if len(other_tags) == 0:
            return False
        response = self.__s3_client.get_object_tagging(Bucket=self.__target_bucket, Key=self.__target_key)
        if 'TagSet' not in response:
            return False
        all_tags = {k['Key']: k['Value'] for k in response['TagSet']}
        for k, v in other_tags.items():
            all_tags[k] = v
            pass
        self.__tag_existing_obj(all_tags)
        return True

    def download(self, local_dir, file_name=None):
        if not FileUtils.dir_exist(local_dir):
            raise ValueError('missing directory')
        if file_name is None:
            file_name = os.path.basename(self.__target_key)
        local_file_path = os.path.join(local_dir, file_name)
        self.__s3_client.download_file(self.__target_bucket, self.__target_key, local_file_path)
        return local_file_path
