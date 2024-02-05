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

import logging
import os
from io import BytesIO

from parquet_flask.aws.aws_cred import AwsCred
from parquet_flask.utils.file_utils import FileUtils

LOGGER = logging.getLogger(__name__)


class AwsS3(AwsCred):
    def __init__(self):
        super().__init__()
        self.__valid_s3_schemas = ['s3://', 's3a://', 's3s://']
        self.__s3_client = self.get_client('s3')
        self.__target_bucket = None
        self.__target_key = None

    def get_s3_stream(self):
        return self.__s3_client.get_object(Bucket=self.__target_bucket, Key=self.__target_key)['Body']

    def read_small_txt_file(self):
        """
        convenient method to read small text files stored in S3

        :param bucket: bucket name
        :param key: S3 key
        :return: text file contents
        """
        bytestream = BytesIO(self.get_s3_stream().read())  # get the bytes stream of zipped file
        return bytestream.read().decode('UTF-8')

    def get_s3_obj_size(self):
        # get head of the s3 file
        s3_obj_head = self.__s3_client.head_object(
            Bucket=self.__target_bucket,
            Key=self.__target_key,
        )
        # get the object size
        s3_obj_size = int(s3_obj_head['ResponseMetadata']['HTTPHeaders']['content-length'])
        if s3_obj_size is None:  # no object size found. something went wrong.
            return -1
        return s3_obj_size

    def __get_all_s3_files_under(self, bucket, prefix, with_versions=False):
        list_method_name = 'list_object_versions' if with_versions is True else 'list_objects_v2'
        page_key = 'Versions' if with_versions is True else 'Contents'
        paginator = self.__s3_client.get_paginator(list_method_name)
        operation_parameters = {
            'Bucket': bucket,
            'Prefix': prefix
        }
        page_iterator = paginator.paginate(**operation_parameters)
        for eachPage in page_iterator:
            if page_key not in eachPage:
                continue
            for fileObj in eachPage[page_key]:
                yield fileObj

    def get_child_s3_files(self, bucket, prefix, additional_checks=lambda x: True, with_versions=False):
        for fileObj in self.__get_all_s3_files_under(bucket, prefix, with_versions=with_versions):
            if additional_checks(fileObj):
                yield fileObj['Key'], fileObj['Size']

    def set_s3_url(self, s3_url):
        LOGGER.debug(f'setting s3_url: {s3_url}')
        self.__target_bucket, self.__target_key = self.split_s3_url(s3_url)
        LOGGER.debug(f'props: {self.__target_bucket}, {self.__target_key}')
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
            LOGGER.debug(f'setting the downloading filename from target_key: {self.__target_key}')
            file_name = os.path.basename(self.__target_key)
        local_file_path = os.path.join(local_dir, file_name)
        LOGGER.debug(f'downloading to local_file_path: {local_file_path}')
        self.__s3_client.download_file(self.__target_bucket, self.__target_key, local_file_path)
        LOGGER.debug(f'file downloaded')
        return local_file_path

    def delete_keys(self, bucket, keys):
        batches = [keys[i:i + 1000] for i in range(0, len(keys), 1000)]

        for batch in batches:
            LOGGER.debug(f'Deleting {len(batch):,} objects')
            resp = self.__s3_client.delete_objects(Bucket=bucket, Delete=dict(Objects=batch))

            if len(resp['Deleted']) != len(batch):
                LOGGER.error(f'{len(resp["Errors"]):,} objects could not be deleted')

                retries = 3

                while len(resp["Errors"]) > 0 and retries > 0:
                    LOGGER.debug(f'Retrying {len(resp["Errors"])} objects')
                    resp = self.__s3_client.delete_objects(
                        Bucket=bucket,
                        Delete=dict(Objects=[dict(Key=e['Key']) for e in resp['Errors']])
                    )
