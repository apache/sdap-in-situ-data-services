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
import uuid
from multiprocessing.context import Process

from parquet_flask.aws.es_abstract import ESAbstract
from parquet_flask.aws.es_factory import ESFactory
from parquet_flask.io_logic.metadata_tbl_es import MetadataTblES

from parquet_flask.aws.aws_s3 import AwsS3
from parquet_flask.io_logic.cdms_constants import CDMSConstants
from parquet_flask.io_logic.ingest_new_file import IngestNewJsonFile
from parquet_flask.io_logic.metadata_tbl_interface import MetadataTblInterface
from parquet_flask.utils.config import Config
from parquet_flask.utils.file_utils import FileUtils
from parquet_flask.utils.time_utils import TimeUtils

LOGGER = logging.getLogger(__name__)


class IngestAwsJsonProps:
    def __init__(self):
        self.__s3_url = None
        self.__s3_sha_url = None
        self.__uuid = str(uuid.uuid4())
        self.__working_dir = f'/tmp/{str(uuid.uuid4())}'
        self.__is_replacing = False
        self.__is_sanitizing = True
        self.__wait_till_complete = True
        self.__platform_id_key = 'rivid'
        self.__observation_key = 'Qout'
        self.__lat_key = 'lat'
        self.__lon_key = 'lon'
        self.__time_key = 'time'
        self.__chunk_size = 1200
        self.__provider = ''
        self.__project = ''

    @property
    def platform_id_key(self):
        return self.__platform_id_key

    @platform_id_key.setter
    def platform_id_key(self, val):
        """
        :param val:
        :return: None
        """
        self.__platform_id_key = val
        return

    @property
    def observation_key(self):
        return self.__observation_key

    @observation_key.setter
    def observation_key(self, val):
        """
        :param val:
        :return: None
        """
        self.__observation_key = val
        return

    @property
    def lat_key(self):
        return self.__lat_key

    @lat_key.setter
    def lat_key(self, val):
        """
        :param val:
        :return: None
        """
        self.__lat_key = val
        return

    @property
    def lon_key(self):
        return self.__lon_key

    @lon_key.setter
    def lon_key(self, val):
        """
        :param val:
        :return: None
        """
        self.__lon_key = val
        return

    @property
    def time_key(self):
        return self.__time_key

    @time_key.setter
    def time_key(self, val):
        """
        :param val:
        :return: None
        """
        self.__time_key = val
        return

    @property
    def chunk_size(self):
        return self.__chunk_size

    @chunk_size.setter
    def chunk_size(self, val):
        """
        :param val:
        :return: None
        """
        self.__chunk_size = val
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
    def wait_till_complete(self):
        return self.__wait_till_complete

    @wait_till_complete.setter
    def wait_till_complete(self, val):
        """
        :param val:
        :return: None
        """
        self.__wait_till_complete = val
        return

    @property
    def is_sanitizing(self):
        return self.__is_sanitizing

    @is_sanitizing.setter
    def is_sanitizing(self, val):
        """
        :param val:
        :return: None
        """
        self.__is_sanitizing = val
        return

    @property
    def s3_sha_url(self):
        return self.__s3_sha_url

    @s3_sha_url.setter
    def s3_sha_url(self, val):
        """
        :param val:
        :return: None
        """
        self.__s3_sha_url = val
        return

    @property
    def is_replacing(self):
        return self.__is_replacing

    @is_replacing.setter
    def is_replacing(self, val):
        """
        :param val:
        :return: None
        """
        self.__is_replacing = val
        return

    @property
    def working_dir(self):
        return self.__working_dir

    @working_dir.setter
    def working_dir(self, val):
        """
        :param val:
        :return: None
        """
        self.__working_dir = val
        return

    @property
    def s3_url(self):
        return self.__s3_url

    @s3_url.setter
    def s3_url(self, val):
        """
        :param val:
        :return: None
        """
        self.__s3_url = val
        return

    @property
    def uuid(self):
        return self.__uuid

    @uuid.setter
    def uuid(self, val):
        """
        :param val:
        :return: None
        """
        self.__uuid = val
        return
