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

import boto3

from parquet_flask.utils.config import Config

LOGGER = logging.getLogger(__name__)


class AwsCred:
    __DEFAULT_REGION = 'us-west-2'

    def __init__(self):
        self.__region = Config().get_value(Config.aws_region, AwsCred.__DEFAULT_REGION)
        LOGGER.debug(f'using region: {self.__region}')
        self.__boto3_session = {'region_name': self.__region}
        aws_access_key_id = Config().get_value(Config.aws_access_key_id, '')
        aws_secret_access_key = Config().get_value(Config.aws_secret_access_key, '')
        aws_session_token = Config().get_value(Config.aws_session_token, '')
        if aws_access_key_id != '':
            LOGGER.debug('using aws_access_key_id as it is not empty')
            if aws_secret_access_key == '':
                raise ValueError(f'missing aws_secret_access_key for aws_access_key_id ends with {aws_access_key_id[-3:]}')
            self.__boto3_session['aws_access_key_id'] = aws_access_key_id
            self.__boto3_session['aws_secret_access_key'] = aws_secret_access_key
            if aws_session_token != '':
                LOGGER.debug('adding aws_session_token as it is not empty and aws_access_key_id exists')
                self.__boto3_session['aws_session_token'] = aws_session_token
        else:
            LOGGER.debug('using default session as there is  no aws_access_key_id')
    @property
    def region(self):
        return self.__region

    @region.setter
    def region(self, val):
        """
        :param val:
        :return: None
        """
        self.__region = val
        return

    @property
    def boto3_session(self):
        return self.__boto3_session

    @boto3_session.setter
    def boto3_session(self, val):
        """
        :param val:
        :return: None
        """
        self.__boto3_session = val
        return

    def get_session(self):
        return boto3.Session(**self.boto3_session)

    def get_resource(self, service_name: str):
        return boto3.Session(**self.boto3_session).resource(service_name)

    def get_client(self, service_name: str):
        return boto3.Session(**self.boto3_session).client(service_name)
