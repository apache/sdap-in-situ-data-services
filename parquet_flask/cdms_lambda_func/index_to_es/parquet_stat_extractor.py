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

import json
import os

import requests

from parquet_flask.cdms_lambda_func.cdms_lambda_constants import CdmsLambdaConstants
from parquet_flask.cdms_lambda_func.lambda_logger_generator import LambdaLoggerGenerator

LOGGER = LambdaLoggerGenerator.get_logger(__name__, log_level=LambdaLoggerGenerator.get_level_from_env())


class ParquetStatExtractor:
    def __init__(self):
        self.__cdms_url = os.environ.get(CdmsLambdaConstants.cdms_url, None)
        self.__parquet_base_folder = os.environ.get(CdmsLambdaConstants.parquet_base_folder, 'None')
        self.__verify_ssl = os.environ.get(CdmsLambdaConstants.verify_ssl, 'true').strip().upper() == 'TRUE'
        if any([k is None for k in [self.__cdms_url, self.__parquet_base_folder]]):
            raise ValueError(f'invalid env. must have {[CdmsLambdaConstants.cdms_url, CdmsLambdaConstants.parquet_base_folder]}')

    def __get_parquet_s3_path(self, s3_key: str):
        parquet_s3_path = s3_key.replace(self.__parquet_base_folder, '')
        if parquet_s3_path.startswith('/'):
            parquet_s3_path = parquet_s3_path[1:]
        LOGGER.debug(f'parquet_s3_path: {parquet_s3_path}')
        return parquet_s3_path

    def start(self, s3_key: str):
        stats_url = f'{self.__cdms_url}?s3_key={self.__get_parquet_s3_path(s3_key)}'
        LOGGER.debug(f'stats_url: {stats_url}')
        response = requests.get(url=stats_url, verify=self.__verify_ssl)
        if response.status_code > 400:
            raise ValueError(f'wrong status code: {response.status_code}. details: {response.text}')
        LOGGER.debug(f'stats_response_code: {response.status_code}')
        LOGGER.debug(f'stats_result: {response.text}')
        return json.loads(response.text)
