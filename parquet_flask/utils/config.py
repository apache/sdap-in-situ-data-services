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

import os

from parquet_flask.utils.singleton import Singleton


class Config(metaclass=Singleton):
    def __init__(self):
        self.__keys = [
            'master_spark_url',
            'spark_app_name',
            'parquet_file_name',
            'aws_access_key_id',
            'aws_secret_access_key',
            'aws_session_token',
            'in_situ_schema',
        ]
        self.__optional_keys = [
            'spark_ram_size',
        ]
        self.__validate()

    def __validate(self):
        missing_mandatory_keys = [k for k in self.__keys if k not in os.environ]
        if len(missing_mandatory_keys) > 0:
            raise RuntimeError('missing configuration values in environment values: {}'.format(missing_mandatory_keys))
        return

    def get_value(self, key, default_val=None):
        if key in os.environ:
            return os.environ[key]
        return default_val
