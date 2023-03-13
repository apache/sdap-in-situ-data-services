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
import sys

from parquet_flask.cdms_lambda_func.lambda_func_env import LambdaFuncEnv


class LambdaLoggerGenerator:
    @staticmethod
    def remove_default_handlers():
        root_logger = logging.getLogger()
        for each in root_logger.handlers:
            root_logger.removeHandler(each)
        return

    @staticmethod
    def get_level_from_env():
        return int(os.environ.get(LambdaFuncEnv.LOG_LEVEL, logging.INFO))

    @staticmethod
    def get_logger(logger_name: str, log_level: int = logging.INFO, log_format: str = None):
        if log_format is None:
            log_format = LambdaFuncEnv.LOG_FORMAT
        new_logger = logging.getLogger(logger_name)
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(logging.Formatter(log_format))
        stream_handler.setLevel(log_level)
        new_logger.setLevel(log_level)
        new_logger.addHandler(stream_handler)
        return new_logger
