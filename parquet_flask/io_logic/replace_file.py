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

from parquet_flask.io_logic.cdms_constants import CDMSConstants
from parquet_flask.io_logic.ingest_new_file import IngestNewJsonFile
from parquet_flask.io_logic.retrieve_spark_session import RetrieveSparkSession
from parquet_flask.io_logic.sanitize_record import SanitizeRecord
from parquet_flask.utils.config import Config
from parquet_flask.utils.file_utils import FileUtils

from parquet_flask.utils.time_utils import TimeUtils

LOGGER = logging.getLogger(__name__)


class ReplaceJsonFile:
    def __init__(self):
        self.__sss = RetrieveSparkSession()
        config = Config()
        self.__app_name = config.get_spark_app_name()
        self.__master_spark = config.get_value('master_spark_url')
        self.__mode = 'overwrite'
        self.__parquet_name = config.get_value('parquet_file_name')

    def ingest(self, abs_file_path, job_id):
        """
        This method will assume that incoming file has data with in_situ_schema file.

        So, it will definitely has `time`, `project`, and `provider`.

        :param abs_file_path:
        :param job_id:
        :return:
        """
        if not FileUtils.file_exist(abs_file_path):
            raise ValueError('missing file to ingest it. path: {}'.format(abs_file_path))
        LOGGER.debug(f'sanitizing the files')
        input_json = SanitizeRecord(Config().get_value('in_situ_schema')).start(abs_file_path)
        spark_session = self.__sss.retrieve_spark_session(self.__app_name, self.__master_spark)
        spark_session.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        df_writer = IngestNewJsonFile.create_df(spark_session,
                                                input_json[CDMSConstants.observations_key],
                                                job_id,
                                                input_json[CDMSConstants.provider_col],
                                                input_json[CDMSConstants.project_col])
        df_writer.mode(self.__mode).parquet(self.__parquet_name, compression='GZIP')
        LOGGER.debug(f'finished writing parquet')
        return len(input_json[CDMSConstants.observations_key])
