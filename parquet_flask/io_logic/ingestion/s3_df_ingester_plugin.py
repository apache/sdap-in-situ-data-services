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
from tempfile import TemporaryDirectory

import pandas as pd
from pandas import DataFrame

from parquet_flask.aws.aws_s3 import AwsS3
from parquet_flask.io_logic.cdms_constants import CDMSConstants
from parquet_flask.io_logic.ingestion.aws_file_ingester_plugin_abstract import AwsFileIngesterPluginAbstract
from parquet_flask.io_logic.ingest_new_file import IngestNewJsonFile
from parquet_flask.io_logic.sanitize_record import SanitizeRecord
from parquet_flask.utils.config import Config
from parquet_flask.utils.file_utils import FileUtils
from parquet_flask.utils.general_utils import GeneralUtils
from parquet_flask.utils.time_utils import TimeUtils

LOGGER = logging.getLogger(__name__)


class S3DataFramesIngesterPlugin(AwsFileIngesterPluginAbstract):

    def _execute_ingest_data(self):
        try:
            LOGGER.debug(f'ingesting file: {self._saved_file_name}')
            s3 = AwsS3()
            s3_bucket, s3_folder = s3.split_s3_url(self._props.s3_url)
            total_num_records = []
            start_time = TimeUtils.get_current_time_unix()
            for each_path, each_size in s3.get_child_s3_files(s3_bucket, s3_folder):
                LOGGER.debug(f'working on file: {each_path}')
                with TemporaryDirectory() as tmp_dir_name:
                    local_file_path = s3.set_s3_url(f's3://{s3_bucket}/{each_path}').download(tmp_dir_name)
                    df = pd.read_pickle(local_file_path)
                    LOGGER.debug(f'un-pickled the file')
                    num_records = IngestNewJsonFile(self._props.is_replacing).ingest_df(df, self._props.uuid,
                                                                                        self._props.provider,
                                                                                        self._props.project)
                    total_num_records.append(num_records)
            end_time = TimeUtils.get_current_time_unix()
            LOGGER.debug(f'uploading to metadata table')
            self._generate_db_record(start_time, end_time, total_num_records)
            LOGGER.debug(f'deleting used file')
            FileUtils.del_file(self._saved_file_name)
            LOGGER.warning('Disabled tagging S3 due to IAM issues')
            # LOGGER.debug(f'tagging s3')
            # s3.add_tags_to_obj({
            #     'parquet_ingested': TimeUtils.get_time_str(self.__ingested_date),
            #     'job_id': self.__props.uuid,
            # })
        except Exception as e:
            LOGGER.exception(f'deleting error file')
            FileUtils.del_file(self._saved_file_name)
            raise e
        if self._sha512_result is True:
            self._props.result_status_code = 201
            self._props.result_json = {'message': 'ingested', 'job_id': self._props.uuid}
            return self
        self._props.result_status_code = 203
        self._props.result_json = {'message': 'ingested, different sha512', 'cause': self._sha512_cause, 'job_id': self._props.uuid}
        return self
