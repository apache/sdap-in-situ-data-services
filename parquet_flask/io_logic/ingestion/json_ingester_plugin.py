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

from pandas import DataFrame

from parquet_flask.io_logic.cdms_constants import CDMSConstants
from parquet_flask.io_logic.ingestion.aws_file_ingester_plugin_abstract import AwsFileIngesterPluginAbstract
from parquet_flask.io_logic.ingest_new_file import IngestNewJsonFile
from parquet_flask.io_logic.sanitize_record import SanitizeRecord
from parquet_flask.utils.config import Config
from parquet_flask.utils.file_utils import FileUtils
from parquet_flask.utils.general_utils import GeneralUtils
from parquet_flask.utils.time_utils import TimeUtils

LOGGER = logging.getLogger(__name__)


class JsonIngesterPlugin(AwsFileIngesterPluginAbstract):
    def _execute_ingest_data(self):
        try:
            LOGGER.debug(f'ingesting file: {self._saved_file_name}')
            start_time = TimeUtils.get_current_time_unix()
            ingest_new_file = IngestNewJsonFile(self._props.is_replacing)
            ingest_new_file.sanitize_record = self._props.is_sanitizing

            LOGGER.debug(f'sanitizing the files ? : {self._props.is_sanitizing}')
            if self._props.is_sanitizing is True:
                input_json = SanitizeRecord(Config().get_value('in_situ_schema')).start(self._saved_file_name)
            else:
                if not FileUtils.file_exist(self._saved_file_name):
                    raise ValueError('json file does not exist: {}'.format(self._saved_file_name))
                input_json = FileUtils.read_json(self._saved_file_name)

            total_num_records = []
            for i, each_chunk in enumerate(GeneralUtils.chunk_list(input_json[CDMSConstants.observations_key], self._props.chunk_size)):
                LOGGER.debug(f'processing chunk {i}')
                df = DataFrame(each_chunk)
                num_records = IngestNewJsonFile(self._props.is_replacing).ingest_df(df, self._props.uuid,
                                                                                    self._props.provider,
                                                                                    self._props.project)
                total_num_records.append(num_records)
            end_time = TimeUtils.get_current_time_unix()
            LOGGER.debug(f'uploading to metadata table')
            self._generate_db_record(start_time, end_time, num_records)
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
