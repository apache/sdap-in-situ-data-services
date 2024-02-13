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

from parquet_flask.aws.es_abstract import ESAbstract
from parquet_flask.aws.es_factory import ESFactory
from parquet_flask.cdms_lambda_func.cdms_lambda_constants import CdmsLambdaConstants
from parquet_flask.cdms_lambda_func.lambda_logger_generator import LambdaLoggerGenerator
from parquet_flask.cdms_lambda_func.s3_records.s3_2_sqs import S3ToSqs
from parquet_flask.io_logic.cdms_constants import CDMSConstants
from parquet_flask.utils.sns_msg_retriever import LambdaEventMsgRetriever
from parquet_flask.utils.time_utils import TimeUtils
LOGGER = LambdaLoggerGenerator.get_logger(__name__, log_level=LambdaLoggerGenerator.get_level_from_env())


class S3ToESStager:
    def __init__(self):
        if any([k not in os.environ for k in [CdmsLambdaConstants.es_url]]):
            raise ValueError(f'invalid env. must have {[CdmsLambdaConstants.es_url]}')
        self.__es_url = os.environ.get(CdmsLambdaConstants.es_url, None)
        self.__es_index = os.environ.get(CdmsLambdaConstants.es_index, CDMSConstants.staging_file_records_index)
        self.__es_port = int(os.environ.get(CdmsLambdaConstants.es_port, '443'))
        self.__es: ESAbstract = ESFactory().get_instance('AWS', index=self.__es_index, base_url=self.__es_url, port=self.__es_port)

    def start(self, event):
        LOGGER.debug(f'event: {event}')
        sns_msgs = LambdaEventMsgRetriever().from_sqs(event)
        for each_sns_msg in sns_msgs:
            LOGGER.debug(f'each_sns_msg: {each_sns_msg}')
            s3_summary = LambdaEventMsgRetriever().get_s3_from_sns(each_sns_msg)
            inserting_doc = {
                'event_time': TimeUtils.get_current_time_unix(),
                's3_url': f's3://{s3_summary["bucket"]}/{s3_summary["key"]}',
                'ingestion_status': CDMSConstants.ingestion_stage_ready
            }
            self.__es.index_one(inserting_doc, inserting_doc['s3_url'], self.__es_index)
        return
