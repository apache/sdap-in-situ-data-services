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
import time

from parquet_flask.aws.es_abstract import ESAbstract
from parquet_flask.aws.es_factory import ESFactory
from parquet_flask.cdms_lambda_func.cdms_lambda_constants import CdmsLambdaConstants
from parquet_flask.cdms_lambda_func.ingest_s3_to_cdms.ingest_s3_to_cdms import IngestS3ToCdms
from parquet_flask.cdms_lambda_func.lambda_logger_generator import LambdaLoggerGenerator
from parquet_flask.cdms_lambda_func.s3_records.s3_2_sqs import S3ToSqs
from parquet_flask.io_logic.cdms_constants import CDMSConstants
from parquet_flask.utils.sns_msg_retriever import LambdaEventMsgRetriever
from parquet_flask.utils.time_utils import TimeUtils
LOGGER = LambdaLoggerGenerator.get_logger(__name__, log_level=LambdaLoggerGenerator.get_level_from_env())


class ParquetIngestionTracker:
    def __init__(self):
        self.__es_url = os.environ.get(CdmsLambdaConstants.es_url, None)
        self.__es_index = os.environ.get(CdmsLambdaConstants.es_index, CDMSConstants.staging_file_records_index)
        self.__es_port = int(os.environ.get(CdmsLambdaConstants.es_port, '443'))
        if any([k is None for k in [self.__es_url, self.__es_index]]):
            raise ValueError(f'invalid env. must have {[CdmsLambdaConstants.es_url, CdmsLambdaConstants.es_index]}')
        self.__es: ESAbstract = ESFactory().get_instance('AWS', index=self.__es_index, base_url=self.__es_url, port=self.__es_port)

    def start(self, event):
        sns_msgs = LambdaEventMsgRetriever().from_sqs(event)
        updating_docs = {k['s3_url']: {'event_time': TimeUtils.get_current_time_unix(),'ingestion_status': CDMSConstants.ingestion_stage_success} for k in sns_msgs}
        LOGGER.debug(f'succeeded: updating_docs: {updating_docs}')
        self.__es.update_many(doc_dict=updating_docs, index=self.__es_index)
        time.sleep(3.0)
        to_be_ingested_files = self.__es.query({
            'size': len(sns_msgs),
            'sort': [{
                'event_time': {'order': 'asc'}
            }],
            'query': {
                'bool': {
                    'must':[{'term': {'ingestion_status': {'value': CDMSConstants.ingestion_stage_ready}}}]
                }
            }
        })

        to_be_ingested_files = [k['_source'] for k in to_be_ingested_files['hits']['hits']]
        LOGGER.debug(f'to_be_ingested_files: {to_be_ingested_files}')
        ingest_to_cdms = IngestS3ToCdms()
        updating_docs = {}
        for each in to_be_ingested_files:
            s3_url = each['s3_url']
            result = ingest_to_cdms.start_single(s3_url)
            LOGGER.debug(f'result for {s3_url}: {result}')
            updating_docs[s3_url] = {'event_time': TimeUtils.get_current_time_unix(),'ingestion_status': CDMSConstants.ingestion_stage_progress}
        LOGGER.debug(f'progress: updating_docs: {updating_docs}')
        self.__es.update_many(doc_dict=updating_docs, index=self.__es_index)
        return
