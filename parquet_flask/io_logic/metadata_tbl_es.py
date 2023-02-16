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

from parquet_flask.aws.aws_ddb import AwsDdb, AwsDdbProps
from parquet_flask.aws.es_abstract import ESAbstract
from parquet_flask.io_logic.cdms_constants import CDMSConstants
from parquet_flask.io_logic.metadata_tbl_interface import MetadataTblInterface
from parquet_flask.utils.config import Config


class MetadataTblES(MetadataTblInterface):
    """
    Table columns
        - s3_url
        - uuid
        - size
        - ingested_date
        - md5
        - num-of-records

        Table settings
        s3_url as primary key
        secondary index: uuid
    """
    def __init__(self, es_mdw: ESAbstract):
        self.__es: ESAbstract = es_mdw

    def insert_record(self, new_record):
        self.__es.index_one(new_record, new_record[CDMSConstants.s3_url_key], CDMSConstants.entry_file_records_index)
        return self

    def replace_record(self, new_record):
        self.__es.update_one(new_record, new_record[CDMSConstants.s3_url_key], CDMSConstants.entry_file_records_index)
        return

    def get_by_s3_url(self, s3_url):
        result = self.__es.query_by_id(s3_url)
        if result is None:
            return None
        return result['_source']

    def get_by_uuid(self, uuid):
        result = self.__es.query({
            'query': {
                'bool': {
                    'must': [
                        {
                            'term': {'uuid': uuid}
                        }
                    ]
                }
            }
        }, CDMSConstants.entry_file_records_index)
        if result is None:
            return None
        result = result['hits']['hits']
        if len(result) < 1:
            return None
        return result[0]['_source']

    def delete_by_s3_url(self, s3_url):
        self.__es.delete_by_id(s3_url)
        return self

    def query_by_date_range(self, start_time, end_time):
        raise NotImplementedError('cannot implement range query to the primary key in DDB')
