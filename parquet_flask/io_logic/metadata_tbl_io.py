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
from parquet_flask.io_logic.cdms_constants import CDMSConstants
from parquet_flask.io_logic.metadata_tbl_interface import MetadataTblInterface


class MetadataTblIO(MetadataTblInterface):
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
    def __init__(self):
        ddb_props = AwsDdbProps()
        ddb_props.hash_key = CDMSConstants.s3_url_key
        ddb_props.tbl_name = 'cdms_parquet_meta_dev_v1'  # TODO come from config
        self.__uuid_index = 'uuid-index'
        self.__ddb = AwsDdb(ddb_props)

    def insert_record(self, new_record):
        self.__ddb.add_one_item(new_record, new_record[CDMSConstants.s3_url_key])
        return

    def replace_record(self, new_record):
        self.__ddb.add_one_item(new_record, new_record[CDMSConstants.s3_url_key], replace=True)
        return

    def get_by_s3_url(self, s3_url):
        return self.__ddb.get_one_item(s3_url)

    def get_by_uuid(self, uuid):
        return self.__ddb.get_from_index(self.__uuid_index, {CDMSConstants.uuid_key: uuid})

    def query_by_date_range(self, start_time, end_time):
        raise NotImplementedError('cannot implement range query to the primary key in DDB')
