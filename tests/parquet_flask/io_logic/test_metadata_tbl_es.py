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

from time import sleep
from unittest import TestCase

from parquet_flask.aws.es_abstract import ESAbstract
from parquet_flask.aws.es_factory import ESFactory
from parquet_flask.io_logic.cdms_constants import CDMSConstants
from parquet_flask.io_logic.metadata_tbl_es import MetadataTblES


class TestMetadataTblES(TestCase):
    def test_01(self):
        es_url = 'https://search-insitu-parquet-dev-1-vgwt2bx23o5w3gpnq4afftmvaq.us-west-2.es.amazonaws.com'
        aws_es: ESAbstract = ESFactory().get_instance('AWS', index=CDMSConstants.entry_file_records_index, base_url=es_url, port=443)
        meta_tbl_es = MetadataTblES(aws_es)
        meta_tbl_es.insert_record({
            CDMSConstants.s3_url_key: 's3://unit-test-bucket/sample-file.json',
            CDMSConstants.uuid_key: 'unit-test-1',
            CDMSConstants.ingested_date_key: 0,
            CDMSConstants.file_size_key: 111,
            CDMSConstants.checksum_key: 'sample-checksum',
            CDMSConstants.checksum_validation: True,
            CDMSConstants.checksum_cause: '',
            CDMSConstants.job_start_key: 10,
            CDMSConstants.job_end_key: 20,
            CDMSConstants.records_count_key: 100,
        })
        sleep(3)
        record = meta_tbl_es.get_by_s3_url('s3://unit-test-bucket/sample-file.json')
        self.assertNotEqual(record, None, 'record is None')
        self.assertEqual(record[CDMSConstants.checksum_key], 'sample-checksum', 'wrong checksum')
        meta_tbl_es.replace_record({
            CDMSConstants.s3_url_key: 's3://unit-test-bucket/sample-file.json',
            CDMSConstants.uuid_key: 'unit-test-1',
            CDMSConstants.ingested_date_key: 0,
            CDMSConstants.file_size_key: 111,
            CDMSConstants.checksum_key: '',
            CDMSConstants.checksum_validation: False,
            CDMSConstants.checksum_cause: 'sample-reason',
            CDMSConstants.job_start_key: 10,
            CDMSConstants.job_end_key: 20,
            CDMSConstants.records_count_key: 200,
        })
        sleep(3)
        record = meta_tbl_es.get_by_uuid('unit-test-1')
        self.assertNotEqual(record, None, 'record is None')
        self.assertEqual(record[CDMSConstants.checksum_validation], False, 'wrong checksum_validation')
        self.assertEqual(record[CDMSConstants.checksum_cause], 'sample-reason', 'wrong checksum_cause')
        self.assertEqual(record[CDMSConstants.records_count_key], 200, 'wrong records_count_key')
        meta_tbl_es.delete_by_s3_url('s3://unit-test-bucket/sample-file.json')
        sleep(3)
        record = meta_tbl_es.get_by_s3_url('s3://unit-test-bucket/sample-file.json')
        self.assertEqual(record, None, f'record is not None. {record}')

        return
