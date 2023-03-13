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

import unittest
import os

os.environ['master_spark_url'] = ''
os.environ['spark_app_name'] = ''
os.environ['parquet_file_name'] = ''
os.environ['in_situ_schema'] = ''
os.environ['authentication_type'] = ''
os.environ['authentication_key'] = ''
os.environ['parquet_metadata_tbl'] = ''
from parquet_flask.utils.file_utils import FileUtils

from parquet_flask.io_logic.cdms_schema import CdmsSchema


class TestGeneralUtilsV3(unittest.TestCase):
    def test_01(self):
        cdms_schema = CdmsSchema()
        old_struct = cdms_schema.ALL_SCHEMA
        new_struct = cdms_schema.get_schema_from_json(FileUtils.read_json('../../../in_situ_schema.json'))
        self.assertEqual(sorted(str(old_struct)), sorted(str(new_struct)), f'not equal old_struct = {old_struct}. new_struct = {new_struct}')
        return

    def test_02(self):
        cdms_schema = CdmsSchema()
        new_struct = cdms_schema.get_pandas_schema_from_json(FileUtils.read_json('../../../in_situ_schema.json'))
        print(new_struct)
        self.assertTrue(isinstance(new_struct, dict), f'wrong type: {new_struct}')
        return
