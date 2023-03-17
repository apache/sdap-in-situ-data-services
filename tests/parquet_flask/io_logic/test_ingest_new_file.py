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

import json
import os
from unittest import TestCase

from parquet_flask.io_logic.ingest_new_file import get_geospatial_interval


class TestGeneralUtilsV3(TestCase):
    def test_get_geospatial_interval(self):
        icoads_interval_by_platform = {
            "0": "50",
            "16": "7.5" 
        }
        samos_interval_by_platform = {
            "0": "50",
            "17": "7.5"
        }
        os.environ['geospatial_interval_by_platform'] = json.dumps({
            "ICOADS Release 3.0": icoads_interval_by_platform,
            "SAMOS": samos_interval_by_platform
        })
        self.assertDictEqual(get_geospatial_interval('SAMOS'), samos_interval_by_platform, 'wrong for SAMOS')
        self.assertDictEqual(get_geospatial_interval('ICOADS Release 3.0'), icoads_interval_by_platform, 'wrong for ICOADS Release 3.0')
        self.assertEqual(get_geospatial_interval('t1'), {}, 'wrong for t1')
        return
