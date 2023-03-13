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
        os.environ['geospatial_interval_by_project'] = json.dumps({
            "ICOADS Release 3.0": 100,
            "SAMOS": "50",
            "t1": "7.5",
            "SPURS": "75"
        })
        self.assertEqual(get_geospatial_interval('SAMOS'), 50, 'wrong for SAMOS')
        self.assertEqual(get_geospatial_interval('SPURS'), 75, 'wrong for SPURS')
        self.assertEqual(get_geospatial_interval('ICOADS Release 3.0'), 100, 'wrong for ICOADS Release 3.0')
        self.assertEqual(get_geospatial_interval('t1'), 30, 'wrong for t1')
        return
