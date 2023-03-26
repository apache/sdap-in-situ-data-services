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

from parquet_flask.utils.spatial_utils import SpatialUtils


class TestSpatialUtils(unittest.TestCase):
    def test_generate_lat_lon_intervals_01(self):
        result = SpatialUtils.generate_lat_lon_intervals((-2.3, -4.7789), (21.39987, 25.00000), 5)
        expected_result = [(each_lat, each_lon) for each_lon in range(-5, 26, 5) for each_lat in range(-5, 22, 5)]
        self.assertEqual(expected_result, result, 'wrong output')
        return

    def test_generate_lat_lon_intervals_02(self):
        result = SpatialUtils.generate_lat_lon_intervals((-2.3, -4.7789), (0.0, 0.0), 5)
        expected_result = [(-5, -5), (0, -5), (-5, 0), (0, 0)]
        self.assertEqual(expected_result, result, 'wrong output')
        return

    def test_generate_lat_lon_intervals_03(self):
        result = SpatialUtils.generate_lat_lon_intervals((0, 0), (2, 2), 5)
        expected_result = [(0, 0)]
        self.assertEqual(expected_result, result, 'wrong output')
        return
