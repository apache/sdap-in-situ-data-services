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

from parquet_flask.utils.general_utils import GeneralUtils


class TestGeneralUtils(unittest.TestCase):
    def test_is_float(self):
        self.assertTrue(GeneralUtils.is_float('1.2', False), f'wrong result for 1.2')
        self.assertTrue(GeneralUtils.is_float('-1.2', False), f'wrong result for -1.2')
        self.assertTrue(GeneralUtils.is_float('0', False), f'wrong result for 0')
        self.assertTrue(GeneralUtils.is_float('-0', False), f'wrong result for -0')
        self.assertTrue(GeneralUtils.is_float('-0.00000001', False), f'wrong result for -0.00000001')
        self.assertFalse(GeneralUtils.is_float('nan', False), f'wrong result for nan')
        self.assertTrue(GeneralUtils.is_float('nan', True), f'wrong result for nan. accept nan')
        self.assertFalse(GeneralUtils.is_float('0.23a', False), f'wrong result for 0.23a')
        self.assertFalse(GeneralUtils.is_float('0.23^5', False), f'wrong result for 0.23^5')
        return

    def test_gen_float_list_from_comma_sep_str(self):
        self.assertEqual(GeneralUtils.gen_float_list_from_comma_sep_str('-99.34, 23, 44.8765, 34', 4), [-99.34, 23, 44.8765, 34])
        self.assertEqual(GeneralUtils.gen_float_list_from_comma_sep_str('-99.34, 23, 44.8765, 34.987654321234567', 4), [-99.34, 23, 44.8765, 34.987654321234567])
        self.assertEqual(GeneralUtils.gen_float_list_from_comma_sep_str('0, 44.8765', 2), [0, 44.8765])
        self.assertRaises(ValueError, GeneralUtils.gen_float_list_from_comma_sep_str, '-99.34, 23, 44.8765e, 34', 4)
        self.assertRaises(ValueError, GeneralUtils.gen_float_list_from_comma_sep_str, '-99.34, 23, 44.8765, ', 4)
        self.assertRaises(ValueError, GeneralUtils.gen_float_list_from_comma_sep_str, '-99.34, 23', 3)
        return

    def test_floor_lat_long(self):
        self.assertEqual(GeneralUtils.floor_lat_long(-3, 5), '-5_5')
        self.assertEqual(GeneralUtils.floor_lat_long(0, -0), '0_0')
        self.assertEqual(GeneralUtils.floor_lat_long(22, -0), '20_0')
        self.assertEqual(GeneralUtils.floor_lat_long(-2, 4, 3), '-3_3')
        return
