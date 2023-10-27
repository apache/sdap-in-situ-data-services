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
from unittest import TestCase

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp

from parquet_flask.io_logic.cdms_constants import CDMSConstants
from parquet_flask.parquet_stat_extractor.statistics_retriever import StatisticsRetriever
from parquet_flask.utils.time_utils import TimeUtils


class TestStatisticsRetriever(TestCase):
    def test_01(self):
        input_json = [
            {
                'Qout': 3.4,
                'platform': {'short_name': 'test'},
                CDMSConstants.lat_col: 0.0,
                CDMSConstants.lon_col: 1.0,
                CDMSConstants.depth_col: 2.0,
                CDMSConstants.time_col: '2000-01-01T00:00:03Z',
            },
            {
                'Qout': -23.4,
                'platform': {'short_name': 'test'},
                CDMSConstants.lat_col: 1.0,
                CDMSConstants.lon_col: 2.0,
                CDMSConstants.depth_col: 3.0,
                CDMSConstants.time_col: '2000-01-01T00:00:00Z',
            },
            {
                'Qout': 13.4,
                'platform': {'short_name': 'test'},
                CDMSConstants.lat_col: 2.0,
                CDMSConstants.lon_col: 3.0,
                CDMSConstants.depth_col: -1.0,
                CDMSConstants.time_col: '2000-01-01T00:00:01Z',
            },
            {
                'Qout': 13.6,
                'platform': {'short_name': 'test'},
                CDMSConstants.lat_col: 3.0,
                CDMSConstants.lon_col: 0.0,
                CDMSConstants.depth_col: 1.0,
                CDMSConstants.time_col: '2000-01-01T00:00:02Z',
            },
            {
                'Qout': 0.6,
                'platform': {'short_name': 'test'},
                CDMSConstants.lat_col: 0.0,
                CDMSConstants.lon_col: 1.0,
                CDMSConstants.depth_col: float(CDMSConstants.missing_depth_value),
                CDMSConstants.time_col: '2000-01-01T00:00:03Z',
            },

        ]
        os.environ['JAVA_HOME'] = '/Library/Java/JavaVirtualMachines/openjdk-11.jdk/Contents/Home'
        spark = SparkSession.builder \
            .master("local") \
            .appName('TestAppName') \
            .getOrCreate()
        df = spark.createDataFrame(input_json)
        df = df.withColumn(CDMSConstants.time_obj_col, to_timestamp(CDMSConstants.time_col))
        stats_retriever = StatisticsRetriever(df, ['pm2_5', 'Qout']).start()
        print(stats_retriever.to_json())
        self.assertEqual(stats_retriever.min_lat, 0.0, 'wrong min_lat')
        self.assertEqual(stats_retriever.max_lat, 3.0, 'wrong max_lat')
        self.assertEqual(stats_retriever.min_lon, 0.0, 'wrong min_lon')
        self.assertEqual(stats_retriever.max_lon, 3.0, 'wrong max_lon')
        self.assertEqual(stats_retriever.min_depth, -99999.0, 'wrong min_depth')
        self.assertEqual(stats_retriever.max_depth, 3.0, 'wrong max_depth')
        self.assertEqual(stats_retriever.min_datetime, TimeUtils.get_datetime_obj('2000-01-01T00:00:00Z').timestamp(), 'wrong min_datetime')
        self.assertEqual(stats_retriever.max_datetime, TimeUtils.get_datetime_obj('2000-01-01T00:00:03Z').timestamp(), 'wrong max_datetime')
        self.assertEqual(stats_retriever.total, 5, 'wrong total')
        return

    def test_02(self):
        input_json = [
            {
                'Qout': 3.4,
                'platform': {'short_name': 'test'},
                CDMSConstants.lat_col: 0.0,
                CDMSConstants.lon_col: 1.0,
                CDMSConstants.time_col: '2000-01-01T00:00:03Z',
            },
            {
                'Qout': -23.4,
                'platform': {'short_name': 'test'},
                CDMSConstants.lat_col: 1.0,
                CDMSConstants.lon_col: 2.0,
                CDMSConstants.time_col: '2000-01-01T00:00:00Z',
            },
            {
                'Qout': 13.4,
                'platform': {'short_name': 'test'},
                CDMSConstants.lat_col: 2.0,
                CDMSConstants.lon_col: 3.0,
                CDMSConstants.time_col: '2000-01-01T00:00:01Z',
            },
            {
                'Qout': 13.6,
                'platform': {'short_name': 'test'},
                CDMSConstants.lat_col: 3.0,
                CDMSConstants.lon_col: 0.0,
                CDMSConstants.time_col: '2000-01-01T00:00:02Z',
            },
            {
                'Qout': 0.6,
                'platform': {'short_name': 'test'},
                CDMSConstants.lat_col: 0.0,
                CDMSConstants.lon_col: 1.0,
                CDMSConstants.time_col: '2000-01-01T00:00:03Z',
            },

        ]
        os.environ['JAVA_HOME'] = '/Library/Java/JavaVirtualMachines/openjdk-11.jdk/Contents/Home'
        spark = SparkSession.builder \
            .master("local") \
            .appName('TestAppName') \
            .getOrCreate()
        df = spark.createDataFrame(input_json)
        df = df.withColumn(CDMSConstants.time_obj_col, to_timestamp(CDMSConstants.time_col))
        stats_retriever = StatisticsRetriever(df, ['pm2_5', 'Qout']).start()
        print(stats_retriever.to_json())
        self.assertEqual(stats_retriever.min_lat, 0.0, 'wrong min_lat')
        self.assertEqual(stats_retriever.max_lat, 3.0, 'wrong max_lat')
        self.assertEqual(stats_retriever.min_lon, 0.0, 'wrong min_lon')
        self.assertEqual(stats_retriever.max_lon, 3.0, 'wrong max_lon')
        self.assertEqual(stats_retriever.min_depth, None, 'wrong min_depth')
        self.assertEqual(stats_retriever.max_depth, None, 'wrong max_depth')
        self.assertEqual(stats_retriever.min_datetime, TimeUtils.get_datetime_obj('2000-01-01T00:00:00Z').timestamp(), 'wrong min_datetime')
        self.assertEqual(stats_retriever.max_datetime, TimeUtils.get_datetime_obj('2000-01-01T00:00:03Z').timestamp(), 'wrong max_datetime')
        self.assertEqual(stats_retriever.total, 5, 'wrong total')
        return
