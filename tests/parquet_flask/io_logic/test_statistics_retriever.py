from unittest import TestCase

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp

from parquet_flask.io_logic.cdms_constants import CDMSConstants
from parquet_flask.io_logic.statistics_retriever import StatisticsRetriever
from parquet_flask.utils.time_utils import TimeUtils


class TestStatisticsRetriever(TestCase):
    def test_01(self):
        input_json = [
            {
                CDMSConstants.lat_col: 0.0,
                CDMSConstants.lon_col: 1.0,
                CDMSConstants.depth_col: 2.0,
                CDMSConstants.time_col: '2000-01-01T00:00:03Z',
            },
            {
                CDMSConstants.lat_col: 1.0,
                CDMSConstants.lon_col: 2.0,
                CDMSConstants.depth_col: 3.0,
                CDMSConstants.time_col: '2000-01-01T00:00:00Z',
            },
            {
                CDMSConstants.lat_col: 2.0,
                CDMSConstants.lon_col: 3.0,
                CDMSConstants.depth_col: 0.0,
                CDMSConstants.time_col: '2000-01-01T00:00:01Z',
            },
            {
                CDMSConstants.lat_col: 3.0,
                CDMSConstants.lon_col: 0.0,
                CDMSConstants.depth_col: 1.0,
                CDMSConstants.time_col: '2000-01-01T00:00:02Z',
            },
            {
                CDMSConstants.lat_col: 0.0,
                CDMSConstants.lon_col: 1.0,
                CDMSConstants.depth_col: float(CDMSConstants.missing_depth_value),
                CDMSConstants.time_col: '2000-01-01T00:00:03Z',
            },

        ]

        spark = SparkSession.builder \
            .master("local") \
            .appName('TestAppName') \
            .getOrCreate()
        df = spark.createDataFrame(input_json)
        df = df.withColumn(CDMSConstants.time_obj_col, to_timestamp(CDMSConstants.time_col))
        stats_retriever = StatisticsRetriever(df).start()
        self.assertEqual(stats_retriever.min_lat, 0.0, 'wrong min_lat')
        self.assertEqual(stats_retriever.max_lat, 3.0, 'wrong max_lat')
        self.assertEqual(stats_retriever.min_lon, 0.0, 'wrong min_lon')
        self.assertEqual(stats_retriever.max_lon, 3.0, 'wrong max_lon')
        self.assertEqual(stats_retriever.min_depth, 0.0, 'wrong min_depth')
        self.assertEqual(stats_retriever.max_depth, 3.0, 'wrong max_depth')
        self.assertEqual(stats_retriever.min_datetime, TimeUtils.get_datetime_obj('2000-01-01T00:00:00Z').timestamp(), 'wrong min_datetime')
        self.assertEqual(stats_retriever.max_datetime, TimeUtils.get_datetime_obj('2000-01-01T00:00:03Z').timestamp(), 'wrong max_datetime')
        self.assertEqual(stats_retriever.total, 5, 'wrong total')
        return
