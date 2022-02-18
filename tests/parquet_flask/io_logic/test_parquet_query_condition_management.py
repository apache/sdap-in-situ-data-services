import unittest

from parquet_flask.io_logic.parquet_query_condition_management import ParquetQueryConditionManagement
from parquet_flask.io_logic.query_v2 import QueryProps


class TestGeneralUtils(unittest.TestCase):
    def test_01(self):
        props = QueryProps()
        props.provider = 'mock_provider'
        props.project = 'mock_project'
        props.platform_code = '123'
        props.variable = ['air_pressure']
        props.columns = ['air_temp']
        props.quality_flag = True
        props.min_datetime = '2018-03-03T00:00:00Z'
        props.max_datetime = '2018-03-30T00:00:00Z'
        props.min_lat_lon = (0, 0)
        props.max_lat_lon = (2, 2)
        props.min_depth = -50
        props.max_depth = 50

        condition_manager = ParquetQueryConditionManagement('s3a://mock-bucket/base-path/', -99999, props)
        condition_manager.manage_query_props()

        expected_conditions = ["time_obj >= '2018-03-03T00:00:00Z'",
                               "time_obj <= '2018-03-30T00:00:00Z'",
                               'latitude >= 0',
                               'longitude >= 0',
                               'latitude <= 2',
                               'longitude <= 2',
                               '((depth >= -50 AND depth <= 50) OR depth == -99999)',
                               '(air_pressure IS NOT NULL)']
        expected_columns = ['air_temp',
                            'air_pressure',
                            'air_pressure_quality',
                            'time',
                            'depth',
                            'latitude',
                            'longitude']
        self.assertEqual(condition_manager.parquet_name, 's3a://mock-bucket/base-path/provider=mock_provider/project=mock_project/platform_code=123/year=2018/month=3', f'wrong parquet name')
        self.assertEqual(condition_manager.conditions, expected_conditions, f'wrong conditions')
        self.assertEqual(condition_manager.columns, expected_columns, f'wrong __columns')
        return

    def test_02(self):
        props = QueryProps()
        props.provider = 'mock_provider'
        props.project = 'mock_project'
        props.platform_code = '123'
        props.variable = ['air_pressure']
        props.quality_flag = True
        props.min_datetime = '2018-03-03T00:00:00Z'
        props.max_datetime = '2018-04-30T00:00:00Z'
        props.min_lat_lon = (0, 0)
        props.max_lat_lon = (2, 2)
        props.min_depth = -50
        props.max_depth = 50

        condition_manager = ParquetQueryConditionManagement('s3a://mock-bucket/base-path/', -99999, props)
        condition_manager.manage_query_props()

        expected_conditions = ["time_obj >= '2018-03-03T00:00:00Z'",
                               "time_obj <= '2018-04-30T00:00:00Z'",
                               'latitude >= 0',
                               'longitude >= 0',
                               'latitude <= 2',
                               'longitude <= 2',
                               '((depth >= -50 AND depth <= 50) OR depth == -99999)',
                               '(air_pressure IS NOT NULL)']
        expected_columns = []
        self.assertEqual(condition_manager.parquet_name, 's3a://mock-bucket/base-path/provider=mock_provider/project=mock_project/platform_code=123/year=2018/month={3,4}', f'wrong parquet name')
        self.assertEqual(condition_manager.conditions, expected_conditions, f'wrong conditions')
        self.assertEqual(condition_manager.columns, expected_columns, f'wrong __columns')
        return

    def test_03(self):
        props = QueryProps()
        props.provider = 'mock_provider'
        props.project = 'mock_project'
        props.platform_code = '123'
        props.variable = ['air_pressure']
        props.columns = ['air_temp']
        props.quality_flag = True
        props.min_datetime = '2018-03-03T00:00:00Z'
        props.max_datetime = '2019-03-30T00:00:00Z'
        props.min_lat_lon = (0, 0)
        props.max_lat_lon = (2, 2)
        props.min_depth = -50
        props.max_depth = 50

        condition_manager = ParquetQueryConditionManagement('s3a://mock-bucket/base-path/', -99999, props)
        condition_manager.manage_query_props()

        expected_conditions = ["time_obj >= '2018-03-03T00:00:00Z'",
                               "time_obj <= '2019-03-30T00:00:00Z'",
                               'latitude >= 0',
                               'longitude >= 0',
                               'latitude <= 2',
                               'longitude <= 2',
                               '((depth >= -50 AND depth <= 50) OR depth == -99999)',
                               '(air_pressure IS NOT NULL)']
        expected_columns = ['air_temp',
                            'air_pressure',
                            'air_pressure_quality',
                            'time',
                            'depth',
                            'latitude',
                            'longitude']
        self.assertEqual(condition_manager.parquet_name, 's3a://mock-bucket/base-path/provider=mock_provider/project=mock_project/platform_code=123/year={2018,2019}', f'wrong parquet name')
        self.assertEqual(condition_manager.conditions, expected_conditions, f'wrong conditions')
        self.assertEqual(condition_manager.columns, expected_columns, f'wrong __columns')
        return

    def test_04(self):
        props = QueryProps()
        props.provider = 'mock_provider'
        props.project = 'mock_project'
        props.variable = ['air_pressure']
        props.columns = ['air_temp']
        props.quality_flag = True
        props.min_datetime = '2018-03-03T00:00:00Z'
        props.max_datetime = '2018-03-30T00:00:00Z'
        props.min_lat_lon = (0, 0)
        props.max_lat_lon = (2, 2)
        props.min_depth = -50
        props.max_depth = 50

        condition_manager = ParquetQueryConditionManagement('s3a://mock-bucket/base-path/', -99999, props)
        condition_manager.manage_query_props()

        expected_conditions = ["time_obj >= '2018-03-03T00:00:00Z'",
                               "time_obj <= '2018-03-30T00:00:00Z'",
                               'latitude >= 0',
                               'longitude >= 0',
                               'latitude <= 2',
                               'longitude <= 2',
                               '((depth >= -50 AND depth <= 50) OR depth == -99999)',
                               '(air_pressure IS NOT NULL)']
        expected_columns = ['air_temp',
                            'air_pressure',
                            'air_pressure_quality',
                            'time',
                            'depth',
                            'latitude',
                            'longitude']
        self.assertEqual(condition_manager.parquet_name, 's3a://mock-bucket/base-path/provider=mock_provider/project=mock_project', f'wrong parquet name')
        self.assertEqual(condition_manager.conditions, expected_conditions, f'wrong conditions')
        self.assertEqual(condition_manager.columns, expected_columns, f'wrong __columns')
        return

    def test_05(self):
        props = QueryProps()
        props.project = 'mock_project'
        props.platform_code = '123'
        props.variable = ['air_pressure']
        props.columns = ['air_temp']
        props.quality_flag = True
        props.min_datetime = '2018-03-03T00:00:00Z'
        props.max_datetime = '2018-03-30T00:00:00Z'
        props.min_lat_lon = (0, 0)
        props.max_lat_lon = (2, 2)
        props.min_depth = -50
        props.max_depth = 50

        condition_manager = ParquetQueryConditionManagement('s3a://mock-bucket/base-path/', -99999, props)
        condition_manager.manage_query_props()

        expected_conditions = ["project == 'mock_project'",
                               "platform_code == '123'",
                               "time_obj >= '2018-03-03T00:00:00Z'",
                               "time_obj <= '2018-03-30T00:00:00Z'",
                               'latitude >= 0',
                               'longitude >= 0',
                               'latitude <= 2',
                               'longitude <= 2',
                               '((depth >= -50 AND depth <= 50) OR depth == -99999)',
                               '(air_pressure IS NOT NULL)']
        expected_columns = ['air_temp',
                            'air_pressure',
                            'air_pressure_quality',
                            'time',
                            'depth',
                            'latitude',
                            'longitude',
                            'provider',
                            'project']
        self.assertEqual(condition_manager.parquet_name, 's3a://mock-bucket/base-path', f'wrong parquet name')
        self.assertEqual(condition_manager.conditions, expected_conditions, f'wrong conditions')
        self.assertEqual(condition_manager.columns, expected_columns, f'wrong __columns')
        return
