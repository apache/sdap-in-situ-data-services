import unittest

from parquet_flask.io_logic.parquet_query_condition_management_v3 import ParquetQueryConditionManagementV3
from parquet_flask.io_logic.query_v2 import QueryProps


class TestGeneralUtilsV3(unittest.TestCase):
    def test_time_range_01(self):
        props = QueryProps()
        props.provider = 'mock_provider'
        props.project = 'mock_project'
        props.platform_code = ['123']
        props.variable = ['air_pressure']
        props.columns = ['air_temp']
        props.quality_flag = True
        props.min_datetime = '2018-03-03T00:00:00Z'
        props.max_datetime = '2018-03-30T00:00:00Z'
        props.min_lat_lon = (0, 0)
        props.max_lat_lon = (2, 2)
        props.min_depth = -50
        props.max_depth = 50

        condition_manager = ParquetQueryConditionManagementV3('s3a://mock-bucket/base-path/', -99999, props)
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
        expected_parquet_names = {'s3a://mock-bucket/base-path/provider=mock_provider/project=mock_project/platform_code=123/year=2018/month=3'}
        self.assertEqual(1, len(condition_manager.stringify_parquet_names()), f'wrong parquet names list length')
        for each_parquet_name in condition_manager.stringify_parquet_names():
            self.assertTrue(each_parquet_name in expected_parquet_names, f'missing in expected_parquet_names: {each_parquet_name}')
        self.assertEqual(condition_manager.conditions, expected_conditions, f'wrong conditions')
        self.assertEqual(condition_manager.columns, expected_columns, f'wrong __columns')
        return

    def test_time_range_02(self):
        props = QueryProps()
        props.provider = 'mock_provider'
        props.project = 'mock_project'
        props.platform_code = ['123']
        props.variable = ['air_pressure']
        props.columns = ['air_temp']
        props.quality_flag = True
        props.min_datetime = '2018-03-03T00:00:00Z'
        props.max_datetime = '2019-03-30T00:00:00Z'
        props.min_lat_lon = (0, 0)
        props.max_lat_lon = (2, 2)
        props.min_depth = -50
        props.max_depth = 50

        condition_manager = ParquetQueryConditionManagementV3('s3a://mock-bucket/base-path/', -99999, props)
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
        expected_parquet_names = []
        for each in range(3, 13):
            expected_parquet_names.append(f's3a://mock-bucket/base-path/provider=mock_provider/project=mock_project/platform_code=123/year=2018/month={each}')
        for each in range(1, 4):
            expected_parquet_names.append(f's3a://mock-bucket/base-path/provider=mock_provider/project=mock_project/platform_code=123/year=2019/month={each}')
        self.assertEqual(13, len(condition_manager.stringify_parquet_names()), f'wrong parquet names list length')
        for each_parquet_name in condition_manager.stringify_parquet_names():
            self.assertTrue(each_parquet_name in expected_parquet_names, f'missing in expected_parquet_names: {each_parquet_name}')
        self.assertEqual(condition_manager.conditions, expected_conditions, f'wrong conditions')
        self.assertEqual(condition_manager.columns, expected_columns, f'wrong __columns')
        return

    def test_time_range_03(self):
        props = QueryProps()
        props.provider = 'mock_provider'
        props.project = 'mock_project'
        props.platform_code = ['123']
        props.variable = ['air_pressure']
        props.columns = ['air_temp']
        props.quality_flag = True
        props.min_datetime = '2018-03-03T00:00:00Z'
        props.max_datetime = '2020-03-30T00:00:00Z'
        props.min_lat_lon = (0, 0)
        props.max_lat_lon = (2, 2)
        props.min_depth = -50
        props.max_depth = 50

        condition_manager = ParquetQueryConditionManagementV3('s3a://mock-bucket/base-path/', -99999, props)
        condition_manager.manage_query_props()

        expected_conditions = ["time_obj >= '2018-03-03T00:00:00Z'",
                               "time_obj <= '2020-03-30T00:00:00Z'",
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
        expected_parquet_names = [f's3a://mock-bucket/base-path/provider=mock_provider/project=mock_project/platform_code=123/year=2019']
        for each in range(3, 13):
            expected_parquet_names.append(f's3a://mock-bucket/base-path/provider=mock_provider/project=mock_project/platform_code=123/year=2018/month={each}')
        for each in range(1, 4):
            expected_parquet_names.append(f's3a://mock-bucket/base-path/provider=mock_provider/project=mock_project/platform_code=123/year=2020/month={each}')
        self.assertEqual(14, len(condition_manager.stringify_parquet_names()), f'wrong parquet names list length')
        for each_parquet_name in condition_manager.stringify_parquet_names():
            self.assertTrue(each_parquet_name in expected_parquet_names, f'missing in expected_parquet_names: {each_parquet_name}')
        self.assertEqual(condition_manager.conditions, expected_conditions, f'wrong conditions')
        self.assertEqual(condition_manager.columns, expected_columns, f'wrong __columns')
        return

    def test_time_range_04(self):
        props = QueryProps()
        props.provider = 'mock_provider'
        props.project = 'mock_project'
        props.platform_code = ['123']
        props.variable = ['air_pressure']
        props.columns = ['air_temp']
        props.quality_flag = True
        props.min_datetime = '2018-01-03T00:00:00Z'
        props.max_datetime = '2021-03-30T00:00:00Z'
        props.min_lat_lon = (0, 0)
        props.max_lat_lon = (2, 2)
        props.min_depth = -50
        props.max_depth = 50

        condition_manager = ParquetQueryConditionManagementV3('s3a://mock-bucket/base-path/', -99999, props)
        condition_manager.manage_query_props()

        expected_conditions = ["time_obj >= '2018-01-03T00:00:00Z'",
                               "time_obj <= '2021-03-30T00:00:00Z'",
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
        expected_parquet_names = [
            f's3a://mock-bucket/base-path/provider=mock_provider/project=mock_project/platform_code=123/year=2018',
            f's3a://mock-bucket/base-path/provider=mock_provider/project=mock_project/platform_code=123/year=2019',
            f's3a://mock-bucket/base-path/provider=mock_provider/project=mock_project/platform_code=123/year=2020',
        ]
        for each in range(1, 4):
            expected_parquet_names.append(f's3a://mock-bucket/base-path/provider=mock_provider/project=mock_project/platform_code=123/year=2021/month={each}')
        self.assertEqual(6, len(condition_manager.stringify_parquet_names()), f'wrong parquet names list length')
        for each_parquet_name in condition_manager.stringify_parquet_names():
            self.assertTrue(each_parquet_name in expected_parquet_names, f'missing in expected_parquet_names: {each_parquet_name}')
        self.assertEqual(condition_manager.conditions, expected_conditions, f'wrong conditions')
        self.assertEqual(condition_manager.columns, expected_columns, f'wrong __columns')
        return

    def test_time_range_04_01(self):
        props = QueryProps()
        props.provider = 'mock_provider'
        props.project = 'mock_project'
        props.platform_code = ['123', '234', '456']
        props.variable = ['air_pressure']
        props.columns = ['air_temp']
        props.quality_flag = True
        props.min_datetime = '2018-01-03T00:00:00Z'
        props.max_datetime = '2021-03-30T00:00:00Z'
        props.min_lat_lon = (0, 0)
        props.max_lat_lon = (2, 2)
        props.min_depth = -50
        props.max_depth = 50

        condition_manager = ParquetQueryConditionManagementV3('s3a://mock-bucket/base-path/', -99999, props)
        condition_manager.manage_query_props()

        expected_conditions = ["time_obj >= '2018-01-03T00:00:00Z'",
                               "time_obj <= '2021-03-30T00:00:00Z'",
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
        expected_parquet_names = [
            f's3a://mock-bucket/base-path/provider=mock_provider/project=mock_project/platform_code=123/year=2018',
            f's3a://mock-bucket/base-path/provider=mock_provider/project=mock_project/platform_code=234/year=2018',
            f's3a://mock-bucket/base-path/provider=mock_provider/project=mock_project/platform_code=456/year=2018',
            f's3a://mock-bucket/base-path/provider=mock_provider/project=mock_project/platform_code=123/year=2019',
            f's3a://mock-bucket/base-path/provider=mock_provider/project=mock_project/platform_code=234/year=2019',
            f's3a://mock-bucket/base-path/provider=mock_provider/project=mock_project/platform_code=456/year=2019',
            f's3a://mock-bucket/base-path/provider=mock_provider/project=mock_project/platform_code=123/year=2020',
            f's3a://mock-bucket/base-path/provider=mock_provider/project=mock_project/platform_code=234/year=2020',
            f's3a://mock-bucket/base-path/provider=mock_provider/project=mock_project/platform_code=456/year=2020',
        ]
        for each in range(1, 4):
            expected_parquet_names.append(f's3a://mock-bucket/base-path/provider=mock_provider/project=mock_project/platform_code=123/year=2021/month={each}')
            expected_parquet_names.append(f's3a://mock-bucket/base-path/provider=mock_provider/project=mock_project/platform_code=234/year=2021/month={each}')
            expected_parquet_names.append(f's3a://mock-bucket/base-path/provider=mock_provider/project=mock_project/platform_code=456/year=2021/month={each}')
        self.assertEqual(18, len(condition_manager.stringify_parquet_names()), f'wrong parquet names list length')
        for each_parquet_name in condition_manager.stringify_parquet_names():
            self.assertTrue(each_parquet_name in expected_parquet_names, f'missing in expected_parquet_names: {each_parquet_name}')
        self.assertEqual(condition_manager.conditions, expected_conditions, f'wrong conditions')
        self.assertEqual(condition_manager.columns, expected_columns, f'wrong __columns')
        return

    def test_time_range_05(self):
        props = QueryProps()
        props.provider = 'mock_provider'
        props.project = 'mock_project'
        props.platform_code = ['123']
        props.variable = ['air_pressure']
        props.columns = ['air_temp']
        props.quality_flag = True
        props.min_datetime = '2018-03-03T00:00:00Z'
        props.max_datetime = '2019-12-30T00:00:00Z'
        props.min_lat_lon = (0, 0)
        props.max_lat_lon = (2, 2)
        props.min_depth = -50
        props.max_depth = 50

        condition_manager = ParquetQueryConditionManagementV3('s3a://mock-bucket/base-path/', -99999, props)
        condition_manager.manage_query_props()

        expected_conditions = ["time_obj >= '2018-03-03T00:00:00Z'",
                               "time_obj <= '2019-12-30T00:00:00Z'",
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
        expected_parquet_names = [f's3a://mock-bucket/base-path/provider=mock_provider/project=mock_project/platform_code=123/year=2019']
        for each in range(3, 13):
            expected_parquet_names.append(f's3a://mock-bucket/base-path/provider=mock_provider/project=mock_project/platform_code=123/year=2018/month={each}')
        self.assertEqual(11, len(condition_manager.stringify_parquet_names()), f'wrong parquet names list length')
        for each_parquet_name in condition_manager.stringify_parquet_names():
            self.assertTrue(each_parquet_name in expected_parquet_names, f'missing in expected_parquet_names: {each_parquet_name}')
        self.assertEqual(condition_manager.conditions, expected_conditions, f'wrong conditions')
        self.assertEqual(condition_manager.columns, expected_columns, f'wrong __columns')
        return

    def test_time_range_05_01(self):
        props = QueryProps()
        props.provider = 'mock_provider'
        props.project = 'mock_project'
        props.platform_code = ['123', '234']
        props.variable = ['air_pressure']
        props.columns = ['air_temp']
        props.quality_flag = True
        props.min_datetime = '2018-03-03T00:00:00Z'
        props.max_datetime = '2019-12-30T00:00:00Z'
        props.min_lat_lon = (0, 0)
        props.max_lat_lon = (2, 2)
        props.min_depth = -50
        props.max_depth = 50

        condition_manager = ParquetQueryConditionManagementV3('s3a://mock-bucket/base-path/', -99999, props)
        condition_manager.manage_query_props()

        expected_conditions = ["time_obj >= '2018-03-03T00:00:00Z'",
                               "time_obj <= '2019-12-30T00:00:00Z'",
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
        expected_parquet_names = [f's3a://mock-bucket/base-path/provider=mock_provider/project=mock_project/platform_code=123/year=2019',
                                  f's3a://mock-bucket/base-path/provider=mock_provider/project=mock_project/platform_code=234/year=2019'
                                  ]
        for each in range(3, 13):
            expected_parquet_names.append(f's3a://mock-bucket/base-path/provider=mock_provider/project=mock_project/platform_code=123/year=2018/month={each}')
            expected_parquet_names.append(f's3a://mock-bucket/base-path/provider=mock_provider/project=mock_project/platform_code=234/year=2018/month={each}')
        self.assertEqual(22, len(condition_manager.stringify_parquet_names()), f'wrong parquet names list length')
        for each_parquet_name in condition_manager.stringify_parquet_names():
            self.assertTrue(each_parquet_name in expected_parquet_names, f'missing in expected_parquet_names: {each_parquet_name}')
        self.assertEqual(condition_manager.conditions, expected_conditions, f'wrong conditions')
        self.assertEqual(condition_manager.columns, expected_columns, f'wrong __columns')
        return

    def test_time_range_06(self):
        props = QueryProps()
        props.provider = 'mock_provider'
        props.platform_code = ['123', '234', '456']
        props.variable = ['air_pressure']
        props.columns = ['air_temp']
        props.quality_flag = True
        props.min_datetime = '2018-01-03T00:00:00Z'
        props.max_datetime = '2021-03-30T00:00:00Z'
        props.min_lat_lon = (0, 0)
        props.max_lat_lon = (2, 2)
        props.min_depth = -50
        props.max_depth = 50

        condition_manager = ParquetQueryConditionManagementV3('s3a://mock-bucket/base-path/', -99999, props)
        condition_manager.manage_query_props()

        expected_conditions = ["platform_code in ('123','234','456')",
                               "time_obj >= '2018-01-03T00:00:00Z'",
                               "time_obj <= '2021-03-30T00:00:00Z'",
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
                            'project',
                            ]
        expected_parquet_names = [
            f's3a://mock-bucket/base-path/provider=mock_provider',
        ]
        self.assertEqual(1, len(condition_manager.stringify_parquet_names()), f'wrong parquet names list length')
        for each_parquet_name in condition_manager.stringify_parquet_names():
            self.assertTrue(each_parquet_name in expected_parquet_names, f'missing in expected_parquet_names: {each_parquet_name}')
        self.assertEqual(condition_manager.conditions, expected_conditions, f'wrong conditions')
        self.assertEqual(condition_manager.columns, expected_columns, f'wrong __columns')
        return
