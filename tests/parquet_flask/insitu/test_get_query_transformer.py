import json
from unittest import TestCase

from parquet_flask.insitu.file_structure_setting import FileStructureSetting
from parquet_flask.insitu.get_query_transformer import GetQueryTransformer
from parquet_flask.utils.file_utils import FileUtils
from parquet_flask.utils.time_utils import TimeUtils


class TestGetQueryTransformer(TestCase):
    def test_01_transform_param(self):
        data_json_schema = FileUtils.read_json('/Users/wphyo/Projects/access/parquet_test_1/in_situ_schema.json')
        structure_config = FileUtils.read_json('/Users/wphyo/Projects/access/parquet_test_1/insitu.file.structure.config.json')
        file_struct_setting = FileStructureSetting(data_json_schema=data_json_schema, structure_config=structure_config)
        transformer = GetQueryTransformer(file_struct_setting)
        query_param_dict = {
            'provider': 'sample_provider',
            'project': 'sample_project',
            'platform': '1,2,3,4',
            'startTime': '2023-01-01T00:00:00Z',
            'endTime': '2023-01-01T00:00:00Z',
            'minDepth': '-120.12',
            'maxDepth': '-10.102',
            'bbox': '-100.1, -50.2, 22.3, 2.4',
            'variable': 'a1,a2,a3',
            'columns': 'c1, c2, c3'
        }
        transformed_query_param_dict = transformer.transform_param(query_param_dict=query_param_dict)
        mock_transformed_query_param_dict = {
            'provider': 'sample_provider',
            'project': 'sample_project',
            'platform': '1,2,3,4'.split(','),
            'startTime': '2023-01-01T00:00:00Z',
            'endTime': '2023-01-01T00:00:00Z',
            'minDepth': -120.12,
            'maxDepth': -10.102,
            'bbox': [-100.1, -50.2, 22.3, 2.4],
            'variable': 'a1,a2,a3'.split(','),
            'columns': 'c1,c2,c3'.split(',')
        }
        self.assertEqual(transformed_query_param_dict, mock_transformed_query_param_dict)
        return

    def test_02_transform_param(self):
        data_json_schema = FileUtils.read_json('/Users/wphyo/Projects/access/parquet_test_1/in_situ_schema.json')
        structure_config = FileUtils.read_json('/Users/wphyo/Projects/access/parquet_test_1/insitu.file.structure.config.json')
        file_struct_setting = FileStructureSetting(data_json_schema=data_json_schema, structure_config=structure_config)
        transformer = GetQueryTransformer(file_struct_setting)
        query_param_dict = {
            'provider': 'sample_provider',
            'project': 'sample_project',
            'platform': '1,2, 3,4',
            'startTime': '2023-01-01T00:00:00Z',
            'endTime': '2023-01-01T00:00:00Z',
            'minDepth': '-120.12',
            'maxDepth': '-10.102',
            'bbox': '-100.1, -50.2,22.3, 2.4',
        }
        transformed_query_param_dict = transformer.transform_param(query_param_dict=query_param_dict)
        mock_transformed_query_param_dict = {
            'provider': 'sample_provider',
            'project': 'sample_project',
            'platform': '1,2,3,4'.split(','),
            'startTime': '2023-01-01T00:00:00Z',
            'endTime': '2023-01-01T00:00:00Z',
            'minDepth': -120.12,
            'maxDepth': -10.102,
            'bbox': [-100.1, -50.2, 22.3, 2.4],
        }
        self.assertEqual(transformed_query_param_dict, mock_transformed_query_param_dict)
        return

    def test_03_generate_dsl_conditions(self):
        data_json_schema = FileUtils.read_json('/Users/wphyo/Projects/access/parquet_test_1/in_situ_schema.json')
        structure_config = FileUtils.read_json('/Users/wphyo/Projects/access/parquet_test_1/insitu.file.structure.config.json')
        file_struct_setting = FileStructureSetting(data_json_schema=data_json_schema, structure_config=structure_config)
        transformer = GetQueryTransformer(file_struct_setting)
        query_param_dict = {
            'provider': 'sample_provider',
            'project': 'sample_project',
            'platform': '1,2,3,4'.split(','),
            'startTime': '2023-01-01T00:00:00Z',
            'endTime': '2023-01-01T00:00:00Z',
            'minDepth': -120.12,
            'maxDepth': -10.102,
            'bbox': [-100.1, -50.2, 22.3, 2.4],
            'variable': 'a1,a2,a3'.split(','),
            'columns': 'c1,c2,c3'.split(',')
        }
        dsl_dict = transformer.generate_dsl_conditions(query_param_dict)
        mock_dsl_dict = [
            {
                "term": {
                    "provider": "sample_provider"
                }
            },
            {
                "term": {
                    "project": "sample_project"
                }
            },
            {
                "bool": {
                    "should": [
                        {
                            "term": {
                                "platform_code": "1"
                            }
                        },
                        {
                            "term": {
                                "platform_code": "2"
                            }
                        },
                        {
                            "term": {
                                "platform_code": "3"
                            }
                        },
                        {
                            "term": {
                                "platform_code": "4"
                            }
                        }
                    ]
                }
            },
            {
                "range": {
                    "max_depth": {
                        "gte": -120.12
                    }
                }
            },
            {
                "range": {
                    "min_depth": {
                        "lte": -10.102
                    }
                }
            },
            {
                "range": {
                    "max_datetime": {
                        "gte": TimeUtils.get_datetime_obj("2023-01-01T00:00:00Z").timestamp()
                    }
                }
            },
            {
                "range": {
                    "min_datetime": {
                        "lte": TimeUtils.get_datetime_obj("2023-01-01T00:00:00Z").timestamp()
                    }
                }
            },
            {
                "range": {
                    "max_lat": {
                        "gte": -100.1
                    }
                }
            },
            {
                "range": {
                    "max_lon": {
                        "gte": -50.2
                    }
                }
            },
            {
                "range": {
                    "min_lat": {
                        "lte": 22.3
                    }
                }
            },
            {
                "range": {
                    "min_lat": {
                        "lte": 2.4
                    }
                }
            }
        ]
        self.assertEqual(dsl_dict, mock_dsl_dict)
        return

    def test_04_generate_dsl_conditions(self):
        data_json_schema = FileUtils.read_json('/Users/wphyo/Projects/access/parquet_test_1/in_situ_schema.json')
        structure_config = FileUtils.read_json('/Users/wphyo/Projects/access/parquet_test_1/insitu.file.structure.config.json')
        file_struct_setting = FileStructureSetting(data_json_schema=data_json_schema, structure_config=structure_config)
        transformer = GetQueryTransformer(file_struct_setting)
        query_param_dict = {
            'provider': 'sample_provider',
            'project': 'sample_project',
            'platform': '5,6,7,8'.split(','),
            'startTime': '2023-01-01T00:00:00Z',
            'endTime': '2023-01-01T00:00:00Z',
            'variable': 'a1,a2,a3'.split(','),
            'columns': 'c1,c2,c3'.split(',')
        }
        dsl_dict = transformer.generate_dsl_conditions(query_param_dict)
        mock_dsl_dict = [
            {
                "term": {
                    "provider": "sample_provider"
                }
            },
            {
                "term": {
                    "project": "sample_project"
                }
            },
            {
                "bool": {
                    "should": [
                        {
                            "term": {
                                "platform_code": "5"
                            }
                        },
                        {
                            "term": {
                                "platform_code": "6"
                            }
                        },
                        {
                            "term": {
                                "platform_code": "7"
                            }
                        },
                        {
                            "term": {
                                "platform_code": "8"
                            }
                        }
                    ]
                }
            },
            {
                "range": {
                    "max_datetime": {
                        "gte": TimeUtils.get_datetime_obj("2023-01-01T00:00:00Z").timestamp()
                    }
                }
            },
            {
                "range": {
                    "min_datetime": {
                        "lte": TimeUtils.get_datetime_obj("2023-01-01T00:00:00Z").timestamp()
                    }
                }
            },
        ]
        self.assertEqual(dsl_dict, mock_dsl_dict)
        return

    def test_05_generate_parquet_conditions(self):
        data_json_schema = FileUtils.read_json('/Users/wphyo/Projects/access/parquet_test_1/in_situ_schema.json')
        structure_config = FileUtils.read_json('/Users/wphyo/Projects/access/parquet_test_1/insitu.file.structure.config.json')
        file_struct_setting = FileStructureSetting(data_json_schema=data_json_schema, structure_config=structure_config)
        transformer = GetQueryTransformer(file_struct_setting)
        query_param_dict = {
            'provider': 'sample_provider',
            'project': 'sample_project',
            'platform': '1,2,3,4',
            'startTime': '2023-01-01T00:00:00Z',
            'endTime': '2023-01-01T00:00:00Z',
            'minDepth': '-120.12',
            'maxDepth': '-10.102',
            'bbox': '-100.1, -50.2, 22.3, 2.4',
            'variable': 'a1,a2,a3',
            'columns': 'c1, c2, c3'
        }
        transformed_query_param_dict = transformer.transform_param(query_param_dict=query_param_dict)
        parquet_conditions = transformer.generate_parquet_conditions(transformed_query_param_dict)
        mocked_partitions = [" time_obj >= '2023-01-01T00:00:00Z' ",
                             " time_obj <= '2023-01-01T00:00:00Z' ",
                             '(  lat >= -100.1  AND  lon >= -50.2  AND  lat <= 22.3  AND  lon <= 2.4  )',
                             ' depth >= -120.12 ', ' depth <= -10.102 ',
                             '(  a1 IS NOT NULL  OR  a2 IS NOT NULL  OR  a3 IS NOT NULL  )']
        self.assertEqual(json.dumps(mocked_partitions, sort_keys=True), json.dumps(parquet_conditions, sort_keys=True), f'wrong parquet_conditions: {parquet_conditions}')
        print(parquet_conditions)
        return

    def test_generate_retrieving_columns(self):
        data_json_schema = FileUtils.read_json('/Users/wphyo/Projects/access/parquet_test_1/in_situ_schema.json')
        structure_config = FileUtils.read_json(
            '/Users/wphyo/Projects/access/parquet_test_1/insitu.file.structure.config.json')
        file_struct_setting = FileStructureSetting(data_json_schema=data_json_schema, structure_config=structure_config)
        transformer = GetQueryTransformer(file_struct_setting)
        query_param_dict = {
            'provider': 'sample_provider',
            'project': 'sample_project',
            'platform': '1,2,3,4',
            'startTime': '2023-01-01T00:00:00Z',
            'endTime': '2023-01-01T00:00:00Z',
            'minDepth': '-120.12',
            'maxDepth': '-10.102',
            'bbox': '-100.1, -50.2, 22.3, 2.4',
            'variable': 'a1,a2,a3',
            'columns': 'c1, c2, c3,lat'
        }
        transformed_query_param_dict = transformer.transform_param(query_param_dict=query_param_dict)
        columns = transformer.generate_retrieving_columns(transformed_query_param_dict)
        mocked_columns = ["a1", 'a2', 'a3', "time", "lat", "lon", "depth", 'c1', 'c2', 'c3']
        self.assertEqual(sorted(mocked_columns), sorted(columns), f'wrong generated columns')
        return

