import json
from unittest import TestCase

from parquet_flask.insitu.file_structure_setting import FileStructureSetting
from parquet_flask.insitu.get_query_transformer import GetQueryTransformer
from parquet_flask.utils.file_utils import FileUtils


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
            'startTime': '2023-01-01T00:00:00',
            'endTime': '2023-01-01T00:00:00',
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
            'startTime': '2023-01-01T00:00:00',
            'endTime': '2023-01-01T00:00:00',
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
            'startTime': '2023-01-01T00:00:00',
            'endTime': '2023-01-01T00:00:00',
            'minDepth': '-120.12',
            'maxDepth': '-10.102',
            'bbox': '-100.1, -50.2,22.3, 2.4',
        }
        transformed_query_param_dict = transformer.transform_param(query_param_dict=query_param_dict)
        mock_transformed_query_param_dict = {
            'provider': 'sample_provider',
            'project': 'sample_project',
            'platform': '1,2,3,4'.split(','),
            'startTime': '2023-01-01T00:00:00',
            'endTime': '2023-01-01T00:00:00',
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
            'startTime': '2023-01-01T00:00:00',
            'endTime': '2023-01-01T00:00:00',
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
                        "gte": "2023-01-01T00:00:00"
                    }
                }
            },
            {
                "range": {
                    "min_datetime": {
                        "lte": "2023-01-01T00:00:00"
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
            'startTime': '2023-01-01T00:00:00',
            'endTime': '2023-01-01T00:00:00',
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
                        "gte": "2023-01-01T00:00:00"
                    }
                }
            },
            {
                "range": {
                    "min_datetime": {
                        "lte": "2023-01-01T00:00:00"
                    }
                }
            },
        ]
        self.assertEqual(dsl_dict, mock_dsl_dict)
        return
