from parquet_flask.utils.parallel_json_validator import ParallelJsonValidator


STRUCTURE_CONFIG = {
    "type": "object",
    "required": ["data_schema_config", "parquet_ingestion_config", "parquet_file_metadata_extraction_config",
                 "query_statistics_instructions_config", "data_query_config"],
    "properties": {
        "data_schema_config": {
            "type": "object",
            "required": ["data_array_key", "data_dict_key", "has_data_quality", "quality_key_postfix"],
            "properties": {
                "data_array_key": {"type": "string"},
                "data_dict_key": {"type": "string"},
                "has_data_quality": {"type": "boolean"},
                "quality_key_postfix": {"type": "string"}
            }
        },
        "parquet_ingestion_config": {
            "type": "object",
            "required": ["file_metadata_keys", "time_columns", "partitioning_columns", "non_data_columns",
                         "derived_columns"],
            "properties": {
                "file_metadata_keys": {
                    "type": "array",
                    "items": {"type": "string"}
                },
                "time_columns": {
                    "type": "array",
                    "items": {"type": "string"}
                },
                "partitioning_columns": {
                    "type": "array",
                    "items": {"type": "string"}
                },
                "non_data_columns": {
                    "type": "array",
                    "items": {"type": "string"}
                },
                "derived_columns": {
                    "type": "object",
                    "required": [],
                    "properties": {}
                }

            }
        },
        "parquet_file_metadata_extraction_config": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["stat_type", "output_name"],
                "properties": {
                    "output_name": {"type": "string"},
                    "stat_type": {
                        "type": "string",
                        "enum": ["minmax", "data_type_record_count", "record_count"]
                    },
                    "special_data_type": {
                        "type": "string",
                        "enum": ["timestamp"]
                    },
                    "column": {"type": "string"},
                    "columns": {"type": "array", "items": {"type": "string"}}
                }
            }
        },
        "query_statistics_instructions_config": {
            "type": "object",
            "required": ["group_by", "stats", "data_stats"],
            "properties": {
                "group_by": {"type": "array", "items": {"type": "string"}},
                "data_stats": {
                    "type": "object",
                    "required": ["is_included", "stats", "data_prefix"],
                    "properties": {
                        "is_included": {"type": "boolean"},
                        "stats": {"type": "string"},
                        "data_prefix": {"type": "string"},
                    }
                },
                "stats": {
                    "type": "object",
                    "required": ["min", "max", "sum"],
                    "properties": {
                        "min": {"type": "array", "items": {"type": "string"}},
                        "max": {"type": "array", "items": {"type": "string"}},
                        "sum": {"type": "array", "items": {"type": "string"}},
                    }
                },
            }
        },
        "data_query_config": {
            "type": "object",
            "required": ["input_parameter_transformer_schema", "metadata_search_instruction_config",
                         "sort_mechanism_config", "column_filters_config", "parquet_conditions_config", "statics_es_index_schema"],
            "properties": {
                "input_parameter_transformer_schema": {"type": "object"},
                "metadata_search_instruction_config": {
                    "type": "object"
                },
                "sort_mechanism_config": {
                    "type": "object",
                    "required": ["sorting_columns", "page_size_key", "pagination_marker_key", "pagination_marker_time"],
                    "properties": {
                        "page_size_key": {"type": "string"},
                        "pagination_marker_key": {"type": "string"},
                        "pagination_marker_time": {"type": "string"},
                        "sorting_columns": {"type": "array", "items": {"type": "string"}}
                    }
                },
                "column_filters_config": {
                    "type": "object",
                    "required": ["default_columns", "mandatory_column_filter_key", "additional_column_filter_key",
                                 "removing_columns"],
                    "properties": {
                        "removing_columns": {"type": "array", "items": {"type": "string"}},
                        "default_columns": {"type": "array", "items": {"type": "string"}},
                        "mandatory_column_filter_key": {"type": "string"},
                        "additional_column_filter_key": {"type": "string"},
                    }
                },
                "parquet_conditions_config": {"type": "object"},
                "statics_es_index_schema": {"type": "object"}
            }
        }
    }
}


class FileStructureSetting:
    def __init__(self, data_json_schema: dict, structure_config: dict):
        self.__data_json_schema = data_json_schema
        self.__structure_config = structure_config
        result, message = ParallelJsonValidator().load_schema(STRUCTURE_CONFIG).validate_single_json(self.__structure_config)
        if result is False:
            raise ValueError(f'invalid structure_config: {message}')

    def get_quality_postfix(self):
        return self.__structure_config['data_schema_config']['quality_key_postfix']

    def get_data_json_schema(self):
        return self.__data_json_schema

    def get_data_column_definitions(self):
        if 'definitions' not in self.__data_json_schema:
            raise ValueError(f'missing definitions in in_situ_schema: {self.__data_json_schema}')
        base_defs = self.__data_json_schema['definitions']
        data_dict_key = self.__structure_config['data_schema_config']['data_dict_key']
        if data_dict_key not in base_defs:
            raise ValueError(f'missing {data_dict_key} in in_situ_schema["definitions"]: {base_defs}')
        obs_defs = base_defs[data_dict_key]
        if 'properties' not in obs_defs:
            raise ValueError(f'missing properties in in_situ_schema["definitions"]["{data_dict_key}"]: {obs_defs}')
        return obs_defs['properties']

    def get_data_columns(self):
        non_data_columns = self.__structure_config['parquet_ingestion_config']['non_data_columns']
        data_column_names = [k for k in self.get_data_column_definitions().keys() if k not in non_data_columns and not k.endswith(self.__structure_config['data_schema_config']['quality_key_postfix'])]
        return data_column_names

    def get_es_index_schema_parquet_stats(self):
        return self.__structure_config['data_query_config']['statics_es_index_schema']

    def get_query_input_transformer_schema(self):
        return self.__structure_config['data_query_config']['input_parameter_transformer_schema']

    def query_input_metadata_search_instructions(self):
        return self.__structure_config['data_query_config']['metadata_search_instruction_config']

    def get_file_metadata_keys(self):
        return self.__structure_config['parquet_ingestion_config']['file_metadata_keys']

    def get_data_array_key(self):
        return self.__structure_config['data_schema_config']['data_array_key']

    def get_query_input_column_filters(self):
        return self.__structure_config['data_query_config']['column_filters_config']

    def get_query_sort_mechanism(self):
        return self.__structure_config['data_query_config']['sort_mechanism_config']

    def get_query_input_parquet_conditions(self):
        return self.__structure_config['data_query_config']['parquet_conditions_config']

    def get_derived_columns(self):
        return self.__structure_config['parquet_ingestion_config']['derived_columns']

    def get_parquet_file_data_stats_config(self):
        return self.__structure_config['parquet_file_metadata_extraction_config']

    def get_non_data_columns(self):
        return self.__structure_config['parquet_ingestion_config']['non_data_columns']

    def get_query_statistics_instructions(self):
        return self.__structure_config['query_statistics_instructions_config']

    def get_partitioning_columns(self):
        return self.__structure_config['parquet_ingestion_config']['partitioning_columns']
