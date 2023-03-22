from parquet_flask.utils.parallel_json_validator import ParallelJsonValidator

STRUCTURE_CONFIG = {
    "type": "object",
    "required": ["partitioning_columns", "non_data_columns", "derived_columns", "file_metadata_keys", "data_array_key",
                 "data_stats", "query_input_metadata_search_instructions", "es_index_schema_parquet_stats",
                 "query_statistics_instructions",
                 "query_input_parquet_conditions",
                 "query_input_transformer_schema"],
    "properties": {
        "data_array_key": {"type": "string"},
        "partitioning_columns": {"type": "array", "items": {"type": "string"}},
        "non_data_columns": {"type": "array", "items": {"type": "string"}},
        "metadata_keys": {"type": "array", "items": {"type": "string"}},
        "derived_columns": {
            "type": "object",
            "required": [],
            "properties": {}
        },
        "query_input_parquet_conditions": {"type": "object"},
        "query_input_transformer_schema": {"type": "object"},
        "es_index_schema_parquet_stats": {"type": "object"},
        "query_input_metadata_search_instructions": {
            "type": "object"
        },
        "query_statistics_instructions": {
            "type": "object",
            "required": ["group_by", "stats", "data_stats"],
            "properties": {
                "group_by": {"type": "array", "items": {"type": "string"}},
                "include_data_stats": {"type": "boolean"},
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
        "data_stats": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["stat_type", "output_name"],
                "properties": {
                    "output_name": {"type": "string"},
                    "stat_type": {
                        "type": "string",
                        "enum": ["minmax", "count", "record_count"]
                    },
                    "special_data_type": {
                        "type": "string",
                        "enum": ["timestamp"]
                    },
                    "column": {"type": "string"},
                    "columns": {"type": "array", "items": {"type": "string"}}
                }
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

    def get_es_index_schema_parquet_stats(self):
        return self.__structure_config['es_index_schema_parquet_stats']

    def get_query_input_transformer_schema(self):
        return self.__structure_config['query_input_transformer_schema']
    def query_input_metadata_search_instructions(self):
        return self.__structure_config['query_input_metadata_search_instructions']

    def get_file_metadata_keys(self):
        return self.__structure_config['file_metadata_keys']

    def get_data_array_key(self):
        return self.__structure_config['data_array_key']

    def get_query_input_parquet_conditions(self):
        return self.__structure_config['query_input_parquet_conditions']

    def get_derived_columns(self):
        return self.__structure_config['derived_columns']

    def get_data_stats_config(self):
        return self.__structure_config['data_stats']

    def get_non_data_columns(self):
        return self.__structure_config['non_data_columns']

    def get_query_statistics_instructions(self):
        return self.__structure_config['query_statistics_instructions']

    def get_partitioning_columns(self):
        return self.__structure_config['partitioning_columns']

