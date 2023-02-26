STRUCTURE_CONFIG = {
    "type": "object",
    "required": ["partitioning_columns", "non_data_columns", "derived_columns"],
    "partitioning_columns": {"type": "array", "items": {"type": "string"}},
    "non_data_columns": {"type": "array", "items": {"type": "string"}},
    "derived_columns": {
        "type": "object",
        "required": [],
        "properties": {}
    }

}


class FileStructureSetting:
    def __init__(self, data_json_schema: dict, structure_config: dict):
        self.__data_json_schema = data_json_schema
        self.__structure_config = structure_config

    def get_data_array_key(self):
        return self.__structure_config['data_array_key']

    def get_derived_columns(self):
        return self.__structure_config['derived_columns']

    def get_partitioning_columns(self):
        return self.__structure_config['partitioning_columns']

