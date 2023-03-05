import json

from parquet_flask.insitu.file_structure_setting import FileStructureSetting


class GetQueryTransformer:
    def __init__(self, file_struct_setting: FileStructureSetting):
        self.__file_struct_setting = file_struct_setting

    def __transform_value(self, str_value: str, definition: dict):
        if 'type' not in definition:
            raise ValueError(f'missing type in definition: {definition}')
        if definition['type'] == 'string':
            return str_value.strip()
        if definition['type'] == 'number':
            return float(str_value)
        if definition['type'] == 'integer':
            return int(str_value)
        if definition['type'] == 'bool':
            return bool(str_value)
        if definition['type'] == 'array':
            temp_arr = [k.strip() for k in str_value.strip().split(',')]
            if 'items' not in definition:
                return temp_arr
            temp_arr = [self.__transform_value(k, definition['items']) for k in temp_arr]
            return temp_arr
        raise ValueError(f'invalid definition: {definition}')

    def __generate_single_dsl(self, each_condition: dict, input_value: object, value_type: str):
        repr_value = str(input_value)
        if value_type == 'string':
            repr_value = f'"{repr_value}"'
        return json.loads(json.dumps(each_condition).replace('"repr_value"', repr_value))

    def __generate_dsl_stmt(self, dsl_terms: list, input_value: object, value_type: str):
        es_terms = []
        if not isinstance(input_value, list):
            for each_condition in dsl_terms:
                es_terms.append(self.__generate_single_dsl(each_condition, input_value, value_type))
            return es_terms
        for each_condition in dsl_terms:
            es_terms.append({
                'bool': {
                    'should': [self.__generate_single_dsl(each_condition, k, value_type) for k in input_value]
                }
            })
        return es_terms

    def generate_dsl_conditions(self, query_object: dict):
        es_terms = []
        for k, v in self.__file_struct_setting.query_input_metadata_search_instructions().items():
            if k not in query_object:
                continue
            input_value = query_object[k]
            if isinstance(v, list):  # different conditions for different values in array
                if not isinstance(input_value, list):
                    raise ValueError(f'input_value is not list for list conditions: {v} v. {input_value}')
                if len(v) != len(input_value):
                    raise ValueError(f'mismatched length: {v} v. {input_value}')
                for each_pair in zip(v, input_value):
                    temp_v = each_pair[0]
                    es_terms.extend(self.__generate_dsl_stmt(temp_v['dsl_terms'], each_pair[1], temp_v['type']))
                continue
            es_terms.extend(self.__generate_dsl_stmt(v['dsl_terms'], input_value, v['type']))
        return es_terms

    def transform_param(self, query_param_dict: dict):
        transformed_dict = {
            k: self.__transform_value(query_param_dict[k], v) for k, v in self.__file_struct_setting.get_query_input_transformer_schema()['properties'].items()
            if k in query_param_dict
        }
        return transformed_dict
