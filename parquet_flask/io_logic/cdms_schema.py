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
from parquet_flask.insitu.file_structure_setting import FileStructureSetting
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, MapType, LongType, TimestampType, \
    IntegerType


class CdmsSchema:
    def __get_json_datatype(self, datetype_name: str, datatype_def: dict):
        if 'type' in datatype_def:
            temp_type = datatype_def['type']
            if isinstance(temp_type, str):
                return temp_type
            if isinstance(temp_type, list):
                return temp_type[0]
            raise ValueError(f'unknown datatype: {datetype_name}: {datatype_def}')
        if '$ref' in datatype_def:
            return self.__get_json_datatype(datetype_name, self.__get_datatype_def_from_ref(datatype_def['$ref']))
        raise ValueError(f'unknown datatype: {datetype_name}: {datatype_def}')

    def __init__(self, file_structure_setting: FileStructureSetting = None):
        self.__file_structure_setting = file_structure_setting
        # TODO : abstraction - integer & long has some types?. object is mapped to Map(String, String).

        self.__json_to_pandas_data_type = {
            'number': 'double',
            'long': 'int64',
            'integer': 'int64',
            'string': 'object',
            'object': 'object',
        }
        self.__json_to_spark_data_types = {
            'number': DoubleType(),
            'long': LongType(),
            'integer': LongType(),
            'string': StringType(),
            'object': MapType(StringType(), StringType()),
        }
        self.__derived_spark_data_types = {
            'time': TimestampType(),
            'year': IntegerType(),
            'month': IntegerType(),
            'literal': StringType(),
            'column': StringType(),
            'insitu_geo_spatial': StringType(),
        }

        self.__independent_definitions = {}

    def __get_datatype_def_from_ref(self, def_path: str):
        if def_path in self.__independent_definitions:
            return self.__independent_definitions[def_path]
        def_paths = def_path.split('/')[1:]
        current_pointer = self.__file_structure_setting.get_data_json_schema()
        for each_path in def_paths:
            if each_path not in current_pointer:
                raise ValueError(f'missing {each_path} while searching {def_path}')
            current_pointer = current_pointer[each_path]
        self.__independent_definitions[def_path] = current_pointer
        return current_pointer

    def __get_pandas_type(self, json_type: str):
        if json_type not in self.__json_to_pandas_data_type:
            raise ValueError(f'unknown json type. cannot convert to pandas type: {json_type}')
        return self.__json_to_pandas_data_type[json_type]

    def __get_spark_type(self, json_type: str):
        if json_type not in self.__json_to_spark_data_types:
            raise ValueError(f'unknown json type. cannot convert to spark type: {json_type}')
        return self.__json_to_spark_data_types[json_type]

    def get_schema_from_json(self):
        if self.__file_structure_setting is None:
            raise ValueError('pls load FileStructureSetting to continue')
        derived_structs = [StructField(k, self.__derived_spark_data_types[v['updated_type']], True) for k, v in self.__file_structure_setting.get_derived_columns().items()]
        data_column_definitions = self.__file_structure_setting.get_data_column_definitions()
        dynamic_columns = [StructField(k, self.__get_spark_type(self.__get_json_datatype(k, v)), True) for k, v in data_column_definitions.items()]
        return StructType(dynamic_columns + derived_structs)

    def get_pandas_schema_from_json(self):
        if self.__file_structure_setting is None:
            raise ValueError('pls load FileStructureSetting to continue')
        data_column_definitions = self.__file_structure_setting.get_data_column_definitions()
        dynamic_columns = {k: self.__get_pandas_type(self.__get_json_datatype(k, v)) for k, v in data_column_definitions.items()}
        return dynamic_columns
