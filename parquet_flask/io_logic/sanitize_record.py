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

import logging

from insitu.file_structure_setting import FileStructureSetting
from parquet_flask.utils.file_utils import FileUtils
from parquet_flask.utils.general_utils import GeneralUtils
from parquet_flask.utils.parallel_json_validator import ParallelJsonValidator

LOGGER = logging.getLogger(__name__)


class SanitizeRecord:
    def __init__(self, json_schema_path, file_structure_setting: FileStructureSetting):
        self.__file_structure_setting = file_structure_setting
        self.__json_schema_path = json_schema_path
        if not FileUtils.file_exist(json_schema_path):
            raise ValueError('json_schema file does not exist: {}'.format(json_schema_path))
        self.__json_schema = FileUtils.read_json(json_schema_path)
        self.__schema_key_values = {k: v for k, v in self.__json_schema['definitions'][self.__file_structure_setting.get_data_array_key()]['properties'].items()}
        self.__parallel_json_validator = ParallelJsonValidator()

    def __sanitize_record(self, data_blk):
        for k, v in data_blk.items():
            if k in self.__schema_key_values and \
                    'type' in self.__schema_key_values[k] and \
                    self.__schema_key_values[k]['type'] == 'number':
                data_blk[k] = float(v)
        return

    def __validate_json(self, data):
        LOGGER.debug(f'validating input data')
        chunked_data = [{
            "provider": data['provider'],  # TODO provider and project to be abstracted out.
            "project": data['project'],
            self.__file_structure_setting.get_data_array_key(): eachChunk,
        } for eachChunk in GeneralUtils.chunk_list(data[self.__file_structure_setting.get_data_array_key()], 1000)]
        if not self.__parallel_json_validator.is_schema_loaded():
            self.__parallel_json_validator.load_schema(self.__json_schema)
        result, error = self.__parallel_json_validator.validate_json(chunked_data)
        return result, error

    def __get_basic_schema(self):
        basic_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "title": "Cloud-based Data Match-Up Service In Situ Schema",
            "description": "Schema for in situ data",
            "properties": {
                "provider": {
                    "description": "",
                    "type": "string"
                },
                "project": {
                    "description": "",
                    "type": "string"
                },
                self.__file_structure_setting.get_data_array_key(): {
                    "type": "array",
                    "items": {
                        "type": "object"
                    },
                    "minItems": 1
                }
            },
            "required": [
                "provider",
                "project",
                self.__file_structure_setting.get_data_array_key(),
            ]
        }
        return basic_schema

    def start(self, json_file_path):
        if not FileUtils.file_exist(json_file_path):
            raise ValueError('json file does not exist: {}'.format(json_file_path))
        json_obj = FileUtils.read_json(json_file_path)
        is_valid, json_errors = GeneralUtils.is_json_valid(json_obj, self.__get_basic_schema())
        if not is_valid:
            raise ValueError(f'input file has invalid high level schema: {json_file_path}. errors; {json_errors}')
        LOGGER.warning('disabling validation of individual observation record. it is taking a long time')
        is_valid, json_errors = self.__validate_json(json_obj)
        if not is_valid:
            raise ValueError(f'json has some error. Not validating: {json_errors}')
        for each in json_obj[self.__file_structure_setting.get_data_array_key()]:
            self.__sanitize_record(each)
        return json_obj
