import json
import logging

from jsonschema import validate, FormatChecker, ValidationError

from parquet_flask.utils.file_utils import FileUtils

LOGGER = logging.getLogger(__name__)


class SanitizeRecord:
    def __init__(self, json_schema_path):
        self.__json_schema_path = json_schema_path
        if not FileUtils.file_exist(json_schema_path):
            raise ValueError('json_schema file does not exist: {}'.format(json_schema_path))
        self.__json_schema = FileUtils.read_json(json_schema_path)
        self.__schema_key_values = {k: v for k, v in self.__json_schema['definitions']['observation']['properties'].items()}

    def __validate_json(self, json_obj):
        try:
            validate(instance=json_obj, schema=self.__json_schema, format_checker=FormatChecker())
        except ValidationError as error:
            LOGGER.exception('failed to validate json')
            return False
        return True

    def __sanitize_record(self, data_blk):
        for k, v in data_blk.items():
            if k in self.__schema_key_values and \
                    'type' in self.__schema_key_values[k] and \
                    self.__schema_key_values[k]['type'] == 'number':
                data_blk[k] = float(v)
        return

    def start(self, json_file_path):
        if not FileUtils.file_exist(json_file_path):
            raise ValueError('json file does not exist: {}'.format(json_file_path))
        json_obj = FileUtils.read_json(json_file_path)
        if not self.__validate_json(json_obj):
            raise ValueError('input file has invalid schema: {}'.format(json_file_path))
        for each in json_obj:
            self.__sanitize_record(each)
        return json_obj
