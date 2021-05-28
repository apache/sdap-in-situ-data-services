import logging

from parquet_flask.utils.file_utils import FileUtils
from parquet_flask.utils.general_utils import GeneralUtils

LOGGER = logging.getLogger(__name__)


class SanitizeRecord:
    def __init__(self, json_schema_path):
        self.__json_schema_path = json_schema_path
        if not FileUtils.file_exist(json_schema_path):
            raise ValueError('json_schema file does not exist: {}'.format(json_schema_path))
        self.__json_schema = FileUtils.read_json(json_schema_path)
        self.__schema_key_values = {k: v for k, v in self.__json_schema['definitions']['observation']['properties'].items()}

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
        is_valid, json_error = GeneralUtils.is_json_valid(json_obj, self.__json_schema)
        if not is_valid:
            raise ValueError(f'input file has invalid schema: {json_file_path}. errors; {json_error}')
        for each in json_obj:
            self.__sanitize_record(each)
        return json_obj
