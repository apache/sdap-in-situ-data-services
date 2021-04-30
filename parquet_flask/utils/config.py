import os

from parquet_flask.utils.singleton import Singleton


class Config(metaclass=Singleton):
    def __init__(self):
        self.__keys = [
            'master_spark_url',
            'spark_app_name',
            'parquet_file_name',
        ]
        self.__optional_keys = [
            'spark_ram_size',
        ]
        self.__validate()

    def __validate(self):
        missing_mandatory_keys = [k for k in self.__keys if k not in os.environ]
        if len(missing_mandatory_keys) > 0:
            raise RuntimeError('missing configuration values in environment values: {}'.format(missing_mandatory_keys))
        return

    def get_value(self, key, default_val=None):
        if key in os.environ:
            return os.environ[key]
        return default_val
