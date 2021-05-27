import json
from pathlib import Path


class FileUtils:
    @staticmethod
    def file_exist(path):
        return Path(path).is_file()

    @staticmethod
    def dir_exist(path):
        return Path(path).is_dir()

    @staticmethod
    def del_file(path):
        return Path(path).unlink(missing_ok=True)

    @staticmethod
    def read_json(path):
        with open(path, 'r') as ff:
            try:
                return json.loads(ff.read())
            except:
                return None
