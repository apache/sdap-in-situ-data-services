import json
from pathlib import Path


class FileUtils:
    @staticmethod
    def file_exist(path):
        return Path(path).is_file()

    @staticmethod
    def read_json(path):
        with open(path, 'r') as ff:
            try:
                return json.loads(ff.read())
            except:
                return None
