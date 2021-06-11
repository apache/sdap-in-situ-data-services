import json
import os
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
        if FileUtils.file_exist(path):
            Path(path).unlink()
        return

    @staticmethod
    def read_json(path):
        with open(path, 'r') as ff:
            try:
                return json.loads(ff.read())
            except:
                return None

    @staticmethod
    def write_json(file_path, json_obj, overwrite=False, append=False, prettify=False):
        if os.path.exists(file_path) and not overwrite:
            raise ValueError('{} already exists, and not overwriting'.format(file_path))
        with open(file_path, 'a' if append else 'w') as ff:
            json_str = json.dumps(json_obj, indent=4) if prettify else json.dumps(json_obj)
            ff.write(json_str)
            pass
        return
