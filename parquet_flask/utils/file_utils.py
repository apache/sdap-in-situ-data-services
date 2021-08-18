import hashlib
import json
import os
import zlib
from functools import partial
from pathlib import Path


class FileUtils:

    @staticmethod
    def get_crc_checksum(file_path, chunksize=65536):
        """Compute the CRC-32 checksum of the contents of the given filename"""
        with open(file_path, "rb") as f:
            checksum = 0
            for buf in iter(partial(f.read, chunksize), b''):
                checksum = zlib.crc32(buf, checksum)
            return checksum

    @staticmethod
    def get_checksum(file_path):
        with open(file_path, mode='rb') as f:
            d = hashlib.sha512()
            for buf in iter(partial(f.read, 512 * 2**10), b''):
                d.update(buf)
        return d.hexdigest()

    @staticmethod
    def get_size(file_path):
        return os.stat(file_path).st_size

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
