import unittest
import os

os.environ['master_spark_url'] = ''
os.environ['spark_app_name'] = ''
os.environ['parquet_file_name'] = ''
os.environ['in_situ_schema'] = ''
os.environ['authentication_type'] = ''
os.environ['authentication_key'] = ''
os.environ['parquet_metadata_tbl'] = ''
from parquet_flask.utils.file_utils import FileUtils

from parquet_flask.io_logic.cdms_schema import CdmsSchema


class TestGeneralUtilsV3(unittest.TestCase):
    def test_01(self):
        cdms_schema = CdmsSchema()
        old_struct = cdms_schema.ALL_SCHEMA
        new_struct = cdms_schema.get_schema_from_json(FileUtils.read_json('../../../in_situ_schema.json'))
        self.assertEqual(sorted(str(old_struct)), sorted(str(new_struct)), f'not equal old_struct = {old_struct}. new_struct = {new_struct}')
        return
