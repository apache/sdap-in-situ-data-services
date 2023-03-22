from unittest import TestCase

from parquet_flask.insitu.file_structure_setting import FileStructureSetting

from parquet_flask.io_logic.parquet_query_condition_management_v4 import ParquetQueryConditionManagementV4
from parquet_flask.io_logic.partitioned_parquet_path import PartitionedParquetPath
from parquet_flask.utils.file_utils import FileUtils


class TestParquetQueryConditionManagementV4(TestCase):
    def test_01(self):
        es_config = {
            'es_url': 'https://search-insitu-parquet-dev-1-vgwt2bx23o5w3gpnq4afftmvaq.us-west-2.es.amazonaws.com',
            'es_index': 'parquet_stats_alias',
            'es_port': 443,
        }
        insitu_schema = FileUtils.read_json('/Users/wphyo/Projects/access/parquet_test_1/in_situ_schema.json')
        file_structure_setting = FileStructureSetting(insitu_schema, FileUtils.read_json(
            '/Users/wphyo/Projects/access/parquet_test_1/insitu.file.structure.config.json'))
        query_dict = {
            'project': 'ICOADS Release 3.0',
            'provider': 'NCAR',
            'platform': '42,12',
            'variable': 'air_temperature',
            'columns': 'air_temperature,air_pressure',
            'startTime': '2018-01-31T23:58:48Z',
            'endTime': '2018-03-31T23:58:48Z',
            'minDepth': '0.0',
            'maxDepth': '10.0',
            'bbox': '-180,-90,180,90'
        }
        condition_mgmt = ParquetQueryConditionManagementV4('s3a://cdms-dev-in-situ-parquet/CDMS_insitu.geo3.parquet', -99999.0, es_config, file_structure_setting, query_dict)
        condition_mgmt.manage_query_props()
        self.assertTrue(isinstance(condition_mgmt.parquet_names, list))
        self.assertTrue(len(condition_mgmt.parquet_names) > 0)
        self.assertTrue(isinstance(condition_mgmt.parquet_names[0], PartitionedParquetPath))
        return
