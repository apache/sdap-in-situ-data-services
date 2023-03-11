import os
from unittest import TestCase

from parquet_flask.aws.es_abstract import ESAbstract
from parquet_flask.aws.es_factory import ESFactory
from parquet_flask.insitu.file_structure_setting import FileStructureSetting
from parquet_flask.io_logic.parquet_path_retriever import ParquetPathRetriever
from parquet_flask.io_logic.partitioned_parquet_path import PartitionedParquetPath
from parquet_flask.utils.file_utils import FileUtils

#https://doms.jpl.nasa.gov/insitu/1.0/query_data_doms_custom_pagination?startIndex=0&itemsPerPage=1000&startTime=&endTime=&bbox=&minDepth=0.0&maxDepth=5.0&provider=NCAR&project=ICOADS%20Release%203.0&platform=42


class TestParquetPathRetriever(TestCase):
    def test_01(self):
        es_url = 'https://search-insitu-parquet-dev-1-vgwt2bx23o5w3gpnq4afftmvaq.us-west-2.es.amazonaws.com/'
        aws_es: ESAbstract = ESFactory().get_instance('AWS', index='parquet_stats_alias', base_url=es_url, port=443)
        data_json_schema = FileUtils.read_json('/Users/wphyo/Projects/access/parquet_test_1/in_situ_schema.json')
        structure_config = FileUtils.read_json('/Users/wphyo/Projects/access/parquet_test_1/insitu.file.structure.config.json')
        file_struct_setting = FileStructureSetting(data_json_schema=data_json_schema, structure_config=structure_config)
        query_dict = {
            'provider': 'Florida State University, COAPS',
            'project': 'SAMOS',
            'platform': ','.join(['30', '31', '32']),
            'startTime': '2017-01-25T09:00:00Z',
            'endTime': '2018-10-24T09:00:00Z',
            'bbox': ','.join([str(k) for k in [-180.0, -90, 179.38330739034632, 89.90]]),
        }
        ppp_list = ParquetPathRetriever(aws_es, file_struct_setting, '').start(query_dict)
        self.assertTrue(isinstance(ppp_list[0], PartitionedParquetPath), 'result not PartitionedParquetPath type')
        return
