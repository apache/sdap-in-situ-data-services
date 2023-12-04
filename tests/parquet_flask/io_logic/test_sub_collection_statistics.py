import os
from unittest import TestCase

from parquet_flask.io_logic.query_v2 import QueryProps
from parquet_flask.io_logic.sub_collection_statistics import SubCollectionStatistics

os.environ['master_spark_url'] = ''
os.environ['spark_app_name'] = ''
os.environ['parquet_file_name'] = ''
os.environ['in_situ_schema'] = '/Users/wphyo/Projects/access/parquet_test_1/in_situ_schema.json'
os.environ['authentication_type'] = ''
os.environ['authentication_key'] = ''
os.environ['parquet_metadata_tbl'] = ''
os.environ['es_url'] = 'https://search-ideas-api-dev-1-f62xltsguioft2hpjepkrhln3e.us-west-2.es.amazonaws.com'

class TestSubCollectionStatistics(TestCase):
    def test_01(self):
        query_props = QueryProps()
        query_props.provider = 'RAPID_NOAHMP_3x_Garonne'
        query_props.project = 'IDEAS'
        query_props.size = 10000
        sub_collection_stats_api = SubCollectionStatistics(query_props)
        sub_collection_stats = sub_collection_stats_api.start()
        while 'markerPlatform' in sub_collection_stats:
            print(sub_collection_stats['markerPlatform'], len(sub_collection_stats['providers'][0]['projects'][0]['platforms']))
            # print(sub_collection_stats)
            query_props.marker_platform_code = sub_collection_stats['markerPlatform']
            sub_collection_stats = sub_collection_stats_api.start()
        return
