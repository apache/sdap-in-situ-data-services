from unittest import TestCase

from parquet_flask.io_logic.query_insitu_data.query_insitu_data_factory import QueryInsituDataFactory
from parquet_flask.io_logic.query_v2 import QueryProps


class TestQueryInsituDataFactory(TestCase):
    def test_gmu(self):
        query_props = QueryProps()
        query_props.provider = 'PurpleAir-GMU-Intermediate'
        query_props.project = 'air_quality'
        query_props.min_datetime = '2023-01-01T00:00:00'
        query_props.max_datetime = '2023-02-01T00:00:00'
        query_props.platform_id = ['195']
        stats = QueryInsituDataFactory().get_instance(query_props.provider, query_props=query_props, base_url='https://insitu-api.stcenter.net')
        results = stats.search()
        print(results)
        return
