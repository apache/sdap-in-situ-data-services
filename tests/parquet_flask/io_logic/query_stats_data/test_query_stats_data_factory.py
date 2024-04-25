from unittest import TestCase

from parquet_flask.io_logic.query_stats_data.query_stats_data_factory import QueryStatsDataFactory
from parquet_flask.io_logic.query_v2 import QueryProps


class TestQueryStatsDataFactory(TestCase):
    def test_gmu(self):
        query_props = QueryProps()
        # query_props.provider = 'PurpleAir-GMU-Raw-Hourly'
        # query_props.provider = 'PurpleAir-GMU-Cal'
        query_props.provider = 'PurpleAir-GMU-Intermediate'
        # query_props.min_datetime = '2023-01-01T00:00:00'
        # query_props.max_datetime = '2023-02-01T00:00:00'
        stats = QueryStatsDataFactory().get_instance(query_props.provider, query_props=query_props, base_url='https://insitu-api.stcenter.net')
        results = stats.start()
        print(results)
        return
