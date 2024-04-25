from unittest import TestCase

from parquet_flask.io_logic.query_insitu_data.query_insitu_data_factory import QueryInsituDataFactory
from parquet_flask.io_logic.query_v2 import QueryProps


class TestQueryInsituDataFactory(TestCase):
    def test_gmu(self):
        query_props = QueryProps()
        # query_props.provider = 'PurpleAir-GMU-Intermediate'
        query_props.provider = 'PurpleAir-GMU-Raw-Hourly'
        query_props.project = 'air_quality'
        query_props.min_datetime = '2023-01-01T00:00:00'
        query_props.max_datetime = '2023-04-07T23:00:00'
        query_props.platform_id = ['9678']
        stats = QueryInsituDataFactory().get_instance(query_props.provider, query_props=query_props, base_url='https://insitu-api.stcenter.net')
        results = stats.search()
        print(results)
        return

    def test_gmu_01(self):
        # https://ideas-digitaltwin.jpl.nasa.gov/insitu/1.0/query_data_doms_custom_pagination?
        # startIndex=0&itemsPerPage=1000&provider=PurpleAir-GMU-Raw-Hourly&project=air_quality&
        # startTime=2021-08-10T00:00:00Z&endTime=2021-08-13T00:00:00Z&platform=67985&bbox=-122.96576,37.24576,-121.96576,38.24576
        query_props = QueryProps()
        # query_props.provider = 'PurpleAir-GMU-Intermediate'
        query_props.provider = 'PurpleAir-GMU-Raw-Hourly'
        query_props.project = 'air_quality'
        query_props.min_datetime = '2021-08-10T00:00:00Z'
        query_props.max_datetime = '2021-08-13T00:00:00Z'
        query_props.platform_id = ['67985']
        stats = QueryInsituDataFactory().get_instance(query_props.provider, query_props=query_props, base_url='https://insitu-api.stcenter.net')
        results = stats.search()
        print(results)
        return
