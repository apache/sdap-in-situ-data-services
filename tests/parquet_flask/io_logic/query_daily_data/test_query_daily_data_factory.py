from unittest import TestCase

from parquet_flask.io_logic.query_daily_data.insitu_query_props import InsituQueryProps
from parquet_flask.io_logic.query_daily_data.query_daily_data_factory import QueryDailyDataFactory


class TestQueryDailyDataFactory(TestCase):
    def test_gmu(self):
        query_props = InsituQueryProps()
        # query_props.provider = 'PurpleAir-GMU-Cal'
        query_props.provider = 'PurpleAir-GMU-Intermediate'
        query_props.project = 'air_quality'
        query_props.timestamp = '2023-01-01'
        query_props.min_lat_lon = [-90, -180]
        query_props.max_lat_lon = [90, 180]
        # https://ideas-digitaltwin.jpl.nasa.gov/insitu_airnow/1.0/es_insitu_data?itemsPerPage=500&bbox=-180,-90,180,90&provider=PurpleAir-GMU-Intermediate&project=AQIC&timestamp=2023-06-01T00:00:00Z

        stats = QueryDailyDataFactory().get_instance(query_props.provider, base_url='https://insitu-api.stcenter.net')

        results = stats.query(query_props)
        print(results)
        return
