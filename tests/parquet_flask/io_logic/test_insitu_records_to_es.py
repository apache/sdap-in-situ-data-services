from unittest import TestCase

from parquet_flask.io_logic.insitu_records_to_es import InsituRecordsToEs, InsituQueryProps


class TestInsituRecordsToEs(TestCase):
    def test_01_ingest_manual(self):
        es_url = 'https://search-ideas-api-dev-1-f62xltsguioft2hpjepkrhln3e.us-west-2.es.amazonaws.com/'
        s3_url = 's3://aq-in-situ-data-staging/AirNow/daily/2023-10_daily.json.gz'
        InsituRecordsToEs(es_url).ingest(s3_url)
        return

    def test_02_query(self):
        es_url = 'https://search-ideas-api-dev-1-f62xltsguioft2hpjepkrhln3e.us-west-2.es.amazonaws.com/'
        query_props = InsituQueryProps()
        query_props.project = 'air_quality'
        query_props.provider = 'AirNow'
        query_props.timestamp = '2023-10-11'
        # query_props.marker = ['340170006']
        query_props.variable = ['o3', 'pm2_5']
        query_props.max_lat_lon = [0, 0]
        query_props.min_lat_lon = [-90, -90]
        result = InsituRecordsToEs(es_url).query(query_props)
        print(result)
        """
        '000010601', 'short_name': 'Goose Bay'
        '271713201', 'short_name': 'St. Michael
        """
        return
