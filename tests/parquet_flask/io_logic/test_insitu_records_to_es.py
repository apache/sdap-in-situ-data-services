from unittest import TestCase

from parquet_flask.io_logic.insitu_records_to_es import InsituRecordsToEs


class TestInsituRecordsToEs(TestCase):
    def test_01(self):
        es_url = 'https://search-ideas-api-dev-1-f62xltsguioft2hpjepkrhln3e.us-west-2.es.amazonaws.com/'
        s3_url = 's3://aq-in-situ-data-staging/AirNow/daily/2023-10_daily.json.gz'
        InsituRecordsToEs(s3_url, es_url).start()
        return

