import os
from unittest import TestCase

from parquet_flask.cdms_lambda_func.cdms_lambda_constants import CdmsLambdaConstants
from parquet_flask.cdms_lambda_func.index_to_es.parquet_stat_extractor import ParquetStatExtractor
from parquet_flask.utils.general_utils import GeneralUtils


class TestParquetStatExtractor(TestCase):
    def test_01(self):
        parquet_stat_schema = {
            'type': 'object',
            'required': ['total',
                         'min_datetime', 'min_datetime',
                         'min_depth', 'max_depth',
                         'min_lat', 'max_lat',
                         'min_lon', 'max_lon',
                         ],
            'properties': {
                'total': {'type': 'number'},
                'min_datetime': {'type': 'number'},
                'max_datetime': {'type': 'number'},
                'min_depth': {'type': 'number'},
                'max_depth': {'type': 'number'},
                'min_lat': {'type': 'number'},
                'max_lat': {'type': 'number'},
                'min_lon': {'type': 'number'},
                'max_lon': {'type': 'number'},
}
        }
        os.environ[CdmsLambdaConstants.cdms_url] = 'http://localhost:30801/insitu/1.0/extract_stats/'
        os.environ[CdmsLambdaConstants.parquet_base_folder] = 'CDMS_insitu.geo2.parquet'
        os.environ[CdmsLambdaConstants.verify_ssl] = 'false'
        s3_key = 'CDMS_insitu.geo2.parquet/provider=Florida State University, COAPS/project=SAMOS/platform_code=30/geo_spatial_interval=-15_-35/year=2018/month=2/job_id=24054823-eed4-4f44-8138-ee7a39985484/part-00000-cfe9510a-8c31-44b6-aafd-048af0feced3.c000.gz.parquet'
        parquet_stat = ParquetStatExtractor().start(s3_key)
        self.assertTrue(GeneralUtils.is_json_valid(parquet_stat, parquet_stat_schema)[0], f'invalid schema: {parquet_stat}')
        return
