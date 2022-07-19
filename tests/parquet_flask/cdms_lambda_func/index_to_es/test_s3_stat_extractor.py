import json
from unittest import TestCase

from parquet_flask.cdms_lambda_func.index_to_es.s3_stat_extractor import S3StatExtractor


class TestGeneralUtils(TestCase):
    def test_01(self):
        s3_stat = S3StatExtractor('s3://cdms-dev-in-situ-parquet/CDMS_insitu.geo2.parquet/provider=Florida State University, COAPS/project=SAMOS/platform_code=30/geo_spatial_interval=-10_-35/year=2018/month=2/job_id=24054823-eed4-4f44-8138-ee7a39985484/part-00000-cfe9510a-8c31-44b6-aafd-048af0feced3.c000.gz.parquet').start()
        self.assertEqual('cdms-dev-in-situ-parquet', s3_stat.bucket, 'wrong bucket')
        self.assertEqual('Florida State University, COAPS', s3_stat.provider, 'wrong provider')
        self.assertEqual('SAMOS', s3_stat.project, 'wrong project')
        self.assertEqual('30', s3_stat.platform_code, 'wrong platform_code')
        self.assertEqual('-10_-35', s3_stat.geo_interval, 'wrong geo_interval')
        self.assertEqual('2018', s3_stat.year, 'wrong year')
        self.assertEqual('2', s3_stat.month, 'wrong month')
        self.assertEqual('24054823-eed4-4f44-8138-ee7a39985484', s3_stat.job_id, 'wrong job_id')
        self.assertEqual('part-00000-cfe9510a-8c31-44b6-aafd-048af0feced3.c000.gz.parquet', s3_stat.name, 'wrong name')
        mock_output = {
            "bucket": "cdms-dev-in-situ-parquet",
            "geo_spatial_interval": "-10_-35",
            "month": "2",
            "name": "part-00000-cfe9510a-8c31-44b6-aafd-048af0feced3.c000.gz.parquet",
            "platform_code": "30",
            "project": "SAMOS",
            "provider": "Florida State University, COAPS",
            "s3_url": "s3://cdms-dev-in-situ-parquet/CDMS_insitu.geo2.parquet/provider=Florida State University, COAPS/project=SAMOS/platform_code=30/geo_spatial_interval=-10_-35/year=2018/month=2/job_id=24054823-eed4-4f44-8138-ee7a39985484/part-00000-cfe9510a-8c31-44b6-aafd-048af0feced3.c000.gz.parquet",
            "year": "2018"
        }
        self.assertEqual(json.dumps(mock_output, sort_keys=True), json.dumps(s3_stat.to_json(), sort_keys=True), 'wrong json output')
        return

    def test_02(self):
        s3_stat = S3StatExtractor('s3://cdms-dev-in-situ-parquet/CDMS_insitu.geo2.parquet/provider=Florida State University, COAPS/project=SAMOS/platform_code=30/year=2018/month=2/job_id=24054823-eed4-4f44-8138-ee7a39985484/part-00000-cfe9510a-8c31-44b6-aafd-048af0feced3.c000.gz.parquet').start()
        self.assertEqual('cdms-dev-in-situ-parquet', s3_stat.bucket, 'wrong bucket')
        self.assertEqual('Florida State University, COAPS', s3_stat.provider, 'wrong provider')
        self.assertEqual('SAMOS', s3_stat.project, 'wrong project')
        self.assertEqual('30', s3_stat.platform_code, 'wrong platform_code')
        self.assertEqual(None, s3_stat.geo_interval, 'wrong geo_interval')
        self.assertEqual('2018', s3_stat.year, 'wrong year')
        self.assertEqual('2', s3_stat.month, 'wrong month')
        self.assertEqual('24054823-eed4-4f44-8138-ee7a39985484', s3_stat.job_id, 'wrong job_id')
        self.assertEqual('part-00000-cfe9510a-8c31-44b6-aafd-048af0feced3.c000.gz.parquet', s3_stat.name, 'wrong name')
        return
