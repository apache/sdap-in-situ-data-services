import unittest

from parquet_flask.io_logic.partitioned_parquet_path import PartitionedParquetPath


class TestGeneralUtilsV3(unittest.TestCase):
    def test_01(self):
        first = PartitionedParquetPath('my_base').set_provider('abc').set_project('def').set_platform('ghi').set_year('2001').set_month('02').set_lat_lon((3,4))
        second = first.duplicate().set_year('2012').set_lat_lon((5,-8))
        third = first.duplicate().set_platform(None)
        self.assertEqual(first.generate_path(), 'my_base/provider=abc/project=def/platform_code=ghi/geo_spatial_interval=3_4/year=2001/month=02', 'wrong path')
        self.assertEqual(second.generate_path(), 'my_base/provider=abc/project=def/platform_code=ghi/geo_spatial_interval=5_-8/year=2012/month=02', 'wrong path')
        self.assertEqual(third.generate_path(), 'my_base/provider=abc/project=def', 'wrong path')
        return

    def test_02(self):
        first = PartitionedParquetPath('my_base').set_provider('abc').set_project('def').set_platform('ghi').set_year('2001').set_month('02').set_lat_lon([3, 4])
        second = first.duplicate().set_year('2012').set_lat_lon([5,-8])
        third = first.duplicate().set_platform(None)
        self.assertEqual(first.generate_path(), 'my_base/provider=abc/project=def/platform_code=ghi/geo_spatial_interval=3_4/year=2001/month=02', 'wrong path')
        self.assertEqual(second.generate_path(), 'my_base/provider=abc/project=def/platform_code=ghi/geo_spatial_interval=5_-8/year=2012/month=02', 'wrong path')
        self.assertEqual(third.generate_path(), 'my_base/provider=abc/project=def', 'wrong path')
        return

    def test_03(self):
        first = PartitionedParquetPath('my_base').set_provider('abc').set_project('def').set_platform('ghi').set_year('2001').set_month('02').set_lat_lon('3_4')
        second = first.duplicate().set_year('2012').set_lat_lon('5_-8')
        third = first.duplicate().set_platform(None)
        self.assertEqual(first.generate_path(), 'my_base/provider=abc/project=def/platform_code=ghi/geo_spatial_interval=3_4/year=2001/month=02', 'wrong path')
        self.assertEqual(second.generate_path(), 'my_base/provider=abc/project=def/platform_code=ghi/geo_spatial_interval=5_-8/year=2012/month=02', 'wrong path')
        self.assertEqual(third.generate_path(), 'my_base/provider=abc/project=def', 'wrong path')
        return

    def test_04(self):
        es_result = {
    "s3_url": "s3://cdms-dev-in-situ-parquet/CDMS_insitu.geo2.parquet/provider=Florida State University, COAPS/project=SAMOS/platform_code=30/geo_spatial_interval=-25_150/year=2017/month=6/job_id=6f33d0e5-65ca-4281-b4df-2d703adee683/part-00000-9cfcbe81-3ca9-4084-9b8c-db451bd8c076.c000.gz.parquet",
    "bucket": "cdms-dev-in-situ-parquet",
    "name": "part-00000-9cfcbe81-3ca9-4084-9b8c-db451bd8c076.c000.gz.parquet",
    "provider": "Florida State University, COAPS",
    "project": "SAMOS",
    "platform_code": "30",
    "geo_spatial_interval": "-25_150",
    "year": "2017",
    "month": "6",
    "total": 8532,
    "min_datetime": 1497312000,
    "max_datetime": 1497398340,
    "min_depth": -31.5,
    "max_depth": 5.9,
    "min_lat": -23.8257,
    "max_lat": -23.6201,
    "min_lon": 154.4868,
    "max_lon": 154.6771
  }
        first = PartitionedParquetPath('my_base').load_from_es(es_result)
        self.assertEqual(first.generate_path(), 'my_base/provider=Florida State University, COAPS/project=SAMOS/platform_code=30/geo_spatial_interval=-25_150/year=2017/month=6', 'wrong path')
        return


