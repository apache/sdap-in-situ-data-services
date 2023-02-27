import json
from unittest import TestCase

from insitu.file_structure_setting import FileStructureSetting
from parquet_flask.cdms_lambda_func.index_to_es.parquet_file_path_stat_extractor import ParquetFilePathStatExtractor


sample_structure_setting_json = {
            "data_array_key": "observations",
            "file_metadata_keys": [
                "provider",
                "project"
            ],
            "time_columns": ["time"],
            "partitioning_columns": ["provider", "project", "platform_code", "geo_spatial_interval", "year", "month",
                                     "job_id"],
            "non_data_columns": ["time_obj", "time", "provider", "project", "platform_code", "platform", "year",
                                 "month", "job_id", "device", "latitude", "longitude", "depth"],
            "derived_columns": {
                "time_obj": {
                    "original_column": "time",
                    "updated_type": "time"
                },
                "year": {
                    "original_column": "time",
                    "updated_type": "year"
                },
                "month": {
                    "original_column": "time",
                    "updated_type": "month"
                },
                "platform_code": {
                    "original_column": "platform.code",
                    "updated_type": "column"
                },
                "project": {
                    "original_column": "project",
                    "updated_type": "literal"
                },
                "provider": {
                    "original_column": "provider",
                    "updated_type": "literal"
                },
                "job_id": {
                    "original_column": "job_id",
                    "updated_type": "literal"
                },
                "geo_spatial_interval": {
                    "original_column": ["latitude", "longitude"],
                    "split_interval_key": "project",
                    "updated_type": "insitu_geo_spatial"
                }
            }
        }


class TestParquetFilePathStatExtractor(TestCase):
    def test_01(self):
        file_structure_setting = FileStructureSetting({}, sample_structure_setting_json)
        abs_file_path = 's3://cdms-dev-in-situ-parquet/CDMS_insitu.geo2.parquet/provider=Florida State University, COAPS/project=SAMOS/platform_code=30/geo_spatial_interval=-10_-35/year=2018/month=2/job_id=24054823-eed4-4f44-8138-ee7a39985484/part-00000-cfe9510a-8c31-44b6-aafd-048af0feced3.c000.gz.parquet'
        stat_extractor = ParquetFilePathStatExtractor(file_structure_setting, abs_file_path)

        mock_output = {
            "base_name": "cdms-dev-in-situ-parquet",
            "file_name": "part-00000-cfe9510a-8c31-44b6-aafd-048af0feced3.c000.gz.parquet",
            "abs_file_path": abs_file_path,
            "geo_spatial_interval": "-10_-35",
            "month": "2",
            "job_id": "24054823-eed4-4f44-8138-ee7a39985484",
            "platform_code": "30",
            "project": "SAMOS",
            "provider": "Florida State University, COAPS",
            "year": "2018"
        }
        self.assertEqual(json.dumps(mock_output, sort_keys=True), json.dumps(stat_extractor.start().to_json(), sort_keys=True), 'wrong json output')
        return

    def test_backward_compatible_01(self):
        file_structure_setting = FileStructureSetting({}, sample_structure_setting_json)
        abs_file_path = 's3://cdms-dev-in-situ-parquet/CDMS_insitu.geo2.parquet/provider=Florida State University, COAPS/project=SAMOS/platform_code=30/geo_spatial_interval=-10_-35/year=2018/month=2/job_id=24054823-eed4-4f44-8138-ee7a39985484/part-00000-cfe9510a-8c31-44b6-aafd-048af0feced3.c000.gz.parquet'
        stat_extractor = ParquetFilePathStatExtractor(file_structure_setting, abs_file_path, 'bucket', 'name', 's3_url')
        mock_output = {
            "bucket": "cdms-dev-in-situ-parquet",
            "job_id": "24054823-eed4-4f44-8138-ee7a39985484",
            "geo_spatial_interval": "-10_-35",
            "month": "2",
            "name": "part-00000-cfe9510a-8c31-44b6-aafd-048af0feced3.c000.gz.parquet",
            "platform_code": "30",
            "project": "SAMOS",
            "provider": "Florida State University, COAPS",
            "s3_url": "s3://cdms-dev-in-situ-parquet/CDMS_insitu.geo2.parquet/provider=Florida State University, COAPS/project=SAMOS/platform_code=30/geo_spatial_interval=-10_-35/year=2018/month=2/job_id=24054823-eed4-4f44-8138-ee7a39985484/part-00000-cfe9510a-8c31-44b6-aafd-048af0feced3.c000.gz.parquet",
            "year": "2018"
        }
        self.assertEqual(json.dumps(mock_output, sort_keys=True), json.dumps(stat_extractor.start().to_json(), sort_keys=True), 'wrong json output')
        return

    def test_02(self):
        file_structure_setting = FileStructureSetting({}, sample_structure_setting_json)
        abs_file_path = 's3://cdms-dev-in-situ-parquet/CDMS_insitu.geo2.parquet/provider=Florida State University, COAPS/project=SAMOS/platform_code=30/year=2018/month=2/job_id=24054823-eed4-4f44-8138-ee7a39985484/part-00000-cfe9510a-8c31-44b6-aafd-048af0feced3.c000.gz.parquet'
        stat_extractor = ParquetFilePathStatExtractor(file_structure_setting, abs_file_path)

        mock_output = {
            "base_name": "cdms-dev-in-situ-parquet",
            "file_name": "part-00000-cfe9510a-8c31-44b6-aafd-048af0feced3.c000.gz.parquet",
            "abs_file_path": abs_file_path,
            "month": "2",
            "job_id": "24054823-eed4-4f44-8138-ee7a39985484",
            "platform_code": "30",
            "project": "SAMOS",
            "provider": "Florida State University, COAPS",
            "year": "2018"
        }
        self.assertEqual(json.dumps(mock_output, sort_keys=True),
                         json.dumps(stat_extractor.start().to_json(), sort_keys=True), 'wrong json output')
        return
