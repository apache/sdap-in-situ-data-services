import json
from unittest import TestCase

from parquet_flask.insitu.file_structure_setting import FileStructureSetting
from parquet_flask.cdms_lambda_func.index_to_es.parquet_file_path_stat_extractor import ParquetFilePathStatExtractor
from parquet_flask.utils.file_utils import FileUtils


class TestParquetFilePathStatExtractor(TestCase):
    def test_01(self):
        data_json_schema = FileUtils.read_json('../../../../in_situ_schema.json')
        structure_config = FileUtils.read_json('../../../../insitu.file.structure.config.json')
        file_structure_setting = FileStructureSetting(data_json_schema=data_json_schema, structure_config=structure_config)
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
        data_json_schema = FileUtils.read_json('../../../../in_situ_schema.json')
        structure_config = FileUtils.read_json('../../../../insitu.file.structure.config.json')
        file_structure_setting = FileStructureSetting(data_json_schema=data_json_schema, structure_config=structure_config)
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
        data_json_schema = FileUtils.read_json('../../../../in_situ_schema.json')
        structure_config = FileUtils.read_json('../../../../insitu.file.structure.config.json')
        file_structure_setting = FileStructureSetting(data_json_schema=data_json_schema, structure_config=structure_config)
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
