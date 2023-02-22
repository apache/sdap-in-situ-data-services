import json
import os
import tempfile
from unittest import TestCase

from pyspark.sql import SparkSession


class TestGeneralUtilsV3(TestCase):
    def test_get_geospatial_interval(self):
        os.environ['geospatial_interval_by_project'] = json.dumps({
            "ICOADS Release 3.0": 100,
            "SAMOS": "50",
            "t1": "7.5",
            "SPURS": "75"
        })
        from parquet_flask.io_logic.ingest_new_file import get_geospatial_interval, IngestNewJsonFile
        self.assertEqual(get_geospatial_interval('SAMOS'), 50, 'wrong for SAMOS')
        self.assertEqual(get_geospatial_interval('SPURS'), 75, 'wrong for SPURS')
        self.assertEqual(get_geospatial_interval('ICOADS Release 3.0'), 100, 'wrong for ICOADS Release 3.0')
        self.assertEqual(get_geospatial_interval('t1'), 30, 'wrong for t1')
        return

    def test_ingest_01(self):
        os.environ['es_url'] = ''
        os.environ['master_spark_url'] = ''
        os.environ['spark_app_name'] = ''
        os.environ['in_situ_schema'] = '/Users/wphyo/Projects/access/parquet_test_1/in_situ_schema.json'
        os.environ['authentication_type'] = ''
        os.environ['authentication_key'] = ''
        os.environ['parquet_metadata_tbl'] = ''
        os.environ['file_structure_setting'] = '/Users/wphyo/Projects/access/parquet_test_1/insitu.data.settings.json'
        mock_data = {
            "project": "Sample-Project",
            "provider": "Sample-Provider",
            "observations": [
                {
                    "time": "2017-01-08T00:00:00Z",
                    "latitude": 13.4255,
                    "longitude": 144.6627,
                    "depth": -99999.0,
                    "platform": {
                        "code": "30"
                    },
                    "meta": "KAOU_20170108v20001_0000",
                    "sea_water_salinity": 0.168,
                    "sea_water_salinity_quality": 1,
                    "sea_water_temperature": 21.88,
                    "sea_water_temperature_quality": 3
                },
                {
                    "time": "2017-01-08T00:00:00Z",
                    "latitude": 13.4255,
                    "longitude": 144.6627,
                    "depth": -17.1,
                    "platform": {
                        "code": "30"
                    },
                    "meta": "KAOU_20170108v20001_0000",
                    "wind_from_direction": 74.9,
                    "wind_from_direction_quality": 1,
                    "air_pressure": 1008.1,
                    "air_pressure_quality": 1,
                    "relative_humidity": 66.6,
                    "relative_humidity_quality": 1,
                    "wind_speed": 7.8,
                    "wind_speed_quality": 1,
                    "eastward_wind": -7.5,
                    "northward_wind": -2.0,
                    "wind_component_quality": 1,
                    "air_temperature": 27.92,
                    "air_temperature_quality": 1
                },
                {
                    "time": "2017-01-08T00:00:00Z",
                    "latitude": 13.4255,
                    "longitude": 144.6627,
                    "depth": -20.1,
                    "platform": {
                        "code": "30"
                    },
                    "meta": "KAOU_20170108v20001_0000",
                    "downwelling_longwave_flux_in_air": 415.71,
                    "downwelling_longwave_flux_in_air_quality": 1,
                    "surface_downwelling_photosynthetic_photon_flux_in_air": 2838.66,
                    "surface_downwelling_photosynthetic_photon_flux_in_air_quality": 4,
                    "downwelling_shortwave_flux_in_air": 566.2,
                    "downwelling_shortwave_flux_in_air_quality": 1
                },
                {
                    "time": "2017-01-08T00:01:00Z",
                    "latitude": 13.4255,
                    "longitude": 144.6627,
                    "depth": -99999.0,
                    "platform": {
                        "code": "30"
                    },
                    "meta": "KAOU_20170108v20001_0001",
                    "sea_water_salinity": 0.168,
                    "sea_water_salinity_quality": 1,
                    "sea_water_temperature": 21.88,
                    "sea_water_temperature_quality": 3
                },
                {
                    "time": "2017-01-08T00:01:00Z",
                    "latitude": 13.4255,
                    "longitude": 144.6627,
                    "depth": -17.1,
                    "platform": {
                        "code": "32"
                    },
                    "meta": "KAOU_20170108v20001_0001",
                    "wind_from_direction": 71.2,
                    "wind_from_direction_quality": 1,
                    "air_pressure": 1008.09,
                    "air_pressure_quality": 1,
                    "relative_humidity": 66.9,
                    "relative_humidity_quality": 1,
                    "wind_speed": 7.6,
                    "wind_speed_quality": 1,
                    "eastward_wind": -7.2,
                    "northward_wind": -2.4,
                    "wind_component_quality": 1,
                    "air_temperature": 27.96,
                    "air_temperature_quality": 1
                },
                {
                    "time": "2017-01-08T00:01:00Z",
                    "latitude": 13.4255,
                    "longitude": 144.6627,
                    "depth": -20.1,
                    "platform": {
                        "code": "31"
                    },
                    "meta": "KAOU_20170108v20001_0001",
                    "downwelling_longwave_flux_in_air": 416.22,
                    "downwelling_longwave_flux_in_air_quality": 1,
                    "surface_downwelling_photosynthetic_photon_flux_in_air": 2740.37,
                    "surface_downwelling_photosynthetic_photon_flux_in_air_quality": 4,
                    "downwelling_shortwave_flux_in_air": 555.61,
                    "downwelling_shortwave_flux_in_air_quality": 1
                }
            ]
        }
        with tempfile.TemporaryDirectory() as tmp_dir_name:
            os.environ['parquet_file_name'] = os.path.join(tmp_dir_name, 'parquet')
            data_file = os.path.join(tmp_dir_name, 'sample-file.json')
            with open(data_file, 'w') as ff:
                ff.write(json.dumps(mock_data))
            spark = SparkSession.builder.getOrCreate()
            from parquet_flask.io_logic.ingest_new_file import get_geospatial_interval, IngestNewJsonFile
            ingest_file = IngestNewJsonFile(False)
            ingest_file.ingest(data_file, 'test-id', spark)
            print('say hi')
        return
