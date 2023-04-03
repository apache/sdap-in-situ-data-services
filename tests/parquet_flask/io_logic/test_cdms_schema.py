# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest
import os

from parquet_flask.insitu.file_structure_setting import FileStructureSetting
from pyspark.sql.types import StructField, StringType, DoubleType, LongType, IntegerType, TimestampType, MapType, \
    StructType

os.environ['master_spark_url'] = ''
os.environ['spark_app_name'] = ''
os.environ['parquet_file_name'] = ''
os.environ['in_situ_schema'] = ''
os.environ['authentication_type'] = ''
os.environ['authentication_key'] = ''
os.environ['parquet_metadata_tbl'] = ''
from parquet_flask.utils.file_utils import FileUtils

from parquet_flask.io_logic.cdms_schema import CdmsSchema


class TestCdmsSchema(unittest.TestCase):
    def test_01(self):
        data_json_schema = FileUtils.read_json('../../../in_situ_schema.json')
        structure_config = FileUtils.read_json('../../../insitu.file.structure.config.json')
        file_struct_setting = FileStructureSetting(data_json_schema=data_json_schema, structure_config=structure_config)
        cdms_schema = CdmsSchema(file_struct_setting)
        ALL_SCHEMA = StructType([
            StructField('geo_spatial_interval', StringType(), True),  # TODO this is a new column. possible backward compatiblility issue (punted. 2023-03-26)
            StructField('depth', DoubleType(), True),
            StructField('latitude', DoubleType(), True),
            StructField('longitude', DoubleType(), True),
            StructField('meta', StringType(), True),
            StructField('platform', MapType(StringType(), StringType()), True),

            StructField('time', StringType(), True),
            StructField('time_obj', TimestampType(), True),

            StructField('provider', StringType(), True),
            StructField('project', StringType(), True),
            StructField('platform_code', StringType(), True),  # TODO this used to be IntegerType. possible backward compatiblility issue (punted. 2023-03-26)
            StructField('year', IntegerType(), True),
            StructField('month', IntegerType(), True),
            StructField('job_id', StringType(), True),

            StructField('air_pressure', DoubleType(), True),
            StructField('air_pressure_quality', LongType(), True),

            StructField('air_temperature', DoubleType(), True),
            StructField('air_temperature_quality', LongType(), True),

            StructField('dew_point_temperature', DoubleType(), True),
            StructField('dew_point_temperature_quality', LongType(), True),

            StructField('downwelling_longwave_flux_in_air', DoubleType(), True),
            StructField('downwelling_longwave_flux_in_air_quality', LongType(), True),

            StructField('downwelling_longwave_radiance_in_air', DoubleType(), True),
            StructField('downwelling_longwave_radiance_in_air_quality', LongType(), True),

            StructField('downwelling_shortwave_flux_in_air', DoubleType(), True),
            StructField('downwelling_shortwave_flux_in_air_quality', LongType(), True),

            StructField('mass_concentration_of_chlorophyll_in_sea_water', DoubleType(), True),
            StructField('mass_concentration_of_chlorophyll_in_sea_water_quality', LongType(), True),

            StructField('rainfall_rate', DoubleType(), True),
            StructField('rainfall_rate_quality', LongType(), True),

            StructField('relative_humidity', DoubleType(), True),
            StructField('relative_humidity_quality', LongType(), True),

            StructField('sea_surface_salinity', DoubleType(), True),
            StructField('sea_surface_salinity_quality', LongType(), True),

            StructField('sea_surface_skin_temperature', DoubleType(), True),
            StructField('sea_surface_skin_temperature_quality', LongType(), True),

            StructField('sea_surface_subskin_temperature', DoubleType(), True),
            StructField('sea_surface_subskin_temperature_quality', LongType(), True),

            StructField('sea_surface_temperature', DoubleType(), True),
            StructField('sea_surface_temperature_quality', LongType(), True),

            StructField('sea_water_density', DoubleType(), True),
            StructField('sea_water_density_quality', LongType(), True),

            StructField('sea_water_electrical_conductivity', DoubleType(), True),
            StructField('sea_water_electrical_conductivity_quality', LongType(), True),

            StructField('sea_water_practical_salinity', DoubleType(), True),
            StructField('sea_water_practical_salinity_quality', LongType(), True),

            StructField('sea_water_salinity', DoubleType(), True),
            StructField('sea_water_salinity_quality', LongType(), True),

            StructField('sea_water_temperature', DoubleType(), True),
            StructField('sea_water_temperature_quality', LongType(), True),

            StructField('surface_downwelling_photosynthetic_photon_flux_in_air', DoubleType(), True),
            StructField('surface_downwelling_photosynthetic_photon_flux_in_air_quality', LongType(), True),

            StructField('wet_bulb_temperature', DoubleType(), True),
            StructField('wet_bulb_temperature_quality', LongType(), True),

            StructField('wind_speed', DoubleType(), True),
            StructField('wind_speed_quality', LongType(), True),

            StructField('wind_from_direction', DoubleType(), True),
            StructField('wind_from_direction_quality', LongType(), True),

            StructField('wind_to_direction', DoubleType(), True),
            StructField('wind_to_direction_quality', LongType(), True),

            StructField('eastward_wind', DoubleType(), True),
            StructField('northward_wind', DoubleType(), True),
            StructField('wind_component_quality', LongType(), True),

            StructField('device', StringType(), True),
        ])
        new_struct = cdms_schema.get_schema_from_json()
        self.assertEqual(sorted(str(ALL_SCHEMA)), sorted(str(new_struct)), f'not equal old_struct = {ALL_SCHEMA}. new_struct = {new_struct}')
        return

    def test_02(self):
        data_json_schema = FileUtils.read_json('../../../in_situ_schema.json')
        structure_config = FileUtils.read_json('../../../insitu.file.structure.config.json')
        file_struct_setting = FileStructureSetting(data_json_schema=data_json_schema, structure_config=structure_config)
        cdms_schema = CdmsSchema(file_struct_setting)
        new_struct = cdms_schema.get_pandas_schema_from_json()
        print(new_struct)
        self.assertTrue(isinstance(new_struct, dict), f'wrong type: {new_struct}')
        return
