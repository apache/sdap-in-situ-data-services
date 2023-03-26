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
import json
from unittest import TestCase

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp

from parquet_flask.insitu.file_structure_setting import FileStructureSetting
from parquet_flask.io_logic.cdms_constants import CDMSConstants
from parquet_flask.parquet_stat_extractor.statistics_retriever import StatisticsRetriever
from parquet_flask.utils.file_utils import FileUtils


class TestStatisticsRetriever(TestCase):

    def test_new_config_01(self):
        input_json = [
            {
                CDMSConstants.lat_col: 0.0,
                CDMSConstants.lon_col: 1.0,
                CDMSConstants.depth_col: 2.0,
                'air_pressure': 1.1,
                CDMSConstants.time_col: '2000-01-01T00:00:03Z',
            },
            {
                CDMSConstants.lat_col: 1.0,
                CDMSConstants.lon_col: 2.0,
                CDMSConstants.depth_col: 3.0,
                CDMSConstants.time_col: '2000-01-01T00:00:00Z',
            },
            {
                CDMSConstants.lat_col: 2.0,
                CDMSConstants.lon_col: 3.0,
                CDMSConstants.depth_col: 0.0,
                CDMSConstants.time_col: '2000-01-01T00:00:01Z',
            },
            {
                CDMSConstants.lat_col: 3.0,
                CDMSConstants.lon_col: 0.0,
                CDMSConstants.depth_col: 1.0,
                CDMSConstants.time_col: '2000-01-01T00:00:02Z',
            },
            {
                CDMSConstants.lat_col: 0.0,
                CDMSConstants.lon_col: 1.0,
                CDMSConstants.depth_col: float(CDMSConstants.missing_depth_value),
                CDMSConstants.time_col: '2000-01-01T00:00:03Z',
            },

        ]
        file_structure_config_file = '/Users/wphyo/Projects/access/parquet_test_1/insitu.file.structure.config.json'
        file_structure_config_file = FileUtils.read_json(file_structure_config_file)
        file_structure_setting = FileStructureSetting({}, file_structure_config_file)
        spark = SparkSession.builder \
            .master("local") \
            .appName('TestAppName') \
            .getOrCreate()
        df = spark.createDataFrame(input_json)
        df = df.withColumn(CDMSConstants.time_obj_col, to_timestamp(CDMSConstants.time_col))
        stats_retriever = StatisticsRetriever(df, file_structure_setting).start()
        old_result = {'total': 5, 'min_datetime': 946684800.0, 'max_datetime': 946684803.0, 'min_depth': 0.0,
                      'max_depth': 3.0, 'min_lat': 0.0, 'max_lat': 3.0, 'min_lon': 0.0, 'max_lon': 3.0,
                      'observation_counts': {'air_pressure': 1, 'air_temperature': 0, 'dew_point_temperature': 0, 'downwelling_longwave_flux_in_air': 0,
                                             'downwelling_longwave_radiance_in_air': 0, 'downwelling_shortwave_flux_in_air': 0, 'mass_concentration_of_chlorophyll_in_sea_water': 0,
                                             'rainfall_rate': 0, 'relative_humidity': 0, 'sea_surface_salinity': 0, 'sea_surface_skin_temperature': 0,
                                             'sea_surface_subskin_temperature': 0, 'sea_surface_temperature': 0, 'sea_water_density': 0, 'sea_water_electrical_conductivity': 0,
                                             'sea_water_practical_salinity': 0, 'sea_water_salinity': 0, 'sea_water_temperature': 0,
                                             'surface_downwelling_photosynthetic_photon_flux_in_air': 0, 'wet_bulb_temperature': 0, 'wind_speed': 0, 'wind_from_direction': 0,
                                             'wind_to_direction': 0, 'eastward_wind': 0, 'northward_wind': 0}}
        print(json.dumps(old_result, sort_keys=True))
        print(json.dumps(stats_retriever.to_json(), sort_keys=True))
        self.assertEqual(json.dumps(old_result, sort_keys=True), json.dumps(stats_retriever.to_json(), sort_keys=True), 'backward compatibility failed')
        return
