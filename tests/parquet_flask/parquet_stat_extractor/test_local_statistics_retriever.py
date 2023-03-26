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

from parquet_flask.parquet_stat_extractor.local_statistics_retriever import LocalStatisticsRetriever
from parquet_flask.utils.general_utils import GeneralUtils

stats_result_schema = {
    "$schema": "http://json-schema.org/draft-04/schema#",
    "type": "object",
    "properties": {
        "total": {
            "type": "integer"
        },
        "min_datetime": {
            "type": "number"
        },
        "max_datetime": {
            "type": "number"
        },
        "min_depth": {
            "type": "number"
        },
        "max_depth": {
            "type": "number"
        },
        "min_lat": {
            "type": "number"
        },
        "max_lat": {
            "type": "number"
        },
        "min_lon": {
            "type": "number"
        },
        "max_lon": {
            "type": "number"
        },
        "observation_counts": {
            "type": "object",
            "properties": {
                "air_pressure": {
                    "type": "integer"
                },
                "air_temperature": {
                    "type": "integer"
                },
                "dew_point_temperature": {
                    "type": "integer"
                },
                "downwelling_longwave_flux_in_air": {
                    "type": "integer"
                },
                "downwelling_longwave_radiance_in_air": {
                    "type": "integer"
                },
                "downwelling_shortwave_flux_in_air": {
                    "type": "integer"
                },
                "mass_concentration_of_chlorophyll_in_sea_water": {
                    "type": "integer"
                },
                "rainfall_rate": {
                    "type": "integer"
                },
                "relative_humidity": {
                    "type": "integer"
                },
                "sea_surface_salinity": {
                    "type": "integer"
                },
                "sea_surface_skin_temperature": {
                    "type": "integer"
                },
                "sea_surface_subskin_temperature": {
                    "type": "integer"
                },
                "sea_surface_temperature": {
                    "type": "integer"
                },
                "sea_water_density": {
                    "type": "integer"
                },
                "sea_water_electrical_conductivity": {
                    "type": "integer"
                },
                "sea_water_practical_salinity": {
                    "type": "integer"
                },
                "sea_water_salinity": {
                    "type": "integer"
                },
                "sea_water_temperature": {
                    "type": "integer"
                },
                "surface_downwelling_photosynthetic_photon_flux_in_air": {
                    "type": "integer"
                },
                "wet_bulb_temperature": {
                    "type": "integer"
                },
                "wind_speed": {
                    "type": "integer"
                },
                "wind_from_direction": {
                    "type": "integer"
                },
                "wind_to_direction": {
                    "type": "integer"
                },
                "eastward_wind": {
                    "type": "integer"
                },
                "northward_wind": {
                    "type": "integer"
                },
                "meta": {
                    "type": "integer"
                }
            },
            "required": [
                "air_pressure",
                "air_temperature",
                "dew_point_temperature",
                "downwelling_longwave_flux_in_air",
                "downwelling_longwave_radiance_in_air",
                "downwelling_shortwave_flux_in_air",
                "mass_concentration_of_chlorophyll_in_sea_water",
                "rainfall_rate",
                "relative_humidity",
                "sea_surface_salinity",
                "sea_surface_skin_temperature",
                "sea_surface_subskin_temperature",
                "sea_surface_temperature",
                "sea_water_density",
                "sea_water_electrical_conductivity",
                "sea_water_practical_salinity",
                "sea_water_salinity",
                "sea_water_temperature",
                "surface_downwelling_photosynthetic_photon_flux_in_air",
                "wet_bulb_temperature",
                "wind_speed",
                "wind_from_direction",
                "wind_to_direction",
                "eastward_wind",
                "northward_wind",
                # "meta",  # TODO this is no longer showing in new stats retriever
            ]
        }
    },
    "required": [
        "total",
        "min_datetime",
        "max_datetime",
        "min_depth",
        "max_depth",
        "min_lat",
        "max_lat",
        "min_lon",
        "max_lon",
        "observation_counts"
    ]
}


class TestLocalStatisticsRetriever(unittest.TestCase):
    def test_01(self):
        stats_retriever = LocalStatisticsRetriever('part-00000-74ebb882-3536-435b-b736-96bf3be9ee29.c000.gz.parquet', 'in_situ_schema.json', '/Users/wphyo/Projects/access/parquet_test_1/insitu.file.structure.config.json')
        stats = stats_retriever.start()
        validate_result, validate_error = GeneralUtils.is_json_valid(stats, stats_result_schema)
        self.assertTrue(validate_result, f'schema error: {validate_error}')
        return
