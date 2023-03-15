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

from pyspark.sql.types import StructType, StructField, DoubleType, StringType, MapType, LongType, TimestampType, \
    IntegerType


class CdmsSchema:
    ALL_SCHEMA = StructType([
        StructField('depth', DoubleType(), True),
        StructField('latitude', DoubleType(), True),
        StructField('longitude', DoubleType(), True),
        StructField('meta', StringType(), True),
        StructField('platform', MapType(StringType(), StringType()), True),

        StructField('time', StringType(), True),
        StructField('time_obj', TimestampType(), True),

        StructField('provider', StringType(), True),
        StructField('project', StringType(), True),
        StructField('platform_code', IntegerType(), True),
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

    def __get_json_datatype(self, datetype_name: str, datatype_def: dict):
        if 'type' in datatype_def:
            temp_type = datatype_def['type']
            if isinstance(temp_type, str):
                return temp_type
            if isinstance(temp_type, list):
                return temp_type[0]
            raise ValueError(f'unknown datatype: {datetype_name}: {datatype_def}')
        if datetype_name.endswith('_quality'):
            return 'long'
        if datetype_name == 'platform':  # special case
            return 'platform'
        raise ValueError(f'unknown datatype: {datetype_name}: {datatype_def}')

    def __init__(self):
        self.__json_to_pandas_data_type = {
            'number': 'double',
            'long': 'int64',
            'string': 'object',
            'platform': 'object',
        }
        self.__json_to_spark_data_types = {
            'number': DoubleType(),
            'long': LongType(),
            'string': StringType(),
            'platform': MapType(StringType(), StringType()),
        }
        self.__default_columns = [
            StructField('time_obj', TimestampType(), True),

            StructField('provider', StringType(), True),
            StructField('project', StringType(), True),
            StructField('platform_code', IntegerType(), True),
            StructField('year', IntegerType(), True),
            StructField('month', IntegerType(), True),
            StructField('job_id', StringType(), True),
        ]
        self.__non_observation_columns = [
            'time_obj',
            'time',

            'provider',
            'project',
            'platform_code',
            'platform',
            'year',
            'month',
            'job_id',

            'device',

            'latitude',
            'longitude',
            'depth',
        ]

    def __get_pandas_type(self, json_type: str):
        if json_type not in self.__json_to_pandas_data_type:
            raise ValueError(f'unknown json type. cannot convert to pandas type: {json_type}')
        return self.__json_to_pandas_data_type[json_type]

    def __get_spark_type(self, json_type: str):
        if json_type not in self.__json_to_spark_data_types:
            raise ValueError(f'unknown json type. cannot convert to spark type: {json_type}')
        return self.__json_to_spark_data_types[json_type]

    def __get_obs_defs(self, in_situ_schema: dict):
        if 'definitions' not in in_situ_schema:
            raise ValueError(f'missing definitions in in_situ_schema: {in_situ_schema}')
        base_defs = in_situ_schema['definitions']
        if 'observation' not in base_defs:
            raise ValueError(f'missing observation in in_situ_schema["definitions"]: {base_defs}')
        obs_defs = base_defs['observation']
        if 'properties' not in obs_defs:
            raise ValueError(f'missing properties in in_situ_schema["definitions"]["observation"]: {obs_defs}')
        return obs_defs['properties']

    def get_observation_names(self, in_situ_schema: dict):
        obs_names = [k for k in self.__get_obs_defs(in_situ_schema).keys() if k not in self.__non_observation_columns and not k.endswith('_quality')]
        return obs_names

    def get_schema_from_json(self, in_situ_schema: dict):
        dynamic_columns = [StructField(k, self.__get_spark_type(self.__get_json_datatype(k, v)), True) for k, v in self.__get_obs_defs(in_situ_schema).items()]
        return StructType(dynamic_columns + self.__default_columns)

    def get_pandas_schema_from_json(self, in_situ_schema: dict):
        dynamic_columns = {k: self.__get_pandas_type(self.__get_json_datatype(k, v)) for k, v in self.__get_obs_defs(in_situ_schema).items()}
        return dynamic_columns
