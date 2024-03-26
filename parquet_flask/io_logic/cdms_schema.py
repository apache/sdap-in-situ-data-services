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

from pyspark.sql.types import StructType, StructField, DoubleType, \
    StringType, MapType, LongType, TimestampType, IntegerType

class CdmsSchema:
    ALL_SCHEMA = StructType([
        # Common
        StructField('provider', StringType(), True),
        StructField('project', StringType(), True),
        StructField('time', StringType(), True),
        StructField('time_obj', TimestampType(), True),
        StructField('latitude', DoubleType(), True),
        StructField('longitude', DoubleType(), True),
        StructField('platform', MapType(StringType(), StringType()), True),
        StructField('platform_id', StringType(), True),

        # AQ
        StructField('pm2_5', DoubleType(), True),
        StructField('bc', DoubleType(), True),
        StructField('no2', DoubleType(), True),
        StructField('co', DoubleType(), True),
        StructField('co2', DoubleType(), True),
        StructField('no', DoubleType(), True),
        StructField('o3', DoubleType(), True),
        StructField('pm1', DoubleType(), True),
        StructField('pm10', DoubleType(), True),
        StructField('pm25', DoubleType(), True),

        # IDEAS
        StructField('Qout', DoubleType(), True),
        StructField('Streamflow', DoubleType(), True),
        StructField('Gage_height', DoubleType(), True),
        StructField('Stream_water_level_elevation_above_NAVD_1988', DoubleType(), True),
        StructField('Lake_or_reservoir_water_surface_elevation_above_NAVD_1988', DoubleType(), True),
        StructField('Temperature', DoubleType(), True),
        StructField('Spcific_conductance', DoubleType(), True),
        StructField('Turbidity', DoubleType(), True),
        StructField('Suspended_sediment_concentration', DoubleType(), True),
        StructField('Precipitation', DoubleType(), True),
        StructField('Depth_to_water_level', DoubleType(), True),
        StructField('Streamflow_quality', StringType(), True),
        StructField('Gage_height_quality', StringType(), True),
        StructField('Stream_water_level_elevation_above_NAVD_1988_quality', StringType(), True),
        StructField('Lake_or_reservoir_water_surface_elevation_above_NAVD_1988_quality', StringType(), True),
        StructField('Temperature_quality', StringType(), True),
        StructField('Specific_conductance_quality', StringType(), True),
        StructField('Turbidity_quality', StringType(), True),
        StructField('Suspended_sediment_concentration_quality', StringType(), True),
        StructField('Precipitation_quality', StringType(), True),
        StructField('Depth_to_water_level_quality', StringType(), True),
        StructField('vortex_height', DoubleType(), True),
        StructField('vortex_avg_speed', DoubleType(), True),
        StructField('vortex_height_rise_speed', DoubleType(), True),
        StructField('vortex_rain', DoubleType(), True)
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

    def get_mandatory_record_keys(self, in_situ_schema: dict):
        if 'definitions' not in in_situ_schema:
            raise ValueError(f'missing definitions in in_situ_schema: {in_situ_schema}')
        base_defs = in_situ_schema['definitions']
        if 'observation' not in base_defs:
            raise ValueError(f'missing observation in in_situ_schema["definitions"]: {base_defs}')
        obs_defs = base_defs['observation']
        if 'required' not in obs_defs:
            raise ValueError(f'missing required in in_situ_schema["definitions"]["observation"]: {obs_defs}')
        return obs_defs['required']

    def get_schema_from_json(self, in_situ_schema: dict):
        dynamic_columns = [StructField(k, self.__get_spark_type(self.__get_json_datatype(k, v)), True) for k, v in self.__get_obs_defs(in_situ_schema).items()]
        return StructType(dynamic_columns + self.__default_columns)

    def get_pandas_schema_from_json(self, in_situ_schema: dict):
        dynamic_columns = {k: self.__get_pandas_type(self.__get_json_datatype(k, v)) for k, v in self.__get_obs_defs(in_situ_schema).items()}
        return dynamic_columns
