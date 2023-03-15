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

import math


class SpatialUtils:
    @staticmethod
    def generate_lat_lon_intervals(min_lat_lon: tuple, max_lat_lon: tuple, interval: int) -> list:
        """

        :param min_lat_lon: tuple : (lat, lon)
        :param max_lat_lon: tuple : (lat, lon)
        :param interval: int
        :return: list
        """
        def __floor_by_interval(input_value):
            return int(input_value - divmod(input_value, interval)[1])

        def __get_end_range(input_value):
            potential_end_range = math.ceil(input_value)
            return potential_end_range + 1

        if not isinstance(min_lat_lon, tuple) or not isinstance(max_lat_lon, tuple) or len(min_lat_lon) != 2 or len(max_lat_lon) != 2:
            raise ValueError(f'incorrect input. min_lat_lon & max_lat_lon should be tuple with size 2: {min_lat_lon}, {max_lat_lon}')
        min_lat = __floor_by_interval(min_lat_lon[0])
        min_lon = __floor_by_interval(min_lat_lon[1])

        lat_intervals = [k for k in range(min_lat, __get_end_range(max_lat_lon[0]), interval)]
        lon_intervals = [k for k in range(min_lon, __get_end_range(max_lat_lon[1]), interval)]

        lat_long_list = [(each_lat, each_lon) for each_lon in lon_intervals for each_lat in lat_intervals]
        return lat_long_list
