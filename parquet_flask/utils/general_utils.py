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
from hashlib import sha256
from math import isnan

import fastjsonschema


class GeneralUtils:
    @staticmethod
    def is_json_valid(payload, schema):
        try:
            fastjsonschema.validate(schema, payload)
        except Exception as error:
            return False, str(error)
        return True, None

    @staticmethod
    def chunk_list(input_list, chunked_size):
        """Yield successive n-sized chunks from l."""
        for i in range(0, len(input_list), chunked_size):
            yield input_list[i:i + chunked_size]

    @staticmethod
    def is_int(input_val: str, accept_nan: bool = False):
        try:
            if input_val is None:
                return False
            value = int(input_val)
            if isnan(value) is True and accept_nan is False:
                return False
            return True
        except ValueError:
            return False

    @staticmethod
    def is_float(input_val: str, accept_nan: bool = False):
        try:
            if input_val is None:
                return False
            value = float(input_val)
            if isnan(value) is True and accept_nan is False:
                return False
            return True
        except ValueError:
            return False

    @staticmethod
    def floor_lat_long(latitude: float, longitude: float, interval: int = 5):
        if interval == 0:
            raise ValueError(f'interval cannot be 0')
        interval_lat = divmod(latitude, interval)
        interval_lon = divmod(longitude, interval)
        return f'{latitude - interval_lat[1]}_{longitude - interval_lon[1]}'

    @staticmethod
    def gen_float_list_from_comma_sep_str(input_val: str, expected_count: int):
        split_bbox_str = input_val.strip().split(',')
        if len(split_bbox_str) != expected_count:
            raise ValueError(f'incorrect length for bbox: {input_val}. expected_count: {expected_count}')
        split_is_float = [GeneralUtils.is_float(k) for k in split_bbox_str]
        if not all(split_is_float):
            raise ValueError(f'one or more is not float for bbox: {input_val}')
        return [float(k) for k in split_bbox_str]

    @staticmethod
    def gen_sha_256_json_obj(input_json_obj: dict) -> str:
        return sha256(json.dumps(input_json_obj, sort_keys=True).encode()).hexdigest()
