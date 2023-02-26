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

import fastjsonschema
import logging
from datetime import datetime
from multiprocessing import Pool

LOGGER = logging.getLogger(__name__)


def __validate_small_data(small_data, schema):
    try:
        fastjsonschema.compile(schema)(small_data)
        return None
    except Exception as e:
        return str(e)


def parallel_validate(chunked_data, schema):
    a = datetime.now()
    with Pool(16) as p:
        all_result = p.starmap(__validate_small_data, [(k, schema) for k in chunked_data])
    all_result = [k for k in all_result if k is not None]
    b = datetime.now()
    LOGGER.debug(f'validation took: {b - a}')
    return len(all_result) < 1, all_result


class ParallelJsonValidator(object):
    def __init__(self):
        self.__schema = None

    @property
    def schema(self):
        return self.__schema

    @schema.setter
    def schema(self, val):
        """
        :param val:
        :return: None
        """
        self.__schema = val
        return

    def load_schema(self, input_schema):
        self.schema = input_schema
        return self

    def is_schema_loaded(self):
        return self.__schema is not None

    def __validate_this(self, small_data):
        try:
            self.__schema(small_data)
            return None
        except Exception as e:
            return str(e)

    def validate_single_json(self, input_object):
        try:
            fastjsonschema.compile(self.schema)(input_object)
        except Exception as e:
            return False, str(e)
        return True, ''

    def validate_json(self, chunked_data: list):
        if self.is_schema_loaded() is False:
            raise ValueError(f'schema is not loaded. cannot validate')
        if len(chunked_data) < 1:
            LOGGER.debug(f'no need to validate empty json')
            return True
        LOGGER.debug(f'chunked_data size: {len(chunked_data)}')
        return parallel_validate(chunked_data, self.schema)
